"""Run durable generation jobs with PostgreSQL leases and fenced mutations."""
import asyncio
import logging
import os
import signal
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import httpx
from sqlalchemy import func, or_, select, text, update
from sqlalchemy.exc import DBAPIError, OperationalError

from app.db import SessionLocal
from app.graph import WorkflowFailed, run_workflow
from app.models import Chapter, GenerationJob, Story
from app.worker_health import publish_liveness

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)
LEASE_SECONDS = int(os.getenv("JOB_LEASE_SECONDS", "300"))
MAX_ATTEMPTS = int(os.getenv("JOB_MAX_ATTEMPTS", "3"))
GENERATION_TIMEOUT = int(os.getenv("JOB_GENERATION_TIMEOUT", "280"))
if LEASE_SECONDS < 1 or MAX_ATTEMPTS < 1 or GENERATION_TIMEOUT < 1:
    raise ValueError("Job lease, attempts and generation timeout must be positive")


@dataclass(frozen=True)
class Claim:
    job_id: str
    token: str
    attempt: int


@dataclass(frozen=True)
class Snapshot:
    story_id: str
    version: int
    state: dict
    instruction: str


def database_now(db):
    return db.scalar(select(func.clock_timestamp()))


def finish_job(job, status, error=None, *, available_at=None):
    job.status = status
    job.error = error
    job.claim_token = None
    job.locked_until = None
    job.available_at = available_at
    job.updated_at = func.clock_timestamp()



def mark_conflict(job, current_version):
    job.result = {"expected_version": job.base_version, "current_version": current_version}
    finish_job(job, "conflict", "Story version changed; request a fresh chapter")


def claim_job():
    eligible = or_(
        (GenerationJob.status == "queued") & or_(
            GenerationJob.available_at.is_(None),
            GenerationJob.available_at <= func.statement_timestamp(),
        ),
        (GenerationJob.status == "running") & or_(
            GenerationJob.claim_token.is_(None),
            GenerationJob.locked_until.is_(None),
            GenerationJob.locked_until <= func.statement_timestamp(),
        ),
    )
    while True:
        with SessionLocal.begin() as db:
            job = db.scalar(select(GenerationJob).where(eligible).order_by(
                GenerationJob.created_at, GenerationJob.id,
            ).with_for_update(skip_locked=True).limit(1))
            if job is None:
                return None
            if job.attempts >= MAX_ATTEMPTS:
                finish_job(job, "failed", "Job attempt limit reached")
                continue
            job.status = "running"
            job.attempts += 1
            job.claim_token = str(uuid4())
            job.locked_until = database_now(db) + timedelta(seconds=LEASE_SECONDS)
            job.available_at = None
            job.error = None
            job.updated_at = func.clock_timestamp()
            return Claim(job.id, job.claim_token, job.attempts)


def locked_claim(db, claim):
    job = db.get(GenerationJob, claim.job_id, with_for_update=True)
    now = database_now(db)
    if (job is None or job.status != "running" or job.claim_token != claim.token
            or job.locked_until is None or job.locked_until <= now):
        return None, now
    return job, now


def renew_claim(claim):
    with SessionLocal.begin() as db:
        job, now = locked_claim(db, claim)
        if job is None:
            return False
        job.locked_until = now + timedelta(seconds=LEASE_SECONDS)
        job.updated_at = func.clock_timestamp()
        return True


def load_snapshot(claim):
    with SessionLocal.begin() as db:
        job, _ = locked_claim(db, claim)
        if job is None:
            return None
        story = db.get(Story, job.story_id)
        if job.locked_until <= database_now(db):
            return None
        if story is None:
            finish_job(job, "failed", "Story missing")
            return None
        if job.base_version is None:
            # Legacy requests without a version pin it on their first upgraded claim.
            job.base_version = (
                job.expected_version if job.expected_version is not None else story.version
            )
            job.updated_at = func.clock_timestamp()
        if job.base_version != story.version:
            mark_conflict(job, story.version)
            return None
        return Snapshot(story.id, story.version, story.state, job.instruction)


def commit_chapter(claim, snapshot, generated):
    refined = generated["refinement"]
    checks = refined.get("checks", {})
    if not checks or not all(checks.values()) or not refined.get("text", "").strip():
        raise ValueError("Narrative validation failed")
    chapter = refined["text"]
    new_fact = generated["plot"]["new_fact"]
    version = snapshot.version + 1
    state = {
        **snapshot.state,
        "chapter_count": version,
        "latest_fact": new_fact,
        "facts": [*snapshot.state.get("facts", []), new_fact],
        "last_chapter": chapter,
    }
    contributions = {role: generated[role] for role in ("plot", "character", "dialogue", "refinement")}
    with SessionLocal.begin() as db:
        db.execute(text("SET LOCAL lock_timeout = '5s'"))
        # Serialize commits without blocking the foreign-key check on newly queued jobs.
        current_version = db.scalar(select(Story.version).where(
            Story.id == snapshot.story_id,
        ).with_for_update(key_share=True))
        job, _ = locked_claim(db, claim)
        if job is None or job.story_id != snapshot.story_id or job.base_version != snapshot.version:
            return False
        updated = db.scalar(update(Story).where(
            Story.id == snapshot.story_id, Story.version == snapshot.version,
        ).values(version=version, state=state, updated_at=func.clock_timestamp())
            .returning(Story.version))
        if updated is None:
            mark_conflict(job, current_version)
            return False
        db.add(Chapter(story_id=job.story_id, version=version, text=chapter, contributions=contributions))
        job.result = {
            "story_id": job.story_id, "version": version, "chapter": chapter,
            "state": state, "contributions": contributions,
        }
        finish_job(job, "completed")
        return True


def record_failure(claim, exc):
    retryable = isinstance(exc, (TimeoutError, httpx.TransportError, OperationalError))
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        retryable = code in (408, 429) or 500 <= code < 600
    if isinstance(exc, DBAPIError):
        retryable |= getattr(exc.orig, "sqlstate", None) in {"40001", "40P01", "55P03"}
    message = f"Temporary generation failure ({type(exc).__name__})" if retryable else (
        f"Generation failed ({type(exc).__name__})"
    )
    retry_after = 0
    if isinstance(exc, WorkflowFailed):
        retryable = exc.retryable
        retry_after = exc.retry_after
        message = str(exc)
    with SessionLocal.begin() as db:
        job, now = locked_claim(db, claim)
        if job is None:
            return False
        if retryable and job.attempts < MAX_ATTEMPTS:
            delay = max(retry_after, min(60, 2 ** min(job.attempts, 6)))
            finish_job(job, "queued", message, available_at=now + timedelta(seconds=delay))
        else:
            finish_job(job, "failed", message)
        return True



def release_claim(claim):
    with SessionLocal.begin() as db:
        db.execute(text("SET LOCAL lock_timeout = '5s'"))
        job, _ = locked_claim(db, claim)
        if job is None:
            return False
        if job.attempts >= MAX_ATTEMPTS:
            finish_job(job, "failed", "Worker shut down on the final attempt")
        else:
            finish_job(job, "queued", "Worker shut down; job queued for recovery")
        return True


async def heartbeat(claim, generation):
    while True:
        await asyncio.sleep(LEASE_SECONDS / 3)
        try:
            owns_lease = await asyncio.to_thread(renew_claim, claim)
        except DBAPIError:
            log.warning("Could not renew lease for job %s", claim.job_id)
            owns_lease = False
        if not owns_lease:
            generation.cancel()
            return


async def process(claim):
    try:
        snapshot = await asyncio.to_thread(load_snapshot, claim)
        if snapshot is None:
            return
        generation = asyncio.create_task(run_workflow({
            "story_id": snapshot.story_id, "base_version": snapshot.version,
            "story_snapshot": snapshot.state, "user_instruction": snapshot.instruction,
        }))
        renewal = asyncio.create_task(heartbeat(claim, generation))
        try:
            generated = await asyncio.wait_for(generation, timeout=GENERATION_TIMEOUT)
        except asyncio.CancelledError:
            if asyncio.current_task().cancelling():
                raise
            return  # A lost lease cancelled generation.
        else:
            await asyncio.to_thread(commit_chapter, claim, snapshot, generated)
        finally:
            renewal.cancel()
            await asyncio.gather(renewal, return_exceptions=True)
    except Exception as exc:
        log.exception("Job %s attempt %s failed", claim.job_id, claim.attempt)
        await asyncio.to_thread(record_failure, claim, exc)


async def run_worker(stopping: asyncio.Event):
    stop = asyncio.create_task(stopping.wait())
    try:
        while not stopping.is_set():
            try:
                claim = await asyncio.to_thread(claim_job)
                if claim is not None:
                    if not stopping.is_set():
                        active = asyncio.create_task(process(claim))
                        await asyncio.wait({active, stop}, return_when=asyncio.FIRST_COMPLETED)
                        if active.done():
                            await active
                            continue
                        active.cancel()
                        await asyncio.gather(active, return_exceptions=True)
                    # A commit already in progress can win this race. Both paths fence on the token.
                    await asyncio.to_thread(release_claim, claim)
                    break
            except DBAPIError:
                log.exception("Database error while polling or recording job state")
            await asyncio.wait({stop}, timeout=0.8)
    finally:
        stop.cancel()
        await asyncio.gather(stop, return_exceptions=True)


async def main():
    loop = asyncio.get_running_loop()
    stopping = asyncio.Event()
    for signum in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(signum, stopping.set)
    health_path = os.getenv("WORKER_HEALTH_FILE")
    health = asyncio.create_task(publish_liveness(Path(health_path))) if health_path else None
    try:
        await run_worker(stopping)
    finally:
        if health is not None:
            health.cancel()
            await asyncio.gather(health, return_exceptions=True)
        for signum in (signal.SIGTERM, signal.SIGINT):
            loop.remove_signal_handler(signum)


if __name__ == "__main__":
    asyncio.run(main())

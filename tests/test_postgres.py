"""PostgreSQL integration tests. Set TEST_DATABASE_URL to an isolated database."""
import asyncio
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from time import monotonic, sleep
from uuid import uuid4

import httpx
import pytest
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, inspect, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from app import main, worker
from app.graph import run_workflow
from app.models import Chapter, GenerationJob, Story


@pytest.fixture
def database():
    url = os.getenv("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL is required for PostgreSQL integration tests")
    admin = create_engine(url)
    schema = f"wp1_{uuid4().hex}"
    with admin.begin() as connection:
        connection.execute(text(f"CREATE SCHEMA {schema}"))
    engine = create_engine(make_url(url).update_query_dict({"options": f"-csearch_path={schema}"}))
    try:
        yield engine
    finally:
        engine.dispose()
        with admin.begin() as connection:
            connection.execute(text(f"DROP SCHEMA {schema} CASCADE"))
        admin.dispose()


def migrate(engine, revision="head"):
    with engine.connect() as connection:
        config = Config("alembic.ini")
        config.attributes["connection"] = connection
        command.upgrade(config, revision)


def test_legacy_upgrade_preserves_story_and_chapter(database):
    # Reproduce the inspected pre-queue schema, including its existing unique constraint.
    with database.begin() as connection:
        connection.execute(text("""
            CREATE TABLE stories (
                id varchar(36) PRIMARY KEY, title varchar(200) NOT NULL, genre varchar(80) NOT NULL,
                premise varchar(1000) NOT NULL, version integer NOT NULL, state json NOT NULL,
                created_at timestamptz DEFAULT now()
            )
        """))
        connection.execute(text("""
            CREATE TABLE chapters (
                id serial PRIMARY KEY, story_id varchar(36) NOT NULL REFERENCES stories(id),
                version integer NOT NULL, text varchar(20000) NOT NULL, contributions json NOT NULL,
                UNIQUE(story_id, version)
            )
        """))
        connection.execute(text("""
            INSERT INTO stories (id, title, genre, premise, version, state)
            VALUES ('legacy', 'Legacy', 'mystery', 'Old premise', 1, '{"facts":["old clue"]}')
        """))
        connection.execute(text("""
            INSERT INTO chapters (story_id, version, text, contributions)
            VALUES ('legacy', 1, 'Old chapter', '{"plot":{"new_fact":"old clue"}}')
        """))
    migrate(database)
    migrate(database)
    with sessionmaker(database)() as db:
        story = db.get(Story, "legacy")
        chapter = db.get(Chapter, 1)
        assert (story.version, story.state, story.updated_at) == (1, {"facts": ["old clue"]}, None)
        assert (chapter.text, chapter.contributions, chapter.created_at) == (
            "Old chapter", {"plot": {"new_fact": "old clue"}}, None,
        )
    assert len(inspect(database).get_unique_constraints("chapters")) == 1


def test_duplicate_legacy_records_abort_without_data_loss(database):
    migrate(database, "0001_legacy_baseline")
    with database.begin() as connection:
        connection.execute(text("""
            INSERT INTO stories (id, title, genre, premise, version, state)
            VALUES ('legacy', 'Legacy', 'mystery', 'Old premise', 1, '{}')
        """))
        connection.execute(text("""
            INSERT INTO chapters (story_id, version, text, contributions)
            VALUES ('legacy', 1, 'Keep first', '{}'), ('legacy', 1, 'Keep second', '{}')
        """))
    with pytest.raises(IntegrityError):
        migrate(database)
    with database.connect() as connection:
        assert connection.scalars(text("SELECT text FROM chapters ORDER BY id")).all() == [
            "Keep first", "Keep second",
        ]
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == "0001_legacy_baseline"
    assert "claim_token" not in {col["name"] for col in inspect(database).get_columns("generation_jobs")}


def test_upgrade_preserves_existing_job_records(database):
    migrate(database, "0001_legacy_baseline")
    with database.begin() as connection:
        connection.execute(text("""
            INSERT INTO stories (id, title, genre, premise, version, state)
            VALUES ('legacy', 'Legacy', 'mystery', 'Old premise', 1, '{}')
        """))
        connection.execute(text("""
            INSERT INTO generation_jobs
                (id, story_id, idempotency_key, instruction, status, attempts, result)
            VALUES ('queued', 'legacy', 'q', 'Resume', 'queued', 0, NULL),
                   ('completed', 'legacy', 'c', 'Done', 'completed', 1, json_build_object('version', 1))
        """))
    migrate(database)
    with sessionmaker(database)() as db:
        assert db.get(GenerationJob, "queued").instruction == "Resume"
        assert db.get(GenerationJob, "completed").result == {"version": 1}


@pytest.fixture
def service(database, monkeypatch):
    migrate(database)
    sessions = sessionmaker(database, expire_on_commit=False)
    monkeypatch.setattr(main, "SessionLocal", sessions)
    monkeypatch.setattr(worker, "SessionLocal", sessions)
    with TestClient(main.app) as client:
        response = client.post("/stories", json={"title": "Map", "premise": "An old map"})
        assert response.status_code == 201
        yield client, sessions, response.json()["story_id"]


GENERATED = {
    "plot": {"new_fact": "new clue"},
    "character": {},
    "dialogue": {},
    "refinement": {"text": "A chapter", "checks": {"valid": True}},
}


def submit(service, key, payload=None):
    client, _, story_id = service
    return client.post(
        f"/stories/{story_id}/continue",
        json=payload if payload is not None else {"instruction": "Continue", "expected_version": 0},
        headers={"Idempotency-Key": key},
    )


def parallel(*operations):
    ready = Barrier(len(operations), timeout=10)

    def run(operation):
        ready.wait()
        return operation()

    with ThreadPoolExecutor(max_workers=len(operations)) as pool:
        futures = [pool.submit(run, operation) for operation in operations]
        return [future.result(timeout=15) for future in futures]


def expire(sessions, claim):
    with sessions.begin() as db:
        db.get(GenerationJob, claim.job_id).locked_until = db.scalar(
            text("SELECT clock_timestamp() - interval '1 second'")
        )


def test_concurrent_idempotency_and_atomic_version_conflict(service):
    a, b, other = parallel(
        lambda: submit(service, "same"), lambda: submit(service, "same"),
        lambda: submit(service, "different"),
    )
    assert a.status_code == b.status_code == other.status_code == 202
    assert a.json()["job_id"] == b.json()["job_id"]
    assert submit(service, "same", {"instruction": "Different", "expected_version": 0}).status_code == 409
    assert other.json()["job_id"] != a.json()["job_id"]

    first, second = parallel(worker.claim_job, worker.claim_job)
    assert first.job_id != second.job_id
    first_snapshot = worker.load_snapshot(first)
    second_snapshot = worker.load_snapshot(second)
    assert first_snapshot.version == second_snapshot.version == 0
    assert sorted(parallel(
        lambda: worker.commit_chapter(first, first_snapshot, GENERATED),
        lambda: worker.commit_chapter(second, second_snapshot, GENERATED),
    )) == [False, True]

    client, sessions, story_id = service
    story = client.get(f"/stories/{story_id}").json()
    assert story["version"] == 1 and story["chapters"] == [{"version": 1, "text": "A chapter"}]
    outcomes = [client.get(f"/jobs/{claim.job_id}") for claim in (first, second)]
    assert all(result.status_code == 200 and result.headers["Cache-Control"] == "no-store"
               for result in outcomes)
    assert {result.json()["status"] for result in outcomes} == {"completed", "conflict"}
    conflict = next(result.json() for result in outcomes if result.json()["status"] == "conflict")
    assert conflict["result"] == {"expected_version": 0, "current_version": 1}
    replay = submit(service, "same")
    assert replay.status_code == 202 and replay.json()["job_id"] == a.json()["job_id"]
    with sessions() as db:
        assert len(db.scalars(select(GenerationJob.id)).all()) == 2


def test_expired_and_reclaimed_workers_cannot_mutate_job(service):
    _, sessions, _ = service
    submit(service, "reclaim")
    old = worker.claim_job()
    snapshot = worker.load_snapshot(old)
    assert worker.renew_claim(old)
    expire(sessions, old)
    assert not worker.renew_claim(old)
    assert not worker.record_failure(old, TimeoutError())
    assert not worker.commit_chapter(old, snapshot, GENERATED)
    current = worker.claim_job()
    assert current.job_id == old.job_id and current.token != old.token
    assert worker.load_snapshot(old) is None
    assert not worker.record_failure(old, ValueError())
    assert not worker.release_claim(old)
    assert not worker.commit_chapter(old, snapshot, GENERATED)
    assert worker.commit_chapter(current, worker.load_snapshot(current), GENERATED)


def test_lease_expiring_while_commit_waits_for_story_lock(service):
    _, sessions, story_id = service
    submit(service, "lock")
    claim = worker.claim_job()
    snapshot = worker.load_snapshot(claim)
    with ThreadPoolExecutor(max_workers=1) as pool:
        with sessions.begin() as db:
            db.get(Story, story_id, with_for_update=True)
            blocker = db.scalar(text("SELECT pg_backend_pid()"))
            future = pool.submit(worker.commit_chapter, claim, snapshot, GENERATED)
            deadline = monotonic() + 5
            while monotonic() < deadline:
                with sessions() as observer:
                    waiting = observer.scalar(text("""
                        SELECT EXISTS (
                            SELECT 1 FROM pg_stat_activity
                            WHERE :blocker = ANY(pg_blocking_pids(pid))
                        )
                    """), {"blocker": blocker})
                if waiting:
                    break
                sleep(0.02)
            else:
                pytest.fail("Commit did not block on the story row")
            expire(sessions, claim)
        assert future.result(timeout=10) is False


def test_claim_limit_and_retry_backoff(service, monkeypatch):
    _, sessions, _ = service
    monkeypatch.setattr(worker, "MAX_ATTEMPTS", 2)
    submit(service, "limit")
    first = worker.claim_job()
    assert worker.record_failure(first, httpx.ConnectTimeout("unavailable"))
    assert worker.claim_job() is None
    with sessions.begin() as db:
        db.get(GenerationJob, first.job_id).available_at = None
    second = worker.claim_job()
    assert second.attempt == 2
    expire(sessions, second)
    assert worker.claim_job() is None
    with sessions() as db:
        failed = db.get(GenerationJob, first.job_id)
        assert (failed.status, failed.attempts, failed.claim_token) == ("failed", 2, None)


def test_insert_failure_rolls_back_story_and_job(service):
    _, sessions, story_id = service
    submit(service, "rollback")
    claim = worker.claim_job()
    snapshot = worker.load_snapshot(claim)
    with sessions.begin() as db:
        db.add(Chapter(story_id=story_id, version=1, text="Existing", contributions={}))
    with pytest.raises(IntegrityError):
        worker.commit_chapter(claim, snapshot, GENERATED)
    with sessions() as db:
        assert db.get(Story, story_id).version == 0
        assert db.get(GenerationJob, claim.job_id).status == "running"
        assert db.scalar(select(Chapter.text)) == "Existing"


def test_omitted_version_stays_pinned_after_retry(service):
    _, sessions, _ = service
    submit(service, "omitted", {"instruction": "Continue"})
    queued = worker.claim_job()
    assert worker.load_snapshot(queued).version == 0
    assert worker.record_failure(queued, TimeoutError())
    submit(service, "winning")
    winner = worker.claim_job()
    assert worker.commit_chapter(winner, worker.load_snapshot(winner), GENERATED)
    with sessions.begin() as db:
        db.get(GenerationJob, queued.job_id).available_at = None
    retried = worker.claim_job()
    assert worker.load_snapshot(retried) is None
    with sessions() as db:
        assert db.get(GenerationJob, queued.job_id).status == "conflict"


def test_worker_renews_lease_during_generation(service, monkeypatch):
    _, sessions, _ = service
    monkeypatch.setattr(worker, "LEASE_SECONDS", 1)
    submit(service, "heartbeat")
    claim = worker.claim_job()

    def lease_expiry():
        with sessions() as db:
            return db.get(GenerationJob, claim.job_id).locked_until

    original_expiry = lease_expiry()

    async def generate(_):
        async with asyncio.timeout(5):
            while await asyncio.to_thread(lease_expiry) <= original_expiry:
                await asyncio.sleep(0.05)
        return GENERATED

    monkeypatch.setattr(worker, "run_workflow", generate)
    asyncio.run(worker.process(claim))
    with sessions() as db:
        assert db.get(GenerationJob, claim.job_id).status == "completed"


@pytest.mark.parametrize(("status", "expected"), [(429, "queued"), (422, "failed")])
def test_worker_records_workflow_errors_without_committing(service, monkeypatch, status, expected):
    client, sessions, story_id = service
    submission = submit(service, f"agent-error-{status}")
    claim = worker.claim_job()

    def reject(_):
        return httpx.Response(status, text="internal response body", headers={"Retry-After": "45"})

    async def generate(state):
        return await run_workflow(state, transport=httpx.MockTransport(reject))

    monkeypatch.setattr(worker, "run_workflow", generate)
    asyncio.run(worker.process(claim))

    job = client.get(f'/jobs/{submission.json()["job_id"]}').json()
    assert job["status"] == expected
    assert job["error"] == f"character/http_status ({status}), plot/http_status ({status})"
    story = client.get(f"/stories/{story_id}").json()
    assert story["version"] == 0 and story["chapters"] == []
    if status == 429:
        with sessions() as db:
            queued = db.get(GenerationJob, claim.job_id)
            assert (queued.available_at - worker.database_now(db)).total_seconds() > 40


def test_worker_commits_validated_http_workflow(service, monkeypatch):
    from agents.mock import generate
    from app.schemas import AgentRequest

    client, _, story_id = service
    submission = submit(service, "workflow-commit")
    claim = worker.claim_job()

    def respond(request):
        role = request.url.host
        payload = AgentRequest.model_validate_json(request.content)
        return httpx.Response(200, json={
            "agent": role, "story_id": payload.story_id, "base_version": payload.version,
            "contribution": generate(role, payload),
        })

    async def generate_chapter(state):
        return await run_workflow(state, transport=httpx.MockTransport(respond))

    monkeypatch.setattr(worker, "run_workflow", generate_chapter)
    asyncio.run(worker.process(claim))

    job = client.get(f'/jobs/{submission.json()["job_id"]}').json()
    story = client.get(f"/stories/{story_id}").json()
    assert job["status"] == "completed"
    assert story["version"] == job["result"]["version"] == 1
    assert story["chapters"] == [{"version": 1, "text": job["result"]["chapter"]}]
    assert set(job["result"]["contributions"]) == {"plot", "character", "dialogue", "refinement"}


def wait_for_claim(sessions, job_id, entered_generation):
    deadline = monotonic() + 10
    while monotonic() < deadline:
        with sessions() as db:
            job = db.get(GenerationJob, job_id)
            if job.status == "running" and job.claim_token and entered_generation.exists():
                return worker.Claim(job.id, job.claim_token, job.attempts)
        sleep(0.02)
    pytest.fail("Worker subprocess did not take the job")


@pytest.mark.parametrize("signal_name", ["terminate", "kill"])
def test_worker_process_termination_and_recovery(service, monkeypatch, tmp_path, signal_name):
    client, sessions, story_id = service
    job_id = submit(service, f"process-{signal_name}").json()["job_id"]
    # This worker uses the real polling loop and database, with generation paused mid-flight.
    entered_generation = tmp_path / "entered-generation"
    script = """
import asyncio
import os
from pathlib import Path

from app import worker

async def pause(_):
    Path(os.environ["WP4_GENERATION_MARKER"]).touch()
    await asyncio.Event().wait()

worker.run_workflow = pause
asyncio.run(worker.main())
"""
    url = sessions.kw["bind"].url.render_as_string(hide_password=False)
    with subprocess.Popen(
        [sys.executable, "-B", "-c", script],
        env={**os.environ, "DATABASE_URL": url, "JOB_LEASE_SECONDS": "3",
             "JOB_MAX_ATTEMPTS": "3", "JOB_GENERATION_TIMEOUT": "30",
             "WP4_GENERATION_MARKER": str(entered_generation)},
    ) as child:
        try:
            abandoned = wait_for_claim(sessions, job_id, entered_generation)
            getattr(child, signal_name)()
            assert child.wait(timeout=10) == (0 if signal_name == "terminate" else -9)
        finally:
            if child.poll() is None:
                child.kill()

    with sessions() as db:
        job = db.get(GenerationJob, job_id)
        if signal_name == "terminate":
            assert job.status == "queued" and job.claim_token is None
        else:
            assert job.status == "running" and job.claim_token == abandoned.token
    if signal_name == "kill":
        assert worker.claim_job() is None

    deadline = monotonic() + 10
    while (replacement := worker.claim_job()) is None:
        if monotonic() >= deadline:
            pytest.fail("Replacement worker did not recover the job")
        sleep(0.05)
    assert replacement.job_id == abandoned.job_id
    assert replacement.token != abandoned.token and replacement.attempt == 2
    assert not worker.release_claim(abandoned)
    snapshot = worker.load_snapshot(replacement)
    assert not worker.commit_chapter(abandoned, snapshot, GENERATED)

    async def generate(_):
        return GENERATED

    monkeypatch.setattr(worker, "run_workflow", generate)
    asyncio.run(worker.process(replacement))
    assert client.get(f"/jobs/{job_id}").json()["status"] == "completed"
    assert client.get(f"/stories/{story_id}").json()["chapters"] == [
        {"version": 1, "text": "A chapter"},
    ]


def test_rejects_invalid_idempotency_key_and_version(service):
    assert submit(service, "").status_code == 422
    assert submit(service, "version", {"expected_version": 2_147_483_647}).status_code == 422


def test_reader_sees_complete_versions_while_writer_is_uncommitted(service):
    client, sessions, story_id = service

    with sessions.begin() as db:
        story = db.get(Story, story_id)
        story.version = 1
        story.state = {**story.state, "chapter_count": 1, "last_chapter": "New chapter", "facts": ["New fact"]}
        db.add(Chapter(story_id=story_id, version=1, text="New chapter", contributions={}))
        db.flush()

        before = client.get(f"/stories/{story_id}").json()
        assert before["version"] == before["state"]["chapter_count"] == 0
        assert before["chapters"] == [] and before["state"]["last_chapter"] is None

    after = client.get(f"/stories/{story_id}").json()
    assert after["version"] == after["state"]["chapter_count"] == 1
    assert after["chapters"] == [{"version": 1, "text": after["state"]["last_chapter"]}]

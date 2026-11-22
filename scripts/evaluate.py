#!/usr/bin/env python3
"""Exercise the Story Cloud HTTP contract and measure repeatable workloads."""
import argparse
import json
import math
import os
import subprocess
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic, sleep
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4

TERMINAL = {"completed", "conflict", "failed"}


class StoryAPI:
    def __init__(self, url, poll_interval=0.2, timeout=60):
        self.url = url.rstrip("/")
        self.poll_interval = poll_interval
        self.timeout = timeout

    def call(self, path, body=None, key=None, expected=200):
        data = None if body is None else json.dumps(body).encode()
        headers = {"Content-Type": "application/json"}
        if key is not None:
            headers["Idempotency-Key"] = key
        request = Request(self.url + path, data=data, headers=headers)
        try:
            response = urlopen(request, timeout=5)
        except HTTPError as exc:
            response = exc
        with response:
            if response.code != expected:
                raise RuntimeError(f"{request.get_method()} {path} returned {response.code}, expected {expected}")
            return json.load(response)

    def story(self):
        return self.call("/stories", {
            "title": "Coursework evidence", "premise": "Alex finds a map in the flooded village",
            "genre": "mystery",
        }, expected=201)["story_id"]

    def submit(self, story_id, version, key=None, instruction="Search the tunnel carefully", expected=202):
        return self.call(f"/stories/{story_id}/continue", {
            "expected_version": version, "instruction": instruction,
        }, key=key or uuid4().hex, expected=expected)

    def wait(self, job_id, observe=None):
        deadline = monotonic() + self.timeout
        polls = 0
        while monotonic() < deadline:
            job = self.call(f"/jobs/{job_id}")
            polls += 1
            if observe is not None:
                observe()
            if job["status"] in TERMINAL:
                return job, polls
            sleep(self.poll_interval)
        raise TimeoutError(f"job {job_id} did not reach a terminal state")

    def verify_story(self, story_id, version=None):
        story = self.call(f"/stories/{story_id}")
        actual = story["version"]
        chapters = story["chapters"]
        state = story["state"]
        if (
            (version is not None and actual != version)
            or [chapter["version"] for chapter in chapters] != list(range(1, actual + 1))
            or state["chapter_count"] != actual
            or len(state["facts"]) != actual
            or state["last_chapter"] != (chapters[-1]["text"] if chapters else None)
        ):
            raise RuntimeError(f"story {story_id} returned an inconsistent committed snapshot")
        return actual


def run_checks(api):
    story = api.story()
    key = uuid4().hex
    with ThreadPoolExecutor(max_workers=3) as pool:
        first = pool.submit(api.submit, story, 0, key)
        replay = pool.submit(api.submit, story, 0, key)
        other = pool.submit(api.submit, story, 0)
        job_id = first.result()["job_id"]
        if replay.result()["job_id"] != job_id or other.result()["job_id"] == job_id:
            raise RuntimeError("idempotency did not identify the original request")
        jobs = [pool.submit(api.wait, item, lambda: api.verify_story(story)) for item in (
            job_id, other.result()["job_id"],
        )]
        outcomes = [job.result()[0] for job in jobs]
    api.submit(story, 0, key, instruction="Explore the archive", expected=409)
    if api.submit(story, 0, key)["job_id"] != job_id:
        raise RuntimeError("terminal job replay created a new job")
    if sorted(job["status"] for job in outcomes) != ["completed", "conflict"]:
        raise RuntimeError("competing writers did not produce one commit and one conflict")
    conflict = next(job for job in outcomes if job["status"] == "conflict")
    if conflict["result"] != {"expected_version": 0, "current_version": 1}:
        raise RuntimeError("conflict did not report the committed version")
    return {"story_id": story, "committed_version": api.verify_story(story, 1),
            "completed": 1, "conflicts": 1, "idempotent_replay": True,
            "different_payload_http_status": 409}


def percentiles(values):
    if not values:
        return None
    ordered = sorted(values)
    return {f"p{percent}": round(ordered[math.ceil(len(ordered) * percent / 100) - 1], 2)
            for percent in (50, 95)}


def measure_case(api, users, rounds, workload, epoch):
    story_count = users if workload == "independent" else rounds
    stories = [api.story() for _ in range(story_count)]
    started = monotonic()

    def generate(story, base_version):
        before = monotonic()
        job = api.submit(story, base_version)
        accepted_ms = (monotonic() - before) * 1000
        result, polls = api.wait(job["job_id"])
        return result["status"], accepted_ms, (monotonic() - before) * 1000, polls

    rows = []
    with ThreadPoolExecutor(max_workers=users) as pool:
        for round_number in range(rounds):
            targets = stories if workload == "independent" else [stories[round_number]] * users
            version = round_number if workload == "independent" else 0
            rows.extend(pool.map(lambda story: generate(story, version), targets))
    elapsed = monotonic() - started
    outcomes = Counter(row[0] for row in rows)
    expected_version = rounds if workload == "independent" else 1
    for story in stories:
        api.verify_story(story, expected_version)
    expected_completed = rounds * users if workload == "independent" else rounds
    if outcomes != Counter(completed=expected_completed, conflict=len(rows) - expected_completed):
        raise RuntimeError(f"unexpected outcomes at {users} users: {dict(outcomes)}")
    polls = sum(row[3] for row in rows)
    return {
        "workload": workload, "users": users, "rounds": rounds,
        "started_after_s": round(started - epoch, 3), "duration_s": round(elapsed, 3),
        "submitted": len(rows), "completed": outcomes["completed"], "conflicts": outcomes["conflict"],
        "failed": outcomes["failed"], "poll_requests": polls,
        "http_requests_per_s": round((len(rows) + polls) / elapsed, 2),
        "jobs_per_s": round(len(rows) / elapsed, 2),
        "completed_chapters_per_s": round(outcomes["completed"] / elapsed, 2),
        "completion_pct": round(100 * outcomes["completed"] / len(rows), 2),
        "conflict_pct": round(100 * outcomes["conflict"] / len(rows), 2),
        "submission_ms": percentiles([row[1] for row in rows]),
        "terminal_ms": percentiles([row[2] for row in rows]),
        "completed_ms": percentiles([row[2] for row in rows if row[0] == "completed"]),
    }


class DockerSampler:
    def __init__(self, project, epoch):
        self.epoch = epoch
        self.samples = []
        self.errors = []
        self.stop = threading.Event()
        self.containers = subprocess.check_output([
            "docker", "ps", "--filter", f"label=com.docker.compose.project={project}",
            "--format", "{{.Names}}",
        ], text=True, timeout=10).splitlines()
        if not self.containers:
            raise RuntimeError(f"no running containers for Compose project {project}")
        self.thread = threading.Thread(target=self.collect)

    def collect(self):
        while not self.stop.is_set():
            try:
                rows = subprocess.check_output([
                    "docker", "stats", "--no-stream", "--format", "{{json .}}", *self.containers,
                ], text=True, stderr=subprocess.DEVNULL, timeout=10)
                containers = [json.loads(line) for line in rows.splitlines()]
                self.samples.append({"after_s": round(monotonic() - self.epoch, 3), "containers": [
                    {"name": row["Name"], "cpu": row["CPUPerc"], "memory": row["MemUsage"]}
                    for row in containers
                ]})
            except (subprocess.SubprocessError, ValueError) as exc:
                self.errors.append(type(exc).__name__)
                return
            self.stop.wait(1)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.stop.set()
        self.thread.join()


def write_result(result, output):
    text = json.dumps(result, indent=2) + "\n"
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(text)
    else:
        print(text, end="")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=os.getenv("STORY_BASE_URL", "http://127.0.0.1:8000"))
    parser.add_argument("--output", type=Path)
    parser.add_argument("--poll", type=float, default=0.2)
    parser.add_argument("--timeout", type=float, default=60)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("checks", help="assert consistency, idempotency and conflict semantics")
    load = commands.add_parser("load", help="measure isolated and contested story workloads")
    load.add_argument("--users", nargs="+", type=int, default=[1, 5, 10, 20])
    load.add_argument("--rounds", type=int, default=5)
    load.add_argument("--workload", nargs="+", choices=["independent", "hotspot"],
                      default=["independent", "hotspot"])
    load.add_argument("--mode", choices=["mock", "real"], required=True,
                      help="label the actual agent configuration; the API does not expose it")
    load.add_argument("--label", required=True, help="describe environment and replica counts")
    load.add_argument("--compose-project", help="sample container CPU and memory using Docker")
    args = parser.parse_args()
    if args.poll <= 0 or args.timeout <= 0:
        parser.error("poll and timeout must be positive")
    api = StoryAPI(args.url, args.poll, args.timeout)
    result = {"measured_at_utc": datetime.now(timezone.utc).isoformat(), "url": args.url}
    if args.command == "checks":
        result.update(run_checks(api))
    else:
        if args.rounds < 1 or any(user < 1 for user in args.users):
            parser.error("users and rounds must be positive")
        epoch = monotonic()

        def cases():
            return [measure_case(api, user, args.rounds, workload, epoch)
                    for workload in args.workload for user in args.users]

        result.update(label=args.label, model_mode=args.mode, poll_interval_s=args.poll)
        if args.compose_project:
            with DockerSampler(args.compose_project, epoch) as sampler:
                result["cases"] = cases()
            result.update(docker_samples=sampler.samples, docker_sample_errors=sampler.errors)
        else:
            result["cases"] = cases()
    write_result(result, args.output)


if __name__ == "__main__":
    main()

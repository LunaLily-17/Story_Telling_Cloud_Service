#!/usr/bin/env python3
"""Measure replica distribution, worker crash recovery and Kubernetes rollouts."""
import argparse
import json
import re
import select
import subprocess
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from threading import Event
from time import monotonic, sleep
from uuid import uuid4

from evaluate import StoryAPI, write_result

ROLES = ("story-api", "story-worker", "plot", "character", "dialogue", "refinement")


def command(*args, timeout=15):
    result = subprocess.run(args, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f"{args[0]} exited {result.returncode}: {result.stderr.strip()}")
    return result.stdout


def compose_container(project, service, *, evidence=False):
    filters = ["--filter", f"label=com.docker.compose.project={project}",
               "--filter", f"label=com.docker.compose.service={service}"]
    if evidence:
        filters += ["--filter", "label=story-cloud.evidence=true"]
    found = command("docker", "ps", *filters, "--format", "{{.Names}}").splitlines()
    if len(found) != 1:
        raise RuntimeError(f"expected exactly one running {service} in {project}, found {len(found)}")
    return found[0]


def compose_balance(api, project, requests):
    gateway = compose_container(project, "gateway")
    token = uuid4().hex
    for _ in range(requests):
        api.call(f"/health?wp6_probe={token}")
    counts = Counter()
    deadline = monotonic() + 5
    while monotonic() < deadline:
        logs = command("docker", "logs", "--since", "1m", gateway)
        counts = Counter(match.group(1) for line in logs.splitlines()
                         if token in line and (match := re.search(r"\bapi/(api\d+)\b", line)))
        if sum(counts.values()) == requests:
            break
        sleep(0.1)
    if len(counts) < 2 or sum(counts.values()) != requests:
        raise RuntimeError(f"gateway logs did not confirm distribution: {dict(counts)}")
    return {"project": project, "requests": requests, "requests_by_backend": dict(counts)}


def compose_scale(api, project):
    compose_container(project, "postgres", evidence=True)
    roles = ("api", "worker", "plot", "character", "dialogue", "refinement")

    def counts():
        services = command(
            "docker", "ps", "--filter", f"label=com.docker.compose.project={project}",
            "--format", '{{.Label "com.docker.compose.service"}}',
        ).splitlines()
        return {role: services.count(role) for role in roles}

    before = counts()
    if set(before.values()) != {1}:
        raise RuntimeError("scale experiment requires one running replica per application role")
    started = monotonic()
    command("docker", "compose", "-p", project,
            "-f", "docker-compose.yml", "-f", "docker-compose.replicas.yml",
            "-f", "scripts/compose.evidence.yml", "up", "-d", "--no-build", "--wait",
            "--wait-timeout", str(max(1, int(api.timeout))), timeout=api.timeout + 15)
    elapsed = round(monotonic() - started, 3)
    after = counts()
    if set(after.values()) != {2}:
        raise RuntimeError(f"expected two ready replicas per role, found {after}")
    return {"project": project, "before": before, "after": after, "scale_ready_s": elapsed,
            "service_check": api.call("/ready")}


@contextmanager
def hold_story_lock(database, story):
    # NO KEY UPDATE still permits the foreign-key check when the API inserts its job.
    with subprocess.Popen(
        ["docker", "exec", "-i", database, "psql", "-XqAt", "-U", "story", "-d", "story",
         "-v", "ON_ERROR_STOP=1", "-v", f"story_id={story}"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
    ) as process:
        try:
            process.stdin.write(
                "BEGIN;\nSELECT 'locked' FROM stories WHERE id = :'story_id' FOR NO KEY UPDATE;\n"
            )
            process.stdin.flush()
            readable, _, _ = select.select([process.stdout], [], [], 10)
            if not readable or process.stdout.readline().strip() != "locked":
                raise RuntimeError("could not acquire the evidence story lock")
            yield
        finally:
            try:
                process.stdin.write("ROLLBACK;\n\\q\n")
                process.stdin.flush()
                process.wait(timeout=5)
            except (BrokenPipeError, subprocess.TimeoutExpired):
                process.kill()
                process.wait()


def compose_crash(api, project):
    worker = compose_container(project, "worker", evidence=True)
    database = compose_container(project, "postgres", evidence=True)
    lease = int(command("docker", "exec", worker, "printenv", "JOB_LEASE_SECONDS"))
    if api.timeout <= lease + 3:
        raise ValueError("observation timeout must exceed the lease by more than three seconds")
    story = api.story()
    killed = False
    try:
        with hold_story_lock(database, story):
            submitted = monotonic()
            job_id = api.submit(story, 0)["job_id"]
            deadline = submitted + api.timeout
            while monotonic() < deadline:
                job = api.call(f"/jobs/{job_id}")
                if job["status"] == "running" and job["attempts"] == 1:
                    running = monotonic()
                    break
                if job["status"] in {"completed", "conflict", "failed"}:
                    raise RuntimeError(f"job terminated before crash injection: {job['status']}")
                sleep(api.poll_interval)
            else:
                raise TimeoutError("worker did not claim the evidence job")
            interrupted = monotonic()
            command("docker", "kill", "--signal", "SIGKILL", worker)
            killed = True
    finally:
        if killed:
            command("docker", "start", worker)
    final, _ = api.wait(job_id)
    finished = monotonic()
    if final["status"] != "completed" or final["attempts"] != 2:
        raise RuntimeError(f"recovery produced {final['status']} after {final['attempts']} attempts")
    return {
        "project": project, "signal": "SIGKILL", "lease_seconds": lease,
        "job_id": job_id, "story_id": story, "attempts": final["attempts"], "status": final["status"],
        "submit_to_running_s": round(running - submitted, 3),
        "kill_to_completion_s": round(finished - interrupted, 3),
        "committed_version": api.verify_story(story, 1),
    }


def kube_client(context, namespace):
    def call(*args, timeout=15):
        return command("kubectl", "--context", context, "-n", namespace, *args, timeout=timeout)
    return call


def ready_pods(kube, role):
    items = json.loads(kube("get", "pods", "-l", f"app={role}", "-o", "json"))["items"]
    return {pod["metadata"]["uid"]: pod["metadata"]["name"] for pod in items
            if not pod["metadata"].get("deletionTimestamp")
            and any(c["type"] == "Ready" and c["status"] == "True"
                    for c in pod["status"].get("conditions", []))}


def deployment_replicas(kube, role):
    return json.loads(kube("get", "deployment", role, "-o", "json"))["spec"]["replicas"]


def wait_for_replicas(kube, role, minimum, deadline, *, old_uids=()):
    while monotonic() < deadline:
        pods = ready_pods(kube, role)
        if len(pods) >= minimum and (not old_uids or set(pods) - set(old_uids)):
            return pods
        sleep(0.5)
    raise TimeoutError(f"{role} did not reach {minimum} ready replicas")


def kube_balance(api, kube, requests):
    pods = ready_pods(kube, "story-api")
    if len(pods) < 2:
        raise RuntimeError("distribution test requires two ready API pods")
    token = uuid4().hex
    for _ in range(requests):
        api.call(f"/health?wp6_probe={token}")
    deadline = monotonic() + 5
    while True:
        counts = {
            name: sum(token in line for line in kube("logs", name, "-c", "api", "--since=1m").splitlines())
            for name in pods.values()
        }
        if sum(counts.values()) == requests or monotonic() >= deadline:
            break
        sleep(0.2)
    if sum(counts.values()) != requests or sum(count > 0 for count in counts.values()) < 2:
        raise RuntimeError(f"API pod logs did not confirm distribution: {counts}")
    return {"requests": requests, "requests_by_pod": counts}


def kube_failover(api, kube, role):
    desired = deployment_replicas(kube, role)
    baseline = ready_pods(kube, role)
    if desired < 2 or len(baseline) < desired:
        raise RuntimeError("failure test requires at least two ready replicas before deletion")
    api.call("/ready")
    story = api.story()
    victim = next(iter(baseline.values()))
    stop = Event()
    probes = []
    started = monotonic()

    def monitor():
        while not stop.is_set():
            at = round(monotonic() - started, 3)
            try:
                api.call("/ready")
                probes.append({"after_s": at, "ready": True})
            except (OSError, RuntimeError, ValueError):
                probes.append({"after_s": at, "ready": False})
            stop.wait(api.poll_interval)

    def generate():
        before = monotonic()
        job_id = api.submit(story, 0)["job_id"]
        job, _ = api.wait(job_id)
        return job["status"], round(monotonic() - before, 3), job["attempts"]

    with ThreadPoolExecutor(max_workers=2) as pool:
        watch = pool.submit(monitor)
        try:
            kube("delete", "pod", victim, "--wait=false")
            job = pool.submit(generate)
            replacement = wait_for_replicas(kube, role, desired, started + api.timeout,
                                            old_uids=baseline)
            replacement_seconds = round(monotonic() - started, 3)
            status, job_seconds, attempts = job.result()
        finally:
            stop.set()
            watch.result()
    if status != "completed":
        raise RuntimeError(f"job during pod failure became {status}")
    return {
        "deployment": role, "deleted_pod": victim, "ready_before": sorted(baseline.values()),
        "ready_after": sorted(replacement.values()), "replacement_ready_s": replacement_seconds,
        "generation_status": status, "generation_completion_s": job_seconds,
        "generation_attempts": attempts, "committed_version": api.verify_story(story, 1),
        "readiness_failures": sum(not sample["ready"] for sample in probes), "readiness_samples": probes,
    }


def kube_scale(api, kube, role, replicas):
    initial = deployment_replicas(kube, role)
    autoscaled = json.loads(kube("get", "hpa", "-o", "json"))["items"]
    if any(hpa["spec"]["scaleTargetRef"]["name"] == role for hpa in autoscaled):
        raise RuntimeError(f"remove the HPA for {role} from the test namespace before manual scaling")
    if replicas <= initial:
        raise ValueError("target replicas must exceed the deployment's current replica count")
    wait_for_replicas(kube, role, initial, monotonic() + api.timeout)
    started = monotonic()
    try:
        kube("scale", f"deployment/{role}", f"--replicas={replicas}")
        ready = wait_for_replicas(kube, role, replicas, started + api.timeout)
        scale_seconds = round(monotonic() - started, 3)
        results = {"deployment": role, "initial_replicas": initial, "target_replicas": replicas,
                   "ready_replicas": sorted(ready.values()), "scale_ready_s": scale_seconds}
        try:
            results["top_pods"] = kube("top", "pods", "-l", f"app={role}").splitlines()
        except RuntimeError:
            results["top_pods"] = None
        results["service_check"] = api.call("/ready")
    finally:
        kube("scale", f"deployment/{role}", f"--replicas={initial}")
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--timeout", type=float, default=60)
    parser.add_argument("--output", type=Path)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("compose-balance", "compose-crash", "compose-scale"):
        sub = commands.add_parser(name)
        sub.add_argument("--project", required=True)
        if name == "compose-balance":
            sub.add_argument("--requests", type=int, default=24)
    for name in ("k8s-failover", "k8s-scale", "k8s-balance"):
        sub = commands.add_parser(name)
        sub.add_argument("--context", required=True)
        sub.add_argument("--namespace", required=True)
        if name == "k8s-balance":
            sub.add_argument("--requests", type=int, default=40)
        else:
            sub.add_argument("--deployment", choices=ROLES, required=True)
        if name == "k8s-scale":
            sub.add_argument("--replicas", type=int, required=True)
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("timeout must be positive")
    if args.command.endswith("-balance") and args.requests < 2:
        parser.error("at least two requests are required")
    api = StoryAPI(args.url, timeout=args.timeout)
    if args.command == "compose-balance":
        result = compose_balance(api, args.project, args.requests)
    elif args.command == "compose-crash":
        result = compose_crash(api, args.project)
    elif args.command == "compose-scale":
        result = compose_scale(api, args.project)
    else:
        kube = kube_client(args.context, args.namespace)
        if args.command == "k8s-failover":
            result = kube_failover(api, kube, args.deployment)
        elif args.command == "k8s-scale":
            result = kube_scale(api, kube, args.deployment, args.replicas)
        else:
            result = kube_balance(api, kube, args.requests)
        result.update(context=args.context, namespace=args.namespace)
    write_result({"measured_at_utc": datetime.now(timezone.utc).isoformat(), "url": args.url,
                  "exercise": args.command, **result}, args.output)


if __name__ == "__main__":
    main()

"""Chaos test (CLAUDE.md Phase 3 DoD): starts a job, kills the worker
process partway through, asserts the job completes exactly once via
another worker.

What this actually does:
  1. Ensures at least 2 worker replicas are running.
  2. Submits a long-running `sleep` job and waits for a worker to claim it.
  3. Temporarily disables that specific container's restart policy, then
     `docker kill`s it (SIGKILL — immediate, no graceful shutdown, no
     chance to ack or nack) to simulate a hard crash mid-job.
  4. Waits for the job to reach a terminal state and asserts:
       - it succeeded (the work wasn't lost)
       - `claim_count` is >= 2 (proof a second claim actually happened —
         retry_count would NOT show this, since a killed process never
         raises the exception that increments it; see worker/main.py)
       - `claimed_by` names a *different* worker than the one killed
         (restart policy was disabled specifically so the killed
         container can't win the race back against its own restart)
  5. Restores the killed container's restart policy and restarts it, so
     the stack is left in its normal running state.

Requires: `docker compose up` already running, reachable at
http://localhost:8000, and the `docker` CLI on PATH. Run from anywhere:

    pip install -r chaos/requirements.txt
    python chaos/kill_worker_midjob.py
"""
import subprocess
import sys
import time

import httpx

API_BASE_URL = "http://localhost:8000"
JOB_DURATION_SECONDS = 8
CLAIM_TIMEOUT_SECONDS = 10
COMPLETION_TIMEOUT_SECONDS = 40


def _run(*args: str) -> str:
    result = subprocess.run(["docker", *args], capture_output=True, text=True, check=True)
    return result.stdout.strip()


def _worker_container_name_for_hostname(hostname: str) -> str:
    """Docker sets a container's hostname to its own short ID by default
    (confirmed: `docker ps` short IDs match `claimed_by` values exactly),
    so this just maps one back to the other.
    """
    output = _run("ps", "--filter", "name=job-platform-worker", "--format", "{{.ID}} {{.Names}}")
    for line in output.splitlines():
        container_id, name = line.split(maxsplit=1)
        if hostname == container_id or hostname.startswith(container_id):
            return name
    raise RuntimeError(f"no running worker container matches hostname {hostname!r}")


def _ensure_at_least_two_workers() -> None:
    output = _run("ps", "--filter", "name=job-platform-worker", "--format", "{{.Names}}")
    worker_count = len([line for line in output.splitlines() if line.strip()])
    if worker_count < 2:
        print(f"only {worker_count} worker replica(s) running - scaling to 2...")
        subprocess.run(["docker", "compose", "up", "-d", "--scale", "worker=2"], check=True)
        time.sleep(3)


def _wait_for(client: httpx.Client, job_id: str, predicate, timeout: float) -> dict:
    deadline = time.time() + timeout
    job: dict = {}
    while time.time() < deadline:
        job = client.get(f"/jobs/{job_id}").json()
        if predicate(job):
            return job
        time.sleep(0.3)
    return job


def main() -> int:
    _ensure_at_least_two_workers()

    with httpx.Client(base_url=API_BASE_URL, timeout=10.0) as client:
        print(f"submitting a {JOB_DURATION_SECONDS}s sleep job...")
        resp = client.post("/jobs", json={"type": "sleep", "payload": {"seconds": JOB_DURATION_SECONDS}})
        resp.raise_for_status()
        job_id = resp.json()["id"]
        print(f"job {job_id} submitted")

        job = _wait_for(
            client, job_id,
            lambda j: j["status"] == "running" and j.get("claimed_by"),
            CLAIM_TIMEOUT_SECONDS,
        )
        original_claimant = job.get("claimed_by")
        if not original_claimant:
            print("FAIL: no worker claimed the job in time")
            return 1
        print(f"job claimed by worker hostname={original_claimant}")

        container_name = _worker_container_name_for_hostname(original_claimant)

        print(f"disabling restart policy on {container_name} (so it can't race back in)...")
        _run("update", "--restart=no", container_name)

        print(f"docker kill {container_name} - simulating a hard crash mid-job...")
        _run("kill", container_name)

        try:
            print("waiting for the job to complete via a different worker...")
            final = _wait_for(
                client, job_id,
                lambda j: j["status"] in ("succeeded", "failed", "dead_letter"),
                COMPLETION_TIMEOUT_SECONDS,
            )
        finally:
            print(f"restoring restart policy on {container_name} and starting it back up...")
            _run("update", "--restart=unless-stopped", container_name)
            subprocess.run(["docker", "start", container_name], capture_output=True, check=False)

        print(
            f"final: status={final.get('status')} claimed_by={final.get('claimed_by')} "
            f"claim_count={final.get('claim_count')} retry_count={final.get('retry_count')}"
        )

        failures = []
        if final.get("status") != "succeeded":
            failures.append(f"expected status=succeeded, got {final.get('status')!r}")
        if (final.get("claim_count") or 0) < 2:
            failures.append(
                f"expected claim_count >= 2 (proof of a reclaim after the kill), got {final.get('claim_count')!r}"
            )
        if final.get("claimed_by") == original_claimant:
            failures.append(
                "expected a different worker to complete the job, but claimed_by is unchanged "
                f"({original_claimant!r}) - restart-policy disable may not have taken effect in time"
            )

        if failures:
            print("FAIL:")
            for f in failures:
                print(f"  - {f}")
            return 1

        print(
            "PASS: job completed exactly once, via a different worker, "
            "after the original was killed mid-job"
        )
        return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""
fusion-council-backend integration test harness.
Runs API + worker in Docker Compose, creates a run, polls for completion, validates answer.

Usage:
  python3 /home/moltbot/repositories/fusion-council-backend/scripts/integration_test.py

Exit codes:
  0 — pass
  1 — fail
  2 — error (setup/infrastructure)
"""

import json
import subprocess
import sys
import time
import httpx

COMPOSE_DIR = "/home/moltbot/repositories/fusion-council-backend"
API_URL = "http://localhost:8080"
HEALTH_URL = f"{API_URL}/healthz"
AUTH_HEADER = {"Authorization": "Bearer dev-key-1", "Content-Type": "application/json"}
MAX_WAIT_SECONDS = 120
POLL_INTERVAL = 2


def run(cmd: str, check=True, capture=True) -> subprocess.CompletedProcess:
    """Run a shell command."""
    return subprocess.run(
        cmd, shell=True, cwd=COMPOSE_DIR,
        capture_output=capture, text=True, check=check
    )


def wait_for_health(max_wait=60):
    """Block until API healthz returns 200."""
    start = time.time()
    while time.time() - start < max_wait:
        try:
            r = httpx.get(HEALTH_URL, timeout=5)
            if r.status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(2)
    return False


def create_run(mode="single", prompt="What is 2+2?", max_tokens=256):
    """Create a run via the API. Returns run_id or None."""
    payload = {
        "mode": mode,
        "prompt": prompt,
        "max_output_tokens": max_tokens,
    }
    try:
        r = httpx.post(f"{API_URL}/v1/runs", headers=AUTH_HEADER, json=payload, timeout=10)
        if r.status_code == 201:
            return r.json().get("run_id")
        print(f"FAIL: create run returned {r.status_code}: {r.text[:200]}")
        return None
    except Exception as e:
        print(f"FAIL: create run error: {e}")
        return None


def poll_run(run_id: str):
    """Poll until run reaches terminal state. Returns final run JSON or None."""
    start = time.time()
    while time.time() - start < MAX_WAIT_SECONDS:
        try:
            r = httpx.get(f"{API_URL}/v1/runs/{run_id}", headers=AUTH_HEADER, timeout=10)
            if r.status_code == 200:
                data = r.json()
                status = data.get("status", "")
                if status in ("succeeded", "failed", "cancelled"):
                    return data
        except Exception:
            pass
        time.sleep(POLL_INTERVAL)
    print(f"FAIL: run {run_id} did not complete within {MAX_WAIT_SECONDS}s")
    return None


def main():
    print("=" * 60)
    print("Fusion Council Backend — Integration Test (Docker Compose)")
    print("=" * 60)

    # Step 1: Build and start
    print("\n[1/5] Building and starting Docker Compose...")
    result = run("docker compose build 2>&1", check=False)
    if result.returncode != 0:
        print(f"FAIL: docker compose build failed:\n{result.stderr[-500:]}")
        return 2

    result = run("docker compose up -d 2>&1", check=False)
    if result.returncode != 0:
        print(f"FAIL: docker compose up failed:\n{result.stderr[-500:]}")
        return 2

    # Step 2: Wait for health
    print("[2/5] Waiting for API health check...")
    if not wait_for_health():
        print("FAIL: API did not become healthy within 60s")
        run("docker compose logs --tail=20 2>&1", check=False)
        return 2
    print("  API healthy ✓")

    # Step 3: Create run
    print("[3/5] Creating single-mode run (prompt: 'What is 2+2?')...")
    run_id = create_run(mode="single", prompt="What is 2+2?", max_tokens=256)
    if not run_id:
        return 1
    print(f"  Run created: {run_id}")

    # Step 4: Poll for completion
    print("[4/5] Polling for completion (max 120s)...")
    final = poll_run(run_id)
    if not final:
        return 1

    status = final.get("status", "")
    answer = final.get("final_answer", "")
    print(f"  Status: {status}")
    print(f"  Answer: {answer}")

    # Step 5: Validate
    print("[5/5] Validating result...")
    if status != "succeeded":
        print(f"FAIL: run status is '{status}', expected 'succeeded'")
        return 1
    if not answer or not answer.strip():
        print("FAIL: final_answer is empty — likely max_tokens too low for thinking model")
        return 1
    if "4" not in answer:
        print(f"FAIL: answer '{answer}' doesn't contain expected '4'")
        return 1

    print("\n✅ Integration test PASSED")
    print(f"   Provider returned: '{answer}'")
    return 0


if __name__ == "__main__":
    try:
        code = main()
    except Exception as e:
        print(f"ERROR: {e}")
        code = 2
    finally:
        # Always tear down
        print("\n[cleanup] Stopping Docker Compose...")
        run("docker compose down -v 2>&1", check=False)
    sys.exit(code)
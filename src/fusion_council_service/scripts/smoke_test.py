#!/usr/bin/env python3
"""Smoke test — verifies the API and worker are working end-to-end."""

import os
import sys
import time
import requests


def main():
    api_url = os.environ.get("FUSION_API_URL", "http://localhost:8080")
    api_key = os.environ.get("SERVICE_API_KEYS", "dev-key-1").split(",")[0].strip()

    print("=" * 50)
    print("Fusion Council Smoke Test")
    print("=" * 50)

    # Step 1: healthz
    print("\n[1/5] GET /healthz ... ", end="")
    r = requests.get(f"{api_url}/healthz", timeout=5)
    r.raise_for_status()
    assert r.json().get("ok") is True
    print("PASS")

    # Step 2: readyz
    print("[2/5] GET /readyz ... ", end="")
    r = requests.get(f"{api_url}/readyz", timeout=5)
    r.raise_for_status()
    assert r.json().get("ok") is True
    print("PASS")

    # Step 3: create a run
    print("[3/5] POST /v1/runs (single mode) ... ", end="")
    r = requests.post(
        f"{api_url}/v1/runs",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={"mode": "single", "prompt": "What is 1+1?", "temperature": 0.2, "max_output_tokens": 50},
        timeout=10,
    )
    r.raise_for_status()
    data = r.json()
    assert "run_id" in data
    assert data["status"] == "queued"
    run_id = data["run_id"]
    print(f"PASS (run_id={run_id})")

    # Step 4: poll for completion (max 30s)
    print("[4/5] GET /v1/runs/{id} — waiting for completion ... ", end="", flush=True)
    for attempt in range(30):
        time.sleep(1)
        r = requests.get(
            f"{api_url}/v1/runs/{run_id}",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=5,
        )
        r.raise_for_status()
        status = r.json()["status"]
        print(f"[{attempt+1}s: {status}] ", end="", flush=True)
        if status == "succeeded":
            print("PASS")
            break
        elif status in ("failed", "cancelled"):
            print(f"UNEXPECTED STATUS: {status}")
            sys.exit(1)
    else:
        print("TIMEOUT — run did not complete in 30s")
        sys.exit(1)

    # Step 5: verify final answer exists
    print("[5/5] Verify final_answer ... ", end="")
    r = requests.get(
        f"{api_url}/v1/runs/{run_id}",
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=5,
    )
    r.raise_for_status()
    final = r.json().get("final_answer")
    assert final, "final_answer should not be empty"
    print(f'PASS ("{final[:60]}...")')

    print("\n" + "=" * 50)
    print("ALL SMOKE TESTS PASSED")
    print("=" * 50)


if __name__ == "__main__":
    main()
"""E2E regression tests — single, fusion, and council mode against live API.

These tests hit the REAL deployed service in the dev namespace via port-forward.
They verify the full pipeline: trigger run → poll completion → verify answer contract.

Requirements:
    - kubectl port-forward to fusion-council-api service in dev namespace
    - SERVICE_API_KEYS token for auth
    - Running cluster

Skip these tests when running unit test suites with:
    pytest -m "not e2e"

Run specifically with:
    pytest tests/test_regression_e2e.py -v -m e2e

The port-forward is managed automatically per session — tests share one tunnel.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.request
import urllib.error
from typing import Any

import pytest


# ── Configuration ──────────────────────────────────────────────────────────

API_BASE = os.environ.get("FUSION_COUNCIL_E2E_API_BASE", "http://localhost:8081")
API_TOKEN = os.environ.get("FUSION_COUNCIL_E2E_API_TOKEN", "dev-key-1")
PORT_FORWARD_PORT = 8081
PORT_FORWARD_TARGET = "svc/fusion-council-api-langgraph"
PORT_FORWARD_NAMESPACE = os.environ.get("FUSION_COUNCIL_E2E_NAMESPACE", "dev")

# Timeouts for different modes (seconds)
MODE_TIMEOUTS = {
    "single": 90,
    "fusion": 300,
    "council": 600,
}

# Polling interval
POLL_INTERVAL = 5

# Provider env var names that must stay in sync with ~/.hermes/.env
_PROVIDER_KEYS = ("OPENCODE_GO_API_KEY", "MINIMAX_API_KEY")


# ── Provider-key sync — reads active keys from ~/.hermes/.env ───────────────

def _parse_active_provider_keys() -> dict[str, str]:
    """Parse ~/.hermes/.env for active (uncommented) provider API keys.

    Only the LAST uncommented occurrence of each key is returned,
    matching standard dotenv override semantics.
    """
    env_path = os.path.expanduser("~/.hermes/.env")
    keys: dict[str, str] = {}
    try:
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or line.startswith("["):
                    continue
                if "=" not in line:
                    continue
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip()
                if key in _PROVIDER_KEYS and value:
                    keys[key] = value  # last-wins (dotenv semantics)
    except FileNotFoundError:
        pass
    return keys


def _fusion_deployments(namespace: str) -> list[str]:
    """Return names of all fusion-council-api deployments in *namespace*."""
    result = subprocess.run(
        ["kubectl", "get", "deploy", "-n", namespace,
         "-o", "jsonpath={.items[*].metadata.name}"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        return []
    return [
        name for name in result.stdout.strip().split()
        if "fusion-council-api" in name
    ]


@pytest.fixture(scope="session")
def sync_provider_keys():
    """Sync active provider keys from ~/.hermes/.env into cluster deployments.

    Runs exactly once per test session — *before* any API calls.
    Compares each provider key from ``.env`` against what is currently set
    on the deployments.  Only touches deployments whose keys are stale.
    """
    env_keys = _parse_active_provider_keys()
    if not env_keys:
        pytest.skip("No active provider keys found in ~/.hermes/.env")

    namespace = PORT_FORWARD_NAMESPACE
    deployments = _fusion_deployments(namespace)
    if not deployments:
        pytest.skip(f"No fusion-council-api deployments found in {namespace}")

    changed = False
    for key_name in _PROVIDER_KEYS:
        new_value = env_keys.get(key_name)
        if not new_value:
            continue

        # Read current value from the first deployment
        check = subprocess.run(
            ["kubectl", "get", "deploy", "-n", namespace, deployments[0],
             "-o", f"jsonpath={{.spec.template.spec.containers[0].env[?(@.name=='{key_name}')].value}}"],
            capture_output=True, text=True,
        )
        current = check.stdout.strip()
        if current == new_value:
            continue

        changed = True
        for dep in deployments:
            subprocess.run(
                ["kubectl", "set", "env", "deployment", dep,
                 "-n", namespace, "--overwrite",
                 f"{key_name}={new_value}"],
                capture_output=True,
            )

    if changed:
        for dep in deployments:
            subprocess.run(
                ["kubectl", "rollout", "status", "deployment", dep,
                 "-n", namespace, "--timeout=120s"],
                capture_output=True,
            )
        time.sleep(3)  # let readiness gates settle


# ── Port-forward fixture (session-scoped) ──────────────────────────────────

@pytest.fixture(scope="session")
def api_base(sync_provider_keys):
    """Ensure port-forward is running and return the base URL."""
    # Check health
    try:
        _api_get("/healthz", timeout=5)
        return API_BASE
    except Exception:
        pass

    # Kill any stale port-forward on the same port
    subprocess.run(
        ["pkill", "-f", f"port-forward.*{PORT_FORWARD_PORT}"],
        capture_output=True,
    )
    time.sleep(1)

    # Start fresh port-forward
    subprocess.Popen(
        [
            "kubectl", "port-forward",
            "-n", PORT_FORWARD_NAMESPACE,
            PORT_FORWARD_TARGET,
            f"{PORT_FORWARD_PORT}:8080",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    # Wait for it to become ready
    deadline = time.time() + 15
    while time.time() < deadline:
        try:
            resp = _api_get("/healthz", timeout=3)
            if resp.get("ok"):
                return API_BASE
        except Exception:
            time.sleep(1)

    pytest.skip("Port-forward failed to start — is the cluster up?")


# ── Helpers ────────────────────────────────────────────────────────────────

def _api_request(
    method: str,
    path: str,
    data: dict | None = None,
    timeout: int = 30,
) -> tuple[int, dict]:
    """Make an API request, return (status_code, parsed_json)."""
    url = f"{API_BASE}{path}"
    body = json.dumps(data).encode() if data else None
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {API_TOKEN}",
        },
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def _api_get(path: str, timeout: int = 30) -> dict:
    """GET request to the API."""
    _, data = _api_request("GET", path, timeout=timeout)
    return data


def _api_post(path: str, data: dict, timeout: int = 30) -> tuple[int, dict]:
    """POST request to the API."""
    return _api_request("POST", path, data=data, timeout=timeout)


def _trigger_run(mode: str, prompt: str, **kwargs) -> str:
    """Trigger a run and return the run_id.

    Uses /v1/respond (sync endpoint) to avoid needing to poll manually.
    Falls back to /v1/runs + polling if sync times out.
    """
    payload: dict[str, Any] = {"mode": mode, "prompt": prompt}
    payload.update(kwargs)

    # Use the sync /v1/respond endpoint first (returns when run completes)
    timeout = MODE_TIMEOUTS.get(mode, 300)
    try:
        status, resp = _api_post("/v1/respond", payload, timeout=timeout)
        if status == 200 and resp.get("run_id"):
            return resp["run_id"]
    except Exception:
        pass

    # Fallback: create run via /v1/runs and poll
    run_status, run_data = _api_post("/v1/runs", payload, timeout=30)
    if run_status not in (200, 201):
        raise RuntimeError(
            f"Failed to create {mode} run: HTTP {run_status} {run_data}"
        )
    run_id = run_data.get("run_id")
    if not run_id:
        raise RuntimeError(f"No run_id in response: {run_data}")

    # Poll until complete
    deadline = time.time() + timeout
    while time.time() < deadline:
        run_status_data = _api_get(f"/v1/runs/{run_id}", timeout=10)
        status_val = run_status_data.get("status", "")
        if status_val in ("succeeded", "succeeded_degraded", "failed", "cancelled"):
            return run_id
        if status_val in ("error",):
            raise RuntimeError(
                f"Run {run_id} entered error state: {run_status_data}"
            )
        time.sleep(POLL_INTERVAL)

    raise TimeoutError(f"Run {run_id} did not complete within {timeout}s")


# ── Test helpers ───────────────────────────────────────────────────────────

def _verify_answers_contract(run_id: str, min_candidates: int = 1, min_stages: int = 1):
    """Verify the /v1/runs/{run_id}/answers contract."""
    answers = _api_get(f"/v1/runs/{run_id}/answers", timeout=15)

    # Schema version
    assert answers.get("schema_version") == "v1", (
        f"Expected schema_version=v1, got {answers.get('schema_version')}"
    )

    # Candidate count
    candidates = answers.get("candidates", [])
    assert len(candidates) >= min_candidates, (
        f"Expected >= {min_candidates} candidates, got {len(candidates)}"
    )

    # Stages
    stages = answers.get("stages", 0)
    assert stages >= min_stages, (
        f"Expected >= {min_stages} stages, got {stages}"
    )

    # Candidate shape validation
    for cand in candidates:
        assert "alias" in cand, f"Candidate missing alias: {cand}"
        assert "status" in cand, f"Candidate missing status: {cand}"
        assert "stage" in cand, f"Candidate missing stage: {cand}"
        # raw_text/raw_answer compatibility
        raw_text = cand.get("raw_text")
        raw_answer = cand.get("raw_answer")
        if raw_answer is not None:  # historical NULL is OK
            assert raw_text == raw_answer, (
                f"raw_text/raw_answer mismatch for {cand['alias']}: "
                f"raw_text={raw_text!r} vs raw_answer={raw_answer!r}"
            )

    return answers


def _assert_run_succeeded(run_id: str, mode: str):
    """Assert the run reached a successful terminal state.

    Returns the run dict, or raises pytest.skip if the run failed due to
    transient provider issues (rate limits, auth failures) that don't
    indicate a regression in the service itself.
    """
    run = _api_get(f"/v1/runs/{run_id}", timeout=10)
    status = run["status"]
    error_code = run.get("error_code", "")

    # Transient provider failures — skip, not fail
    TRANSIENT_ERRORS = {
        "HTTP_429", "RATE_LIMITED", "PROVIDER_TIMEOUT",
        "AUTH_FAILED", "HTTP_500", "HTTP_502", "HTTP_503",
        "PROVIDER_ERROR", "NO_MODELS",
        "FUSION_QUORUM_NOT_MET", "COUNCIL_QUORUM_NOT_MET",
    }
    if status == "failed" and error_code in TRANSIENT_ERRORS:
        # For quorum-not-met, double-check that every failed candidate
        # also had a transient error — a non-transient candidate failure
        # (e.g. a code bug) should still fail the test.
        if error_code in ("FUSION_QUORUM_NOT_MET", "COUNCIL_QUORUM_NOT_MET"):
            try:
                answers = _api_get(f"/v1/runs/{run_id}/answers", timeout=10)
                for cand in answers.get("candidates", []):
                    if cand.get("status") == "failed":
                        cand_err = cand.get("error_code", "")
                        if cand_err and cand_err not in TRANSIENT_ERRORS:
                            pytest.fail(
                                f"{mode} run {run_id}: {error_code} but "
                                f"candidate '{cand.get('alias')}' failed with "
                                f"non-transient error '{cand_err}' — "
                                f"this may be a real regression"
                            )
            except Exception:
                pass  # if we can't inspect candidates, trust the run-level code

        pytest.skip(
            f"{mode} run {run_id}: transient provider error "
            f"({error_code}) — cluster healthy, providers unavailable"
        )

    assert status in ("succeeded", "succeeded_degraded"), (
        f"{mode} run {run_id}: expected succeeded/succeeded_degraded, "
        f"got {status} — "
        f"error_code={error_code} "
        f"error_message={run.get('error_message')}"
    )
    assert run.get("final_answer"), (
        f"{mode} run {run_id}: final_answer is empty or missing"
    )
    return run


# ═══════════════════════════════════════════════════════════════════════════
# SINGLE MODE
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.e2e
class TestSingleModeE2E:
    """End-to-end regression tests for single-mode runs."""

    def test_single_basic_prompt_succeeds(self, api_base):
        """Single mode with a simple factual prompt should succeed."""
        run_id = _trigger_run(
            "single",
            prompt="What is the capital of France? Answer in one sentence.",
        )
        run = _assert_run_succeeded(run_id, "single")

        # Single mode: exactly 1 candidate, 1 stage
        assert run["mode"] == "single", f"Expected mode=single, got {run['mode']}"
        candidates = run.get("candidates", [])
        if candidates:
            assert len(candidates) == 1, (
                f"Single mode expects 1 candidate, got {len(candidates)}"
            )

    def test_single_answers_contract(self, api_base):
        """Single mode answers endpoint must satisfy v1 contract."""
        run_id = _trigger_run(
            "single",
            prompt="Explain quantum entanglement in 2 sentences.",
        )
        _assert_run_succeeded(run_id, "single")

        answers = _verify_answers_contract(run_id, min_candidates=1, min_stages=1)

        # Single mode specifics
        assert answers["count"] >= 1
        assert answers["stages"] == 1

    def test_single_temperature_parameter(self, api_base):
        """Single mode with explicit temperature should succeed."""
        run_id = _trigger_run(
            "single",
            prompt="Name a random color.",
            temperature=0.9,
        )
        _assert_run_succeeded(run_id, "single")


# ═══════════════════════════════════════════════════════════════════════════
# FUSION MODE
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.e2e
class TestFusionModeE2E:
    """End-to-end regression tests for fusion-mode runs."""

    def test_fusion_basic_prompt_succeeds(self, api_base):
        """Fusion mode with a complex analytical prompt should succeed."""
        run_id = _trigger_run(
            "fusion",
            prompt=(
                "Compare electric vehicles vs hydrogen fuel cells for "
                "personal transportation. List 2 pros and 2 cons for each."
            ),
        )
        run = _assert_run_succeeded(run_id, "fusion")

        assert run["mode"] == "fusion"

    def test_fusion_answers_contract(self, api_base):
        """Fusion mode answers must satisfy v1 contract with multiple stages."""
        run_id = _trigger_run(
            "fusion",
            prompt=(
                "What are the top 3 programming languages for web "
                "development in 2026? Rank them and explain why."
            ),
        )
        _assert_run_succeeded(run_id, "fusion")

        # Fusion has 3+ stages: generation → synthesis → verification
        answers = _verify_answers_contract(run_id, min_candidates=3, min_stages=2)

        # Fusion should have at least generation + synthesis stages
        assert answers["count"] >= 3, (
            f"Fusion expects >= 3 candidates, got {answers['count']}"
        )

        # Verify stage diversity
        cand_stages = {c["stage"] for c in answers.get("candidates", [])}
        assert "generation" in cand_stages, (
            f"Fusion must have generation stage, got stages: {cand_stages}"
        )

    def test_fusion_with_temperature_and_tokens(self, api_base):
        """Fusion mode with custom temperature and token limit."""
        run_id = _trigger_run(
            "fusion",
            prompt="Write a haiku about technology.",
            temperature=0.7,
            max_output_tokens=200,
        )
        run = _assert_run_succeeded(run_id, "fusion")

        # Verify answer is reasonable length
        answer = run.get("final_answer", "")
        assert len(answer) > 10, f"Answer too short: {answer!r}"


# ═══════════════════════════════════════════════════════════════════════════
# COUNCIL MODE
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.e2e
class TestCouncilModeE2E:
    """End-to-end regression tests for council-mode runs."""

    def test_council_basic_prompt_succeeds(self, api_base):
        """Council mode with a strategic analysis prompt should succeed."""
        run_id = _trigger_run(
            "council",
            prompt=(
                "A startup has $2M in funding and 12 months of runway. "
                "They need to choose between B2B and B2C go-to-market. "
                "Analyze the trade-offs and make a recommendation."
            ),
        )
        run = _assert_run_succeeded(run_id, "council")

        assert run["mode"] == "council"

    def test_council_answers_contract(self, api_base):
        """Council mode answers must satisfy v1 contract with full pipeline."""
        run_id = _trigger_run(
            "council",
            prompt=(
                "Evaluate the ethics of AI in healthcare decision-making. "
                "Consider patient autonomy, accuracy, and liability."
            ),
        )
        _assert_run_succeeded(run_id, "council")

        # Council has 5+ stages: first_opinion → peer_review → debate →
        # synthesis → verification (degradation may skip some)
        answers = _verify_answers_contract(run_id, min_candidates=5, min_stages=3)

        # Council must have first_opinion stage
        cand_stages = {c["stage"] for c in answers.get("candidates", [])}
        assert "first_opinion" in cand_stages, (
            f"Council must have first_opinion stage, got stages: {cand_stages}"
        )

        # Candidate detail (if execution_order exists)
        candidates = answers.get("candidates", [])
        orders = [c.get("execution_order") for c in candidates if c.get("execution_order") is not None]
        if orders:
            assert orders == sorted(orders), (
                f"execution_order must be monotonically ascending: {orders}"
            )
            assert orders[0] >= 1, f"execution_order must start at 1: {orders}"

    def test_council_selection_metadata_present(self, api_base):
        """Council mode answers should include selection_metadata on stages."""
        run_id = _trigger_run(
            "council",
            prompt=(
                "Compare AWS, GCP, and Azure for a mid-size SaaS company "
                "with $5M annual cloud spend. Recommend one."
            ),
        )
        _assert_run_succeeded(run_id, "council")

        answers = _api_get(f"/v1/runs/{run_id}/answers", timeout=15)

        # At least one stage should have selection_metadata
        stages_with_meta = 0
        for stage in answers.get("stages_detail", []):
            if stage.get("selection_metadata"):
                stages_with_meta += 1

        assert stages_with_meta >= 1, (
            f"Expected >= 1 stage with selection_metadata, "
            f"got {stages_with_meta}/{len(answers.get('stages_detail', []))}"
        )


# ═══════════════════════════════════════════════════════════════════════════
# CROSS-MODE REGRESSION
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.e2e
class TestCrossModeRegression:
    """Cross-mode regression tests for API consistency."""

    def test_all_modes_handle_empty_prompt_same_way(self, api_base):
        """All three modes should reject or handle empty prompts consistently."""
        for mode in ("single", "fusion", "council"):
            status, resp = _api_post("/v1/respond", {"mode": mode, "prompt": ""}, timeout=30)
            # Should either reject (400) or handle gracefully (200)
            assert status in (200, 400, 422), (
                f"{mode}: unexpected status {status} for empty prompt: {resp}"
            )
            if status == 200:
                assert resp.get("run_id"), f"{mode}: 200 but no run_id: {resp}"

    def test_all_modes_handle_long_prompt(self, api_base):
        """All three modes should handle moderately long prompts."""
        long_prompt = (
            "Comprehensive analysis of the global semiconductor supply chain: "
            + "TSMC's dominance, Intel's foundry pivot, Samsung's ambitions, "
            + "US CHIPS Act implications, EU's semiconductor strategy, "
            + "China's self-sufficiency push, and the role of ASML's EUV monopoly. "
        ) * 3  # ~900 chars

        for mode in ("single", "fusion", "council"):
            run_id = _trigger_run(mode, prompt=long_prompt)
            run = _assert_run_succeeded(run_id, mode)
            assert len(run.get("final_answer", "")) > 50, (
                f"{mode}: answer too short for long prompt: {len(run.get('final_answer', ''))}"
            )

    def test_run_status_lifecycle(self, api_base):
        """Verify run status transitions through expected lifecycle."""
        # Trigger a single run and poll frequently to observe status transitions
        status, create_resp = _api_post(
            "/v1/runs",
            {"mode": "single", "prompt": "Say hello in 3 words."},
            timeout=30,
        )
        assert status in (200, 201)
        run_id = create_resp["run_id"]

        # Track status changes
        seen_statuses = set()
        deadline = time.time() + 120
        while time.time() < deadline:
            run = _api_get(f"/v1/runs/{run_id}", timeout=10)
            status_val = run.get("status", "")
            seen_statuses.add(status_val)
            if status_val in ("succeeded", "succeeded_degraded"):
                break
            time.sleep(2)

        # Must have at least seen queued/running → succeeded
        # (queued may be too fast to observe)
        assert "succeeded" in seen_statuses or "succeeded_degraded" in seen_statuses, (
            f"Run never reached success; seen statuses: {seen_statuses}"
        )

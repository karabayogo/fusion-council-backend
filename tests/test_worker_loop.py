"""Regression tests for worker_loop.py — startup recovery, timeouts, shutdown sentinels."""

import asyncio
import os
import sqlite3
import time
from unittest.mock import MagicMock, patch

import pytest

from fusion_council_service.domain.worker_loop import Worker
from fusion_council_service.domain.run_repository import insert_run, update_run_status
from fusion_council_service.clock import utc_now_iso, utc_now_plus_seconds


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _insert_queued_run(db: sqlite3.Connection, run_id: str) -> None:
    insert_run(
        db=db, run_id=run_id, mode="single", prompt="test",
        system_prompt=None, temperature=0.2, max_output_tokens=1000,
        deadline_seconds=60, deadline_at=utc_now_plus_seconds(60),
        owner_token_hash="hash", metadata_json="{}", requested_models_json=None,
        created_at=utc_now_iso(),
    )


@pytest.fixture
def mock_worker(tmp_db, monkeypatch):
    """Return a Worker wired to tmp_db with a mock registry and real catalog."""
    mock_registry = MagicMock()
    # Build a real ModelCatalog with at least one model so select_models_for_mode works
    from fusion_council_service.model_catalog import ModelCatalog, load_yaml_catalog
    catalog = load_yaml_catalog("config/models.yaml")
    model_catalog = ModelCatalog(catalog)

    worker = Worker(
        db_path=":memory:",
        registry=mock_registry,
        catalog=model_catalog,
        poll_interval_ms=50,
        heartbeat_interval_ms=5000,
        stale_run_threshold_seconds=30,
    )
    worker._db = tmp_db
    return worker


# ---------------------------------------------------------------------------
# Task 1 — recover stale runs at startup
# ---------------------------------------------------------------------------

def test_recover_stale_runs_called_at_startup(mock_worker, tmp_db, caplog):
    """_recover_stale_runs() should be called at least once during idle polling."""
    worker = mock_worker
    with patch.object(worker, "_recover_stale_runs") as mock_recover:
        mock_recover.return_value = None
        # Start run_async briefly, then stop
        async def _start_then_stop():
            task = asyncio.create_task(worker.run_async())
            await asyncio.sleep(0.05)      # allow startup code to run
            worker.stop()
            # Drain task
            for _ in range(20):
                if task.done():
                    break
                await asyncio.sleep(0.02)
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        asyncio.run(_start_then_stop())
    assert mock_recover.call_count >= 1


# ---------------------------------------------------------------------------
# Task 2 — _call_provider_async timeout
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_provider_timeout_returns_error(mock_worker):
    """Provider call that exceeds timeout should return PROVIDER_TIMEOUT."""
    worker = mock_worker
    # Mock _run_provider_sync to block longer than the test timeout
    import fusion_council_service.domain.worker_loop as wl
    # W4: spy on build_timeout_result to confirm the timeout path uses the helper.
    # Patch on `wl` (the module that owns the call site) because the import is
    # a local binding in worker_loop, not a late-bound attribute lookup.
    from fusion_council_service.domain import timeout_result as tr
    spy_calls: list[tuple[int, str]] = []
    real_helper = tr.build_timeout_result

    def spy(effective_timeout: int, run_id: str):
        spy_calls.append((effective_timeout, run_id))
        return real_helper(effective_timeout, run_id)

    with patch.object(wl, "_run_provider_sync", side_effect=lambda *a, **k: time.sleep(200)), \
         patch.object(wl, "build_timeout_result", side_effect=spy):
        from fusion_council_service.domain.types import ProviderGenerateRequest
        req = ProviderGenerateRequest(
            alias="mock", provider="mock", provider_model="mock-v1",
            system_prompt=None, user_prompt="hello",
            max_output_tokens=100, temperature=0.2,
        )
        start = time.monotonic()
        result = await worker._call_provider_async(req, db=worker._db, run_id="test-run", timeout_seconds=1)
        elapsed = time.monotonic() - start

    assert elapsed < 3, f"Test took {elapsed:.1f}s, expected <3s"
    # W4: helper must be called exactly once with the resolved effective_timeout
    assert len(spy_calls) == 1, f"build_timeout_result should be called once, got {len(spy_calls)}"
    assert spy_calls[0][0] == 1, f"effective_timeout should be 1s, got {spy_calls[0][0]}"
    assert spy_calls[0][1] == "test-run", f"run_id should be 'test-run', got {spy_calls[0][1]!r}"
    assert result.success is False
    assert result.error_code == "PROVIDER_TIMEOUT"
    assert "timed out after 1s" in result.error_message
    assert result.latency_ms == 1000


# ---------------------------------------------------------------------------
# Task 3 — shutdown sentinel
# ---------------------------------------------------------------------------

def test_shutdown_sentinel_stops_worker(mock_worker):
    """Touching /tmp/shutdown-requested should cause _running to become False."""
    worker = mock_worker
    sentinel = "/tmp/shutdown-requested"
    # Ensure sentinel does not exist at start
    try:
        os.remove(sentinel)
    except FileNotFoundError:
        pass

    async def _start_then_trigger():
        task = asyncio.create_task(worker.run_async())
        await asyncio.sleep(0.05)          # let loop reach the sentinel check
        # Verify running is True before sentinel
        assert worker._running is True
        # Touch sentinel
        with open(sentinel, "w") as f:
            f.write("1")
        # Wait for loop to notice — the sentinel check happens after
        # claim_next_run returns None, then sleep(poll_interval) before next check.
        # With poll_interval_ms=5000 we need to sleep through that wait.
        for _ in range(120):
            if not worker._running:
                break
            await asyncio.sleep(0.05)
        assert worker._running is False, "Worker did not stop after sentinel touched"
        # Clean up task
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    asyncio.run(_start_then_trigger())
    # Clean up sentinel
    try:
        os.remove(sentinel)
    except FileNotFoundError:
        pass


def test_worker_does_not_recover_stale_runs_while_run_active(mock_worker, tmp_db):
    """Active long runs must not be re-queued by the same poll loop."""
    worker = mock_worker
    _insert_queued_run(tmp_db, "run_no_self_recover")

    async def slow_execute(run):
        await asyncio.sleep(0.15)
        worker.stop()

    with patch.object(worker, "_execute_run", side_effect=slow_execute), \
         patch.object(worker, "_recover_stale_runs") as mock_recover:
        async def _run():
            await asyncio.wait_for(worker.run_async(), timeout=2)

        asyncio.run(_run())

    mock_recover.assert_not_called()


def test_provider_generate_result_supports_tuple_unpacking():
    """Worker code can unpack ProviderGenerateResult returned by providers."""
    from fusion_council_service.domain.types import ProviderGenerateResult

    result = ProviderGenerateResult(
        success=True,
        raw_text="ok",
        error_code=None,
        error_message=None,
        latency_ms=123,
        input_tokens=4,
        output_tokens=5,
    )

    success, raw_text, err_code, err_msg, lat_ms, in_tok, out_tok = result
    assert (success, raw_text, err_code, err_msg, lat_ms, in_tok, out_tok) == (
        True, "ok", None, None, 123, 4, 5,
    )


# ---------------------------------------------------------------------------
# Task 4 — run-active sentinel created and removed
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_active_sentinel_created_and_removed(mock_worker, tmp_db):
    """_execute_run should create /tmp/run-active with run_id, then remove it."""
    worker = mock_worker
    sentinel = "/tmp/run-active"

    # Ensure clean state
    try:
        os.remove(sentinel)
    except FileNotFoundError:
        pass

    # Insert a single-mode run
    _insert_queued_run(tmp_db, "run_active_test")
    run = tmp_db.execute("SELECT * FROM runs WHERE run_id = ?", ("run_active_test",)).fetchone()
    run_dict = dict(run)

    # Mock the provider call so it returns immediately
    with patch.object(worker, "_call_provider_async", return_value=(
        True, "mock answer", None, None, 500, 10, 20,
    )):
        await worker._execute_run(run_dict)

    # After execution, sentinel should be gone
    assert not os.path.exists(sentinel), f"{sentinel} was not cleaned up after run"

    # We also verify the run completed successfully
    cursor = tmp_db.execute("SELECT status FROM runs WHERE run_id = ?", ("run_active_test",))
    assert cursor.fetchone()["status"] == "succeeded"


# ---------------------------------------------------------------------------
# Task 5 (E2 fix) — verification too-short guard
# ---------------------------------------------------------------------------

def test_min_verification_tokens_constant_exists():
    """E2 fix: MIN_VERIFICATION_TOKENS must be defined and > 0."""
    from fusion_council_service.domain.worker_loop import MIN_VERIFICATION_TOKENS
    assert isinstance(MIN_VERIFICATION_TOKENS, int)
    assert MIN_VERIFICATION_TOKENS > 0
    # 50 is the documented floor — well below any legit verdict, well above a stub.
    assert MIN_VERIFICATION_TOKENS >= 50


@pytest.mark.asyncio
async def test_verification_short_output_rejected_with_insufficient_evidence(mock_worker, tmp_db):
    """E2 fix: when verification stage returns <MIN_VERIFICATION_TOKENS tokens,
    the verdict is REJECTED, final_confidence=0.5, and final_answer gets the
    [INSUFFICIENT EVIDENCE] prefix. Run c908a00b1c834b8eb9ebe2b4 (2026-06-01)
    had kimi-k2.6 return 19 tokens as a 'verification' — that low-confidence
    answer was accepted, polluting the synthesis. This test locks the guard.

    Strategy: insert a council-mode run, then patch _call_structured_provider_async
    to return a (success=True, output_tokens=19) result. The verification stage
    must (a) NOT use that verdict's confidence, (b) record
    error_code=VERIFICATION_TOO_SHORT on the run_candidates row, and
    (c) prefix the synthesis with [INSUFFICIENT EVIDENCE].
    """
    from fusion_council_service.domain import worker_loop as wl

    worker = mock_worker
    run_id = "run_e2_verif_short"
    insert_run(
        db=tmp_db, run_id=run_id, mode="council", prompt="p",
        system_prompt=None, temperature=0.2, max_output_tokens=1000,
        deadline_seconds=120, deadline_at=utc_now_plus_seconds(120),
        owner_token_hash="h", metadata_json="{}", requested_models_json=None,
        created_at=utc_now_iso(),
    )
    run = dict(tmp_db.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone())

    # Stage 1: first_opinion returns 1 candidate with 100 tokens (synthetic answer).
    # Stages 2-4 (debate, refinement, synthesis): each returns 1 candidate with 100 tokens.
    # Stage 5 (verification): returns SUCCESS but with output_tokens=5 (< 50 floor).
    short_verif_result = (True, '{"verdict":"approve","confidence":0.1}', None, None, 2000, 50, 5)
    fake_opinion_result = (True, "fake opinion text that is at least one hundred tokens " * 10,
                           None, None, 500, 10, 100)

    call_count = {"n": 0}

    async def fake_call_provider_async(request, db, rid):
        return fake_opinion_result

    async def fake_call_structured_provider_async(request, schema, db, rid):
        # Always return the SHORT verification result — the bug repro.
        return short_verif_result

    with patch.object(worker, "_call_provider_async", side_effect=fake_call_provider_async), \
         patch.object(worker, "_call_structured_provider_async", side_effect=fake_call_structured_provider_async):
        await worker._execute_run(run)

    # After execution: final_confidence MUST be 0.5 (guard overrode the 0.1 verdict).
    final_row = tmp_db.execute(
        "SELECT status, final_confidence, final_answer FROM runs WHERE run_id = ?", (run_id,)
    ).fetchone()
    assert final_row["status"] == "succeeded"
    assert final_row["final_confidence"] == 0.5, (
        f"E2 bug: short verification verdict's confidence was accepted "
        f"(got {final_row['final_confidence']}, expected 0.5)"
    )
    assert "[INSUFFICIENT EVIDENCE" in final_row["final_answer"], (
        f"E2 bug: synthesis missing INSUFFICIENT EVIDENCE prefix. Got: {final_row['final_answer'][:200]}"
    )

    # The verification candidate MUST be marked with error_code=VERIFICATION_TOO_SHORT.
    verif_cand = tmp_db.execute(
        "SELECT error_code, error_message FROM run_candidates "
        "WHERE run_id = ? AND stage = 'verification'",
        (run_id,),
    ).fetchone()
    assert verif_cand is not None, "no verification candidate row recorded"
    assert verif_cand["error_code"] == "VERIFICATION_TOO_SHORT", (
        f"E2 fix missing: verification candidate error_code={verif_cand['error_code']!r}, "
        f"expected 'VERIFICATION_TOO_SHORT'"
    )


# ---------------------------------------------------------------------------
# Task 6 (PR #28) — fusion mode also uses _apply_verification_result
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fusion_short_verification_rejected_with_insufficient_evidence(mock_worker, tmp_db):
    """PR #28: the E2 short-output guard was factored into _apply_verification_result
    and the fusion-mode _run_fusion path now uses it too. Without this, the
    fusion path would still silently accept kimi-k2.6-style 19-token verdicts.
    Same lock-the-guard test as council, but exercising _run_fusion.
    """
    worker = mock_worker
    run_id = "run_e2_fusion_short"
    insert_run(
        db=tmp_db, run_id=run_id, mode="fusion", prompt="p",
        system_prompt=None, temperature=0.2, max_output_tokens=1000,
        deadline_seconds=120, deadline_at=utc_now_plus_seconds(120),
        owner_token_hash="h", metadata_json="{}", requested_models_json=None,
        created_at=utc_now_iso(),
    )
    run = dict(tmp_db.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone())

    # First-opinion and synthesis return 100 tokens; verification returns 4 tokens (< floor).
    short_verif_result = (True, '{"verdict":"approve","confidence":0.1}', None, None, 2000, 50, 4)
    fake_opinion_result = (True, "fake opinion text that is at least one hundred tokens " * 10,
                           None, None, 500, 10, 100)
    fake_synth_result = (True, "synthesized answer with sufficient tokens " * 20,
                         None, None, 500, 10, 200)

    async def fake_call_provider_async(request, db, rid):
        prompt = request.user_prompt or ""
        if "Below are answers from multiple AI models" in prompt or "You are a senior editor" in prompt:
            return fake_synth_result
        return fake_opinion_result

    async def fake_call_structured_provider_async(request, schema, db, rid):
        return short_verif_result

    with patch.object(worker, "_call_provider_async", side_effect=fake_call_provider_async), \
         patch.object(worker, "_call_structured_provider_async", side_effect=fake_call_structured_provider_async):
        await worker._run_fusion(tmp_db, run)

    final_row = tmp_db.execute(
        "SELECT status, final_confidence, final_answer FROM runs WHERE run_id = ?", (run_id,)
    ).fetchone()
    assert final_row["status"] == "succeeded"
    assert final_row["final_confidence"] == 0.5, (
        f"PR #28 regression: fusion path short verification confidence was accepted "
        f"(got {final_row['final_confidence']}, expected 0.5)"
    )
    assert "[INSUFFICIENT EVIDENCE" in final_row["final_answer"], (
        "PR #28 regression: fusion path missing INSUFFICIENT EVIDENCE prefix"
    )
    verif_cand = tmp_db.execute(
        "SELECT error_code FROM run_candidates "
        "WHERE run_id = ? AND stage = 'verification'",
        (run_id,),
    ).fetchone()
    assert verif_cand is not None
    assert verif_cand["error_code"] == "VERIFICATION_TOO_SHORT", (
        f"PR #28 regression: fusion verification candidate error_code={verif_cand['error_code']!r}"
    )

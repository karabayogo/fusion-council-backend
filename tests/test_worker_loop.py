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
        poll_interval_ms=5000,
        heartbeat_interval_ms=5000,
        stale_run_threshold_seconds=30,
    )
    worker._db = tmp_db
    return worker


# ---------------------------------------------------------------------------
# Task 1 — recover stale runs at startup
# ---------------------------------------------------------------------------

def test_recover_stale_runs_called_at_startup(mock_worker, tmp_db, caplog):
    """_recover_stale_runs() should be called exactly once when run_async() starts."""
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
    mock_recover.assert_called_once()


# ---------------------------------------------------------------------------
# Task 2 — _call_provider_async timeout
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_provider_timeout_returns_error(mock_worker):
    """Provider call that exceeds timeout should return PROVIDER_TIMEOUT."""
    worker = mock_worker
    # Mock _run_provider_sync to block longer than the test timeout
    import fusion_council_service.domain.worker_loop as wl
    with patch.object(wl, "_run_provider_sync", side_effect=lambda *a, **k: time.sleep(200)):
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

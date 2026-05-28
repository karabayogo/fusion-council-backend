"""
RED test: _recover_stale_runs() must detect and recover orphaned resumed runs.

A run stuck in orchestration_status='resumed' for > 5 minutes (STALE_THRESHOLD_SEC)
with no checkpoint activity is considered stale. The function must either:
  - Resume: re-schedule the run if a checkpoint exists
  - Abandon: mark it abandoned if no checkpoint exists

Run: cd .../fusion-council-backend && uv run pytest tests/test_recover_stale_runs.py -v
"""
import asyncio
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch, AsyncMock

import pytest


def _stale_row(**overrides):
    """Return a stale run dict with all required fields."""
    return {
        "run_id": "stale-resumed-run",
        "thread_id": "thread-stale-old",
        "checkpoint_namespace": "mode=single",
        "updated_at": (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat(),
        "resume_count": 1,
        **overrides,
    }


def _mock_pool(conn):
    """Build a mock asyncpg Pool: pool.acquire() returns async ctx mgr yielding conn."""
    p = MagicMock()
    cm = AsyncMock()
    cm.__aenter__ = AsyncMock(return_value=conn)
    cm.__aexit__ = AsyncMock(return_value=None)
    p.acquire = MagicMock(return_value=cm)
    p.close = AsyncMock()
    return p


def _mock_settings():
    from fusion_council_service.config import Settings
    ms = MagicMock(spec=Settings)
    ms.DATABASE_PATH = ""
    ms.LANGGRAPH_CHECKPOINT_DB_URL = "postgresql://u:p@h:5432/db"
    return ms


class TestRecoverStaleRuns:
    """RED test — _recover_stale_runs() must exist and handle stale resumed runs."""

    def test_module_imports(self):
        """Verify _recover_stale_runs and STALE_THRESHOLD_SEC are importable from startup."""
        from fusion_council_service.startup import _recover_stale_runs, STALE_THRESHOLD_SEC
        assert callable(_recover_stale_runs)
        assert STALE_THRESHOLD_SEC == 300  # 5 minutes

    def test_stale_threshold_is_300_seconds(self):
        """STALE_THRESHOLD_SEC must be set to 300 (5 minutes) per plan spec."""
        from fusion_council_service.startup import STALE_THRESHOLD_SEC
        assert STALE_THRESHOLD_SEC == 300

    def test_function_is_async(self):
        """_recover_stale_runs must be an async function — it uses asyncpg pool."""
        import inspect
        from fusion_council_service.startup import _recover_stale_runs
        assert inspect.iscoroutinefunction(_recover_stale_runs)

    def test_recover_stale_runs_returns_zero_when_no_db_url(self):
        """If neither LANGGRAPH_CHECKPOINT_DB_URL nor DATABASE_URL is set, return 0."""
        from fusion_council_service.startup import _recover_stale_runs
        ms = _mock_settings()
        ms.LANGGRAPH_CHECKPOINT_DB_URL = ""
        with patch("fusion_council_service.startup._checkpoint_db_url", return_value=""):
            result = asyncio.run(_recover_stale_runs(ms))
        assert result == 0

    def test_recover_stale_runs_returns_zero_when_pool_create_fails(self):
        """If asyncpg.create_pool raises, return 0 without crashing."""
        from fusion_council_service.startup import _recover_stale_runs
        ms = _mock_settings()
        with patch("asyncpg.create_pool", side_effect=Exception("connection refused")):
            result = asyncio.run(_recover_stale_runs(ms))
        assert result == 0

    def test_old_resumed_run_without_checkpoint_is_abandoned(self):
        """A stale resumed run with no checkpoint must be marked abandoned."""
        from fusion_council_service.startup import _recover_stale_runs

        conn = MagicMock()
        conn.fetch = AsyncMock(return_value=[MagicMock(**_stale_row())])
        conn.execute = AsyncMock()
        pool = _mock_pool(conn)

        with patch("asyncpg.create_pool", new_callable=AsyncMock, return_value=pool), \
             patch("fusion_council_service.startup.get_checkpoint_saver", return_value=None):
            result = asyncio.run(_recover_stale_runs(_mock_settings()))

        assert result == 0  # abandoned, not recovered
        assert conn.execute.called

    def test_old_resumed_run_with_checkpoint_increments_resume_count(self):
        """A stale resumed run with a checkpoint must be re-queued (resume_count incremented)."""
        from fusion_council_service.startup import _recover_stale_runs

        conn = MagicMock()
        conn.fetch = AsyncMock(return_value=[MagicMock(**_stale_row())])
        conn.execute = AsyncMock()
        pool = _mock_pool(conn)

        checkpointer = MagicMock()
        checkpointer.get = AsyncMock(return_value=MagicMock())  # checkpoint found

        with patch("asyncpg.create_pool", new_callable=AsyncMock, return_value=pool), \
             patch("fusion_council_service.startup.get_checkpoint_saver", return_value=checkpointer):
            result = asyncio.run(_recover_stale_runs(_mock_settings()))

        assert result == 1  # recovered (re-queued)
        assert conn.execute.called

    def test_non_resumed_statuses_are_never_touched(self):
        """Completed/failed runs must not be touched — SQL filters by orchestration_status='resumed'."""
        from fusion_council_service.startup import _recover_stale_runs

        conn = MagicMock()
        conn.fetch = AsyncMock(return_value=[])  # no resumed rows — completed filtered by SQL
        conn.execute = AsyncMock()
        pool = _mock_pool(conn)

        with patch("asyncpg.create_pool", new_callable=AsyncMock, return_value=pool), \
             patch("fusion_council_service.startup.get_checkpoint_saver", return_value=None):
            result = asyncio.run(_recover_stale_runs(_mock_settings()))

        assert result == 0
        assert not conn.execute.called  # no UPDATE — SELECT only

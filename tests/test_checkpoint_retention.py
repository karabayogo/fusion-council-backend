"""Tests for checkpoint_retention script."""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from fusion_council_service.scripts.checkpoint_retention import purge_old_checkpoints


class TestPurgeOldCheckpoints:
    """Tests for purge_old_checkpoints()."""

    def test_module_importable(self):
        """checkpoint_retention module must be importable."""
        from fusion_council_service.scripts import checkpoint_retention
        assert callable(checkpoint_retention.purge_old_checkpoints)
        assert callable(checkpoint_retention.main)

    @pytest.mark.asyncio
    async def test_returns_zero_when_no_old_rows(self):
        """When no rows exceed the cutoff, return 0 and do not delete."""
        conn = AsyncMock()
        conn.fetchval = AsyncMock(return_value=0)
        conn.execute = AsyncMock()

        deleted = await purge_old_checkpoints(conn, retention_days=7)
        assert deleted == 0
        # Should count checkpoints + writes + blobs (3 fetchval calls)
        assert conn.fetchval.call_count == 3
        conn.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_deletes_old_rows_and_returns_count(self):
        """When old rows exist, delete them and return total deleted count."""
        conn = AsyncMock()
        # Simulate 10 checkpoints, 5 writes, 3 blobs older than cutoff
        conn.fetchval = AsyncMock(side_effect=[10, 5, 3])
        conn.execute = AsyncMock()

        deleted = await purge_old_checkpoints(conn, retention_days=7)

        assert deleted == 18  # 10 + 5 + 3
        # Should have issued three DELETE statements (writes, blobs, checkpoints)
        assert conn.execute.call_count == 3

    @pytest.mark.asyncio
    async def test_handles_none_count_gracefully(self):
        """fetchval may return None if COUNT returns no rows — treat as 0."""
        conn = AsyncMock()
        # Two tables have no old rows, one has some
        conn.fetchval = AsyncMock(side_effect=[None, None, 3])
        conn.execute = AsyncMock()

        deleted = await purge_old_checkpoints(conn, retention_days=7)

        assert deleted == 3
        assert conn.execute.call_count == 3


class TestRetentionDaysValidation:
    """Validate CHECKPOINT_RETENTION_DAYS bounds."""

    @pytest.mark.asyncio
    async def test_retention_days_zero_rejected(self):
        """retention_days == 0 is rejected by main()."""
        from fusion_council_service.scripts.checkpoint_retention import main

        with patch.dict(
            "os.environ",
            {"DATABASE_URL": "postgresql://u:***@h:5432/d", "CHECKPOINT_RETENTION_DAYS": "0"},
            clear=False,
        ):
            pool_mock = AsyncMock()
            pool_mock.acquire.return_value.__aenter__.return_value = AsyncMock()
            pool_mock.acquire.return_value.__aexit__.return_value = None
            pool_mock.close = AsyncMock()

            with patch(
                "fusion_council_service.scripts.checkpoint_retention.asyncpg.create_pool",
                return_value=pool_mock,
            ):
                result = await main()
        assert result == 1

    @pytest.mark.asyncio
    async def test_retention_days_negative_rejected(self):
        """retention_days < 0 is rejected by main()."""
        from fusion_council_service.scripts.checkpoint_retention import main

        with patch.dict(
            "os.environ",
            {"DATABASE_URL": "postgresql://u:***@h:5432/d", "CHECKPOINT_RETENTION_DAYS": "-1"},
            clear=False,
        ):
            pool_mock = AsyncMock()
            pool_mock.close = AsyncMock()

            with patch(
                "fusion_council_service.scripts.checkpoint_retention.asyncpg.create_pool",
                return_value=pool_mock,
            ):
                result = await main()
        assert result == 1

    @pytest.mark.asyncio
    async def test_main_exits_with_error_when_no_database_url(self):
        """main() must exit 1 when DATABASE_URL is not set."""
        from fusion_council_service.scripts.checkpoint_retention import main

        with patch.dict("os.environ", {}, clear=True):
            result = await main()
        assert result == 1

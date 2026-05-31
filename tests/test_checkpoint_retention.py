"""Tests for checkpoint_retention script."""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from fusion_council_service.scripts.checkpoint_retention import report_table_sizes


class TestReportTableSizes:
    """Tests for report_table_sizes()."""

    def test_module_importable(self):
        """checkpoint_retention module must be importable."""
        from fusion_council_service.scripts import checkpoint_retention
        assert callable(checkpoint_retention.report_table_sizes)
        assert callable(checkpoint_retention.main)

    @pytest.mark.asyncio
    async def test_reports_all_three_tables(self):
        """Should query all three checkpoint tables and return dict."""
        conn = AsyncMock()
        conn.fetchval = AsyncMock(side_effect=[10, 5, 3])

        counts = await report_table_sizes(conn)
        assert counts == {
            "checkpoints": 10,
            "checkpoint_writes": 5,
            "checkpoint_blobs": 3,
        }
        assert conn.fetchval.call_count == 3

    @pytest.mark.asyncio
    async def test_handles_none_counts(self):
        """fetchval may return None for empty tables — treat as 0."""
        conn = AsyncMock()
        conn.fetchval = AsyncMock(side_effect=[None, 0, None])

        counts = await report_table_sizes(conn)
        assert counts == {
            "checkpoints": 0,
            "checkpoint_writes": 0,
            "checkpoint_blobs": 0,
        }


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

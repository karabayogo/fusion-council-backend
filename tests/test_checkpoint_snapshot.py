"""Tests for get_checkpoint_snapshot."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from fusion_council_service.domain.orchestration.orchestration_checkpoint import (
    get_checkpoint_snapshot,
)


class TestGetCheckpointSnapshot:
    """Unit tests for get_checkpoint_snapshot helper."""

    @pytest.fixture
    def saver(self):
        return MagicMock()

    def test_returns_none_when_saver_is_none(self):
        result = get_checkpoint_snapshot(None, {"thread_id": "t1", "checkpoint_ns": ""})
        assert result is None

    def test_returns_none_on_saver_aget_error(self, saver):
        saver.aget = AsyncMock(side_effect=RuntimeError("connection failed"))
        result = get_checkpoint_snapshot(saver, {"thread_id": "t1", "checkpoint_ns": ""})
        assert result is None

    def test_returns_none_when_checkpoint_is_none(self, saver):
        saver.aget = AsyncMock(return_value=None)
        result = get_checkpoint_snapshot(saver, {"thread_id": "t1", "checkpoint_ns": ""})
        assert result is None

    def test_extracts_channel_values_from_checkbaroint(self, saver):
        """Standard LangGraph v1 checkpoint structure has channel_values."""
        saver.aget = AsyncMock(
            return_value={
                "v": 4,
                "ts": "2024-07-31T20:14:19.804150+00:00",
                "id": "1ef4f797-8335-6428-8001-8a1503f9b875",
                "channel_values": {
                    "run_id": "r1",
                    "mode": "single",
                    "engine": "langgraph",
                    "current_stage": "generation_call",
                },
                "versions_seen": {},
            }
        )
        result = get_checkpoint_snapshot(saver, {"thread_id": "t1", "checkpoint_ns": ""})
        assert result == {
            "run_id": "r1",
            "mode": "single",
            "engine": "langgraph",
            "current_stage": "generation_call",
        }

    def test_strips_internal_keys_from_flat_checkpoint(self, saver):
        """When channel_values is absent, strip known internal keys and return rest."""
        saver.aget = AsyncMock(
            return_value={
                "v": 4,
                "ts": "2024-07-31T20:14:19.804150+00:00",
                "id": "1ef4f797-8335-6428-8001-8a1503f9b875",
                "parent_checkpoint_id": None,
                "channel_versions": {"x": 1},
                "versions_seen": {},
                "run_id": "r2",
                "current_stage": "synthesis_call",
            }
        )
        result = get_checkpoint_snapshot(saver, {"thread_id": "t1", "checkpoint_ns": ""})
        assert result == {"run_id": "r2", "current_stage": "synthesis_call"}

    def test_returns_none_when_all_keys_are_internal(self, saver):
        """If stripping internal keys leaves nothing, return None."""
        saver.aget = AsyncMock(
            return_value={
                "v": 4,
                "ts": "2024-07-31T20:14:19.804150+00:00",
                "id": "1ef4f797-8335-6428-8001-8a1503f9b875",
                "versions_seen": {},
            }
        )
        result = get_checkpoint_snapshot(saver, {"thread_id": "t1", "checkpoint_ns": ""})
        assert result is None

    def test_returns_none_for_non_dict_checkpoint(self, saver):
        """Scalar values (not dicts) are not valid snapshots."""
        saver.aget = AsyncMock(return_value="not a checkpoint")
        result = get_checkpoint_snapshot(saver, {"thread_id": "t1", "checkpoint_ns": ""})
        assert result is None

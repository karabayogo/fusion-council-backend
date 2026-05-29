"""
Phase 5 Option A tests - Verify fusion nodes perform actual work.

These tests verify that fusion nodes:
1. Accept RunnableConfig parameter
2. Access worker via config.configurable.worker
3. Perform parallel model calls
4. Persist candidates to database
5. Build synthesis prompts
6. Have proper idempotency guards
"""
import pytest
from unittest.mock import Mock, MagicMock, AsyncMock
from datetime import datetime, timezone

from fusion_council_service.domain.orchestration.orchestration_state import OrchestrationState
from fusion_council_service.domain.orchestration.orchestration_nodes_fusion import (
    node_prepare_fusion,
    node_generation_parallel,
    node_generation_persist,
    node_synthesis_call,
    node_synthesis_persist,
    node_verification_call,
    node_verification_persist,
    node_finalize_fusion_success,
    node_finalize_fusion_failure,
)


@pytest.fixture
def mock_worker():
    """Mock worker with database and provider registry."""
    worker = Mock()
    worker.db = Mock()
    worker.provider_registry = Mock()
    worker.catalog = Mock()
    return worker


@pytest.fixture
def mock_config(mock_worker):
    """Mock RunnableConfig with worker in configurable."""
    return {
        "configurable": {
            "worker": mock_worker
        }
    }


@pytest.fixture
def base_fusion_state():
    """Base OrchestrationState for fusion mode."""
    return OrchestrationState(
        run_id="test-run-123",
        mode="fusion",
        engine="langgraph",
        engine_version="v1",
        thread_id="test-thread",
        checkpoint_namespace="test-ns",
        resume_count=0,
        current_stage="",
        candidate_ids=[],
        current_candidate_id=None,
        final_answer=None,
        final_confidence=None,
        error_code=None,
        error_message=None,
        updated_at=datetime.now(timezone.utc),
    )


class TestNodePrepareFusion:
    """Test node_prepare_fusion does actual work."""

    def test_sets_stage_to_prepare_fusion(self, base_fusion_state, mock_config):
        """Verify stage is set to prepare_fusion."""
        result = node_prepare_fusion(base_fusion_state, mock_config)
        assert result["current_stage"] == "prepare_fusion"

    def test_idempotent_if_already_prepared(self, base_fusion_state, mock_config):
        """Verify idempotency - doesn't re-prepare if already at prepare stage."""
        base_fusion_state["current_stage"] = "prepare_fusion"
        result = node_prepare_fusion(base_fusion_state, mock_config)
        assert result["current_stage"] == "prepare_fusion"

    def test_idempotent_if_past_prepare(self, base_fusion_state, mock_config):
        """Verify idempotency - doesn't go back to prepare if past it."""
        base_fusion_state["current_stage"] = "generation_parallel"
        result = node_prepare_fusion(base_fusion_state, mock_config)
        assert result["current_stage"] == "generation_parallel"


class TestNodeGenerationParallel:
    """Test node_generation_parallel performs parallel model calls."""

    @pytest.mark.asyncio
    async def test_calls_multiple_models_in_parallel(
        self, base_fusion_state, mock_config, mock_worker
    ):
        """Verify parallel model calls happen."""
        # Setup: configure worker to return mock results
        mock_result = Mock()
        mock_result.raw_text = "Test answer"
        mock_result.success = True
        mock_result.input_tokens = 100
        mock_result.output_tokens = 50
        
        mock_worker.provider_registry.call_provider_async = AsyncMock(
            return_value=mock_result
        )
        mock_worker.catalog.get_models_for_mode = Mock(
            return_value=[
                {"provider": "test", "model": "model1", "alias": "alias1"},
                {"provider": "test", "model": "model2", "alias": "alias2"},
            ]
        )
        
        base_fusion_state["current_stage"] = "prepare_fusion"
        
        result = await node_generation_parallel(base_fusion_state, mock_config)
        
        # Verify: parallel calls were made
        assert mock_worker.provider_registry.call_provider_async.call_count == 2
        assert result["current_stage"] == "generation_parallel"

    @pytest.mark.asyncio
    async def test_respects_semaphore_concurrency_limit(
        self, base_fusion_state, mock_config, mock_worker
    ):
        """Verify semaphore limits concurrent calls (max 3)."""
        import asyncio
        
        call_count = 0
        max_concurrent = 0
        current_concurrent = 0
        lock = asyncio.Lock()
        
        async def track_concurrency(*args, **kwargs):
            nonlocal call_count, max_concurrent, current_concurrent
            async with lock:
                current_concurrent += 1
                max_concurrent = max(max_concurrent, current_concurrent)
                call_count += 1
            
            await asyncio.sleep(0.01)  # Simulate work
            
            async with lock:
                current_concurrent -= 1
            
            result = Mock()
            result.success = True
            result.raw_text = "answer"
            return result
        
        mock_worker.provider_registry.call_provider_async = track_concurrency
        mock_worker.catalog.get_models_for_mode = Mock(
            return_value=[
                {"provider": "test", "model": f"model{i}", "alias": f"alias{i}"}
                for i in range(5)  # 5 models
            ]
        )
        
        base_fusion_state["current_stage"] = "prepare_fusion"
        await node_generation_parallel(base_fusion_state, mock_config)
        
        # Verify: all 5 calls made, but max 3 concurrent
        assert call_count == 5
        assert max_concurrent <= 3

    @pytest.mark.asyncio
    async def test_returns_generation_parallel_stage(
        self, base_fusion_state, mock_config, mock_worker
    ):
        """Verify stage advances to generation_parallel."""
        mock_worker.provider_registry.call_provider_async = AsyncMock(
            return_value=Mock(success=True, raw_text="answer")
        )
        mock_worker.catalog.get_models_for_mode = Mock(
            return_value=[{"provider": "test", "model": "m", "alias": "a"}]
        )
        
        base_fusion_state["current_stage"] = "prepare_fusion"
        result = await node_generation_parallel(base_fusion_state, mock_config)
        
        assert result["current_stage"] == "generation_parallel"


class TestNodeGenerationPersist:
    """Test node_generation_persist persists candidates."""

    @pytest.mark.asyncio
    async def test_persists_candidates_to_database(
        self, base_fusion_state, mock_config, mock_worker
    ):
        """Verify candidates are persisted to DB."""
        mock_worker.db.insert_candidate = Mock(return_value=True)
        
        base_fusion_state["current_stage"] = "generation_parallel"
        base_fusion_state["current_candidate_id"] = "cand-1"
        
        result = await node_generation_persist(base_fusion_state, mock_config)
        
        # Verify: candidate was inserted
        mock_worker.db.insert_candidate.assert_called_once()
        assert "cand-1" in result["candidate_ids"]
        assert result["current_stage"] == "generation_persist"

    @pytest.mark.asyncio
    async def test_idempotent_if_candidate_already_persisted(
        self, base_fusion_state, mock_config, mock_worker
    ):
        """Verify doesn't duplicate if candidate already in list."""
        mock_worker.db.insert_candidate = Mock(return_value=True)
        
        base_fusion_state["current_stage"] = "generation_persist"
        base_fusion_state["candidate_ids"] = ["cand-1"]
        base_fusion_state["current_candidate_id"] = "cand-1"
        
        result = await node_generation_persist(base_fusion_state, mock_config)
        
        # Verify: not called again
        mock_worker.db.insert_candidate.assert_not_called()
        assert result["candidate_ids"] == ["cand-1"]


class TestNodeSynthesisCall:
    """Test node_synthesis_call builds synthesis prompt and calls model."""

    @pytest.mark.asyncio
    async def test_builds_synthesis_prompt_from_candidates(
        self, base_fusion_state, mock_config, mock_worker
    ):
        """Verify synthesis prompt is built from candidate answers."""
        mock_worker.provider_registry.call_provider_async = AsyncMock(
            return_value=Mock(
                success=True,
                raw_text="Synthesized answer",
                input_tokens=200,
                output_tokens=100,
            )
        )
        
        base_fusion_state["current_stage"] = "generation_persist"
        base_fusion_state["candidate_ids"] = ["cand-1", "cand-2"]
        
        result = await node_synthesis_call(base_fusion_state, mock_config)
        
        # Verify: model was called
        assert mock_worker.provider_registry.call_provider_async.called
        assert result["current_stage"] == "synthesis_call"

    @pytest.mark.asyncio
    async def test_returns_synthesis_call_stage(
        self, base_fusion_state, mock_config, mock_worker
    ):
        """Verify stage advances to synthesis_call."""
        mock_worker.provider_registry.call_provider_async = AsyncMock(
            return_value=Mock(success=True, raw_text="answer")
        )
        
        base_fusion_state["current_stage"] = "generation_persist"
        result = await node_synthesis_call(base_fusion_state, mock_config)
        
        assert result["current_stage"] == "synthesis_call"


class TestNodeSynthesisPersist:
    """Test node_synthesis_persist persists synthesis candidate."""

    @pytest.mark.asyncio
    async def test_persists_synthesis_candidate(
        self, base_fusion_state, mock_config, mock_worker
    ):
        """Verify synthesis candidate is persisted."""
        mock_worker.db.insert_candidate = Mock(return_value=True)
        
        base_fusion_state["current_stage"] = "synthesis_call"
        base_fusion_state["current_candidate_id"] = "synth-1"
        
        result = await node_synthesis_persist(base_fusion_state, mock_config)
        
        mock_worker.db.insert_candidate.assert_called_once()
        assert "synth-1" in result["candidate_ids"]
        assert result["current_stage"] == "synthesis_persist"


class TestNodeVerificationCall:
    """Test node_verification_call performs verification."""

    @pytest.mark.asyncio
    async def test_calls_verification_model(
        self, base_fusion_state, mock_config, mock_worker
    ):
        """Verify verification model is called."""
        mock_worker.provider_registry.call_provider_async = AsyncMock(
            return_value=Mock(
                success=True,
                raw_text="Verified",
                input_tokens=150,
                output_tokens=75,
            )
        )
        
        base_fusion_state["current_stage"] = "synthesis_persist"
        
        result = await node_verification_call(base_fusion_state, mock_config)
        
        assert mock_worker.provider_registry.call_provider_async.called
        assert result["current_stage"] == "verification_call"


class TestNodeVerificationPersist:
    """Test node_verification_persist persists verification result."""

    @pytest.mark.asyncio
    async def test_persists_verification_candidate(
        self, base_fusion_state, mock_config, mock_worker
    ):
        """Verify verification candidate is persisted."""
        mock_worker.db.insert_candidate = Mock(return_value=True)
        
        base_fusion_state["current_stage"] = "verification_call"
        base_fusion_state["current_candidate_id"] = "verif-1"
        
        result = await node_verification_persist(base_fusion_state, mock_config)
        
        mock_worker.db.insert_candidate.assert_called_once()
        assert "verif-1" in result["candidate_ids"]
        assert result["current_stage"] == "verification_persist"


class TestNodeFinalizeFusionSuccess:
    """Test node_finalize_fusion_success sets final answer."""

    def test_copies_computed_final_answer(self, base_fusion_state, mock_config):
        """Verify computed_final_answer is copied to final_answer."""
        base_fusion_state["current_stage"] = "verification_persist"
        base_fusion_state["computed_final_answer"] = "Final synthesized answer"
        base_fusion_state["computed_final_confidence"] = 0.85
        
        result = node_finalize_fusion_success(base_fusion_state, mock_config)
        
        assert result["final_answer"] == "Final synthesized answer"
        assert result["final_confidence"] == 0.85
        assert result["current_stage"] == "finalize_success"

    def test_idempotent_if_already_finalized(self, base_fusion_state, mock_config):
        """Verify doesn't re-finalize if already at finalize stage."""
        base_fusion_state["current_stage"] = "finalize_success"
        base_fusion_state["final_answer"] = "Already finalized"
        
        result = node_finalize_fusion_success(base_fusion_state, mock_config)
        
        assert result["final_answer"] == "Already finalized"
        assert result["current_stage"] == "finalize_success"


class TestNodeFinalizeFusionFailure:
    """Test node_finalize_fusion_failure sets error state."""

    def test_sets_error_state(self, base_fusion_state, mock_config):
        """Verify error_code and error_message are set."""
        base_fusion_state["current_stage"] = "synthesis_call"
        base_fusion_state["error_code"] = "SYNTHESIS_FAILED"
        base_fusion_state["error_message"] = "Synthesis model failed"
        
        result = node_finalize_fusion_failure(base_fusion_state, mock_config)
        
        assert result["error_code"] == "SYNTHESIS_FAILED"
        assert result["error_message"] == "Synthesis model failed"
        assert result["current_stage"] == "finalize_failure"

    def test_defaults_to_unknown_if_no_error_code(self, base_fusion_state, mock_config):
        """Verify defaults to UNKNOWN if no error_code provided."""
        base_fusion_state["current_stage"] = "synthesis_call"
        
        result = node_finalize_fusion_failure(base_fusion_state, mock_config)
        
        assert result["error_code"] == "UNKNOWN"
        assert result["current_stage"] == "finalize_failure"

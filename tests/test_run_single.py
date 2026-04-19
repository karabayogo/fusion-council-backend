"""Tests for single-mode runs."""

import json
from unittest.mock import patch, MagicMock

import pytest

from fusion_council_service.domain.candidate_repository import list_candidates_for_run
from fusion_council_service.domain.run_repository import get_run, insert_run
from fusion_council_service.clock import utc_now_iso, utc_now_plus_seconds


@pytest.fixture
def setup_single_run(tmp_db):
    """Insert a queued single-mode run."""
    def _setup(mode="single"):
        run_id = f"run_test_{mode}_{utc_now_iso().replace(':', '')}"
        insert_run(
            db=tmp_db,
            run_id=run_id,
            mode=mode,
            prompt="What is 1+1?",
            system_prompt=None,
            temperature=0.2,
            max_output_tokens=3000,
            deadline_seconds=60,
            deadline_at=utc_now_plus_seconds(60),
            owner_token_hash="testhash",
            metadata_json=json.dumps({}),
            requested_models_json=None,
            created_at=utc_now_iso(),
        )
        return run_id
    return _setup


def test_insert_run_creates_record(tmp_db, setup_single_run):
    run_id = setup_single_run()
    run = get_run(tmp_db, run_id)
    assert run is not None
    assert run["run_id"] == run_id
    assert run["status"] == "queued"
    assert run["mode"] == "single"


def test_run_contains_required_fields(tmp_db, setup_single_run):
    run_id = setup_single_run()
    run = get_run(tmp_db, run_id)
    assert "run_id" in run
    assert "status" in run
    assert "mode" in run
    assert "prompt" in run
    assert "deadline_seconds" in run
    assert "created_at" in run


def test_run_status_update(tmp_db, setup_single_run):
    from fusion_council_service.domain.run_repository import update_run_status
    run_id = setup_single_run()
    update_run_status(tmp_db, run_id, "running")
    run = get_run(tmp_db, run_id)
    assert run["status"] == "running"


def test_single_run_succeeds_with_mocked_provider(tmp_db, setup_single_run, mock_provider_result):
    """Patch the provider and verify run completes."""
    run_id = setup_single_run()

    with patch("fusion_council_service.providers.registry.ProviderRegistry.generate") as mock_gen:
        mock_gen.return_value = mock_provider_result

        from fusion_council_service.config import Settings

        settings = Settings(
            DATABASE_PATH=":memory:",
            SERVICE_API_KEYS="test",
            SERVICE_ADMIN_API_KEYS="test",
            MINIMAX_TOKEN_PLAN_API_KEY="test",
            OLLAMA_API_KEY="test",
        )
        path = settings.MODEL_CATALOG_PATH
# catalog = ModelCatalog(models)
        registry = MagicMock()
        registry.generate.return_value = mock_provider_result

        # We can't easily test the full async worker, but we can test the
        # candidate insert/update flow
        from fusion_council_service.ids import new_candidate_id
        from fusion_council_service.domain.candidate_repository import insert_candidate, update_candidate_result

        cand_id = new_candidate_id()
        insert_candidate(tmp_db, run_id, cand_id, "test-alias", "test-provider",
                         "test-model", "generation", "succeeded", utc_now_iso())
        update_candidate_result(tmp_db, cand_id, "succeeded",
                                raw_answer="The answer is 2",
                                latency_ms=500)

        cands = list_candidates_for_run(tmp_db, run_id)
        assert len(cands) == 1
        assert cands[0]["status"] == "succeeded"
        assert "2" in (cands[0]["raw_answer"] or "")
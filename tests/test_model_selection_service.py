from fusion_council_service.domain.model_selection import (
    select_healthy_stage_model,
)
from fusion_council_service.model_catalog import ModelCatalog
from fusion_council_service.domain.candidate_repository import insert_candidate
from fusion_council_service.domain.run_repository import insert_run
from fusion_council_service.clock import utc_now_iso


def _insert_run(tmp_db, run_id):
    now = utc_now_iso()
    insert_run(
        tmp_db,
        run_id=run_id,
        mode="council",
        prompt="prompt",
        system_prompt=None,
        temperature=0.2,
        max_output_tokens=1000,
        deadline_seconds=60,
        deadline_at=now,
        owner_token_hash="owner",
        metadata_json="{}",
        requested_models_json="[]",
        created_at=now,
    )


def test_stage_model_selection_deprioritizes_recent_cross_run_provider_failures(tmp_db):
    catalog = ModelCatalog([
        {
            "alias": "recently-failing-reviewer",
            "provider": "provider-a",
            "provider_model": "model-a",
            "family": "test",
            "tier": "test",
            "role_bias": "reviewer",
            "enabled": True,
        },
        {
            "alias": "healthy-reviewer",
            "provider": "provider-b",
            "provider_model": "model-b",
            "family": "test",
            "tier": "test",
            "role_bias": "reviewer",
            "enabled": True,
        },
    ])
    _insert_run(tmp_db, "previous_run")
    _insert_run(tmp_db, "current_run")
    insert_candidate(
        tmp_db,
        "previous_run",
        "cand_failed_recent",
        "recently-failing-reviewer",
        "provider-a",
        "model-a",
        "peer_review",
        "failed",
        utc_now_iso(),
    )

    selected = select_healthy_stage_model(
        db=tmp_db,
        catalog=catalog,
        run_id="current_run",
        role_order=["reviewer"],
    )

    assert selected["alias"] == "healthy-reviewer"

from fusion_council_service.clock import utc_now_iso, utc_now_plus_seconds
from fusion_council_service.domain.run_repository import insert_run, update_run_status


def test_update_run_status_sets_terminal_summary_fields(tmp_db):
    run_id = "run_terminal_status_test"
    insert_run(
        db=tmp_db,
        run_id=run_id,
        mode="single",
        prompt="test",
        system_prompt=None,
        temperature=0.2,
        max_output_tokens=1000,
        deadline_seconds=60,
        deadline_at=utc_now_plus_seconds(60),
        owner_token_hash="testhash",
        metadata_json="{}",
        requested_models_json=None,
        created_at=utc_now_iso(),
    )

    update_run_status(tmp_db, run_id, "succeeded")

    row = tmp_db.execute(
        "SELECT status, current_stage, current_stage_message, progress_percent, finished_at FROM runs WHERE run_id = ?",
        (run_id,),
    ).fetchone()

    assert row["status"] == "succeeded"
    assert row["current_stage"] == "complete"
    assert row["current_stage_message"] == "Run completed"
    assert row["progress_percent"] == 100.0
    assert row["finished_at"]

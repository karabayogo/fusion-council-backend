import json

from fusion_council_service.logging_utils import get_logger, setup_logging


def test_safe_logger_accepts_candidate_stage_extra_fields(capsys):
    setup_logging()
    logger = get_logger("tests.safe_logger")

    logger.info(
        "candidate completed",
        run_id="run-test",
        event_type="candidate.completed",
        candidate_id="cand-test",
        stage="generation",
        model="model-test",
    )

    captured = capsys.readouterr()
    entry = json.loads(captured.out)
    assert entry["message"] == "candidate completed"
    assert entry["run_id"] == "run-test"
    assert entry["event_type"] == "candidate.completed"
    assert entry["candidate_id"] == "cand-test"
    assert entry["stage"] == "generation"
    assert entry["model"] == "model-test"


def test_safe_logger_preserves_stdlib_positional_formatting(capsys):
    setup_logging()
    logger = get_logger("tests.safe_logger")

    logger.warning(
        "run_id=%s candidate_id=%s will be removed",
        "run-test",
        "cand-test",
        run_id="run-test",
        candidate_id="cand-test",
    )

    captured = capsys.readouterr()
    entry = json.loads(captured.out)
    assert entry["message"] == "run_id=run-test candidate_id=cand-test will be removed"
    assert entry["run_id"] == "run-test"
    assert entry["candidate_id"] == "cand-test"

"""Phase 2 regression tests for decision-log resolution/memory/rotation behavior."""

from fusion_council_service.clock import utc_now_iso
from fusion_council_service.domain.decision_log import (
    get_memory_context,
    log_pending_decision,
    resolve_decision_outcome,
    rotate_decision_log,
)


def _fetch_row(tmp_db, run_id: str):
    return tmp_db.execute(
        "SELECT * FROM decision_log WHERE run_id = ?",
        (run_id,),
    ).fetchone()


def test_resolve_decision_outcome_updates_pending_row(tmp_db):
    log_pending_decision(tmp_db, "run_p1", "How to rebalance?", "fusion", "Answer text")

    seen = {}

    def fake_reflection(*, prompt, final_answer, rating, outcome_raw):
        seen.update({
            "prompt": prompt,
            "final_answer": final_answer,
            "rating": rating,
            "outcome_raw": outcome_raw,
        })
        return "Strong synthesis, but cite assumptions explicitly next time."

    result = resolve_decision_outcome(
        tmp_db,
        run_id="run_p1",
        rating="helpful",
        outcome_raw=4.0,
        generate_reflection_fn=fake_reflection,
    )

    assert seen["prompt"] == "How to rebalance?"
    assert seen["final_answer"] == "Answer text"
    assert result["rating"] == "helpful"
    assert result["outcome_raw"] == 4.0
    assert "cite assumptions" in result["reflection"]
    assert result["generated_at"]

    row = _fetch_row(tmp_db, "run_p1")
    assert row is not None
    assert row["pending"] == 0
    assert row["rating"] == "helpful"
    assert float(row["outcome_raw"]) == 4.0
    assert row["reflection"] == result["reflection"]
    assert row["resolved_at"]


def test_resolve_decision_outcome_truncates_reflection(tmp_db):
    log_pending_decision(tmp_db, "run_p2", "Prompt", "council", "Answer")

    result = resolve_decision_outcome(
        tmp_db,
        run_id="run_p2",
        rating="partial",
        outcome_raw=3.0,
        generate_reflection_fn=lambda **_: "x" * 700,
        max_reflection_chars=128,
    )

    assert len(result["reflection"]) == 128
    row = _fetch_row(tmp_db, "run_p2")
    assert len(row["reflection"]) == 128


def test_resolve_decision_outcome_raises_when_no_pending_row(tmp_db):
    try:
        resolve_decision_outcome(
            tmp_db,
            run_id="missing",
            rating="helpful",
            outcome_raw=5.0,
            generate_reflection_fn=lambda **_: "n/a",
        )
        assert False, "Expected ValueError when pending row is missing"
    except ValueError as exc:
        assert "No pending decision found" in str(exc)


def test_get_memory_context_includes_same_and_cross_mode_sections(tmp_db):
    now = utc_now_iso()
    rows = [
        ("same_1", "hash1", "How do I draw down super safely?", "council", "a", "helpful", 4.0, 0, "Mention sequencing risk and tax drag.", now),
        ("same_2", "hash2", "How do I fund retirement travel?", "council", "a", "partial", 3.0, 0, "Include inflation-adjusted cashflow ranges.", now),
        ("cross_1", "hash3", "How to compare brokers?", "fusion", "a", "helpful", 5.0, 0, "Always separate platform risk from product risk.", now),
    ]
    tmp_db.executemany(
        """
        INSERT INTO decision_log
        (run_id, prompt_hash, prompt, mode, final_answer, rating, outcome_raw, pending, reflection, resolved_at, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [r + (now,) for r in rows],
    )
    tmp_db.commit()

    context = get_memory_context(
        tmp_db,
        prompt="How should I sequence retirement withdrawals?",
        mode="council",
        n_same=3,
        n_cross=2,
    )

    assert "Similar Past Questions" in context
    assert "Cross-Mode Insights" in context
    assert "council" in context
    assert "fusion" in context
    assert "sequencing risk" in context


def test_get_memory_context_prioritizes_same_prompt_hash_before_mode_recency(tmp_db):
    target_prompt = "How should I sequence retirement withdrawals?"
    target_hash = __import__("hashlib").sha256(target_prompt.encode("utf-8")).hexdigest()[:16]
    now = utc_now_iso()

    tmp_db.executemany(
        """
        INSERT INTO decision_log
        (run_id, prompt_hash, prompt, mode, final_answer, rating, outcome_raw, pending, reflection, resolved_at, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                "recent_unrelated",
                "deadbeefdeadbeef",
                "How to buy a used car safely?",
                "council",
                "a",
                "helpful",
                4.0,
                0,
                "Unrelated but recent lesson",
                "2026-05-10T10:00:00Z",
                now,
            ),
            (
                "older_related",
                target_hash,
                target_prompt,
                "council",
                "a",
                "helpful",
                5.0,
                0,
                "Related lesson should be prioritized",
                "2026-05-01T10:00:00Z",
                now,
            ),
        ],
    )
    tmp_db.commit()

    context = get_memory_context(
        tmp_db,
        prompt=target_prompt,
        mode="council",
        n_same=1,
        n_cross=0,
    )

    assert "Related lesson should be prioritized" in context
    assert "Unrelated but recent lesson" not in context


def test_rotate_decision_log_deletes_oldest_resolved_entries(tmp_db):
    rows = [
        ("r1", "h1", "p1", "fusion", "a1", "helpful", 5.0, 0, "oldest", "2026-05-01T00:00:00Z"),
        ("r2", "h2", "p2", "fusion", "a2", "helpful", 5.0, 0, "old", "2026-05-02T00:00:00Z"),
        ("r3", "h3", "p3", "fusion", "a3", "helpful", 5.0, 0, "new", "2026-05-03T00:00:00Z"),
        ("r4", "h4", "p4", "fusion", "a4", "helpful", 5.0, 0, "newest", "2026-05-04T00:00:00Z"),
    ]
    tmp_db.executemany(
        """
        INSERT INTO decision_log
        (run_id, prompt_hash, prompt, mode, final_answer, rating, outcome_raw, pending, reflection, resolved_at, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [r + (r[-1],) for r in rows],
    )
    tmp_db.commit()

    deleted = rotate_decision_log(tmp_db, max_resolved_entries=2)
    assert deleted == 2

    remaining = tmp_db.execute(
        "SELECT run_id FROM decision_log WHERE pending = 0 ORDER BY resolved_at ASC"
    ).fetchall()
    assert [r[0] for r in remaining] == ["r3", "r4"]

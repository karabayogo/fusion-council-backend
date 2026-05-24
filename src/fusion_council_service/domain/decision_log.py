"""Decision log helpers for storing run outcomes and outcome-driven memory."""

import hashlib

from fusion_council_service.clock import utc_now_iso
from fusion_council_service.db import (
    commit_tx,
    execute_sql,
    execute_sql_all,
    execute_sql_one,
    is_postgresql,
)


def log_pending_decision(
    db,
    run_id: str,
    prompt: str,
    mode: str,
    final_answer: str,
) -> None:
    """Persist a pending decision row when a run successfully completes.

    Insert is idempotent by run_id to avoid turning repeated completion events
    into run failures.
    """
    prompt_text = (prompt or "")[:2000]
    final_answer_text = (final_answer or "")[:10000]
    prompt_hash = hashlib.sha256(prompt_text.encode("utf-8")).hexdigest()[:16]
    now = utc_now_iso()

    sql = """
        INSERT INTO decision_log
        (run_id, prompt_hash, prompt, mode, final_answer, pending, created_at)
        VALUES
        (:run_id, :prompt_hash, :prompt, :mode, :final_answer, 1, :created_at)
    """
    if is_postgresql():
        sql += " ON CONFLICT (run_id) DO NOTHING"
    else:
        sql = sql.replace("INSERT INTO", "INSERT OR IGNORE INTO", 1)

    execute_sql(
        db,
        sql,
        {
            "run_id": run_id,
            "prompt_hash": prompt_hash,
            "prompt": prompt_text,
            "mode": mode,
            "final_answer": final_answer_text,
            "created_at": now,
        },
    )
    commit_tx(db)


def resolve_decision_outcome(
    db,
    run_id: str,
    rating: str,
    outcome_raw: float,
    generate_reflection_fn,
    max_reflection_chars: int = 500,
) -> dict:
    """Resolve a pending decision row with user outcome + generated reflection."""
    row = execute_sql_one(
        db,
        """
        SELECT prompt, final_answer
        FROM decision_log
        WHERE run_id = :run_id AND pending = 1
        """,
        {"run_id": run_id},
    )
    if row is None:
        raise ValueError(f"No pending decision found for run_id={run_id}")

    reflection = (
        generate_reflection_fn(
            prompt=row.get("prompt", ""),
            final_answer=row.get("final_answer", ""),
            rating=rating,
            outcome_raw=outcome_raw,
        )
        or ""
    ).strip()
    if len(reflection) > max_reflection_chars:
        reflection = reflection[:max_reflection_chars]

    now = utc_now_iso()
    execute_sql(
        db,
        """
        UPDATE decision_log
        SET pending = 0,
            rating = :rating,
            outcome_raw = :outcome_raw,
            reflection = :reflection,
            resolved_at = :resolved_at
        WHERE run_id = :run_id
        """,
        {
            "rating": rating,
            "outcome_raw": outcome_raw,
            "reflection": reflection,
            "resolved_at": now,
            "run_id": run_id,
        },
    )
    commit_tx(db)

    return {
        "rating": rating,
        "outcome_raw": outcome_raw,
        "reflection": reflection,
        "generated_at": now,
    }


def get_memory_context(
    db,
    prompt: str,
    mode: str,
    n_same: int = 3,
    n_cross: int = 2,
    max_chars_per_entry: int = 500,
) -> str:
    """Build formatted memory context from previously resolved decision rows."""
    prompt_text = (prompt or "")[:2000]
    prompt_hash = hashlib.sha256(prompt_text.encode("utf-8")).hexdigest()[:16]

    same_entries = execute_sql_all(
        db,
        """
        SELECT prompt, mode, rating, reflection, resolved_at
        FROM decision_log
        WHERE pending = 0 AND mode = :mode AND prompt_hash = :prompt_hash
        ORDER BY resolved_at DESC
        LIMIT :n_same
        """,
        {"mode": mode, "prompt_hash": prompt_hash, "n_same": n_same},
    )
    same_entries = list(same_entries)

    remaining_same = max(0, int(n_same) - len(same_entries))
    if remaining_same > 0:
        fallback_same_entries = execute_sql_all(
            db,
            """
            SELECT prompt, mode, rating, reflection, resolved_at
            FROM decision_log
            WHERE pending = 0 AND mode = :mode AND prompt_hash != :prompt_hash
            ORDER BY resolved_at DESC
            LIMIT :remaining_same
            """,
            {"mode": mode, "prompt_hash": prompt_hash, "remaining_same": remaining_same},
        )
        same_entries.extend(fallback_same_entries)

    cross_entries = execute_sql_all(
        db,
        """
        SELECT prompt, mode, rating, reflection, resolved_at
        FROM decision_log
        WHERE pending = 0 AND mode != :mode
        ORDER BY resolved_at DESC
        LIMIT :n_cross
        """,
        {"mode": mode, "n_cross": n_cross},
    )
    if not same_entries and not cross_entries:
        return ""

    parts: list[str] = []
    if same_entries:
        parts.append("### Similar Past Questions (resolved, most recent first)")
        for entry in same_entries:
            reflection = (entry.get("reflection") or "No reflection")
            if len(reflection) > max_chars_per_entry:
                reflection = reflection[:max_chars_per_entry] + "..."
            prompt_preview = (entry.get("prompt") or "").replace("\n", " ")[:50]
            rating = entry.get("rating") or "n/a"
            resolved = entry.get("resolved_at") or "n/a"
            mode_name = entry.get("mode") or mode
            parts.append(
                f"[{resolved}] {mode_name} rating={rating} | \"{prompt_preview}...\"\n"
                f"  -> {reflection}"
            )

    if cross_entries:
        parts.append("")
        parts.append("### Cross-Mode Insights")
        for entry in cross_entries:
            reflection = (entry.get("reflection") or "No reflection")
            if len(reflection) > 200:
                reflection = reflection[:200] + "..."
            parts.append(f"[{entry.get('mode') or '?'}] {entry.get('rating') or 'n/a'}: {reflection}")

    return "\n\n".join(parts)


def rotate_decision_log(
    db,
    max_resolved_entries: int = 500,
) -> int:
    """Delete oldest resolved rows if count exceeds max_resolved_entries."""
    count_row = execute_sql_one(
        db,
        "SELECT COUNT(*) AS cnt FROM decision_log WHERE pending = 0",
    )
    resolved_count = int((count_row or {}).get("cnt", 0))
    if resolved_count == 0:
        return 0

    if max_resolved_entries <= 0:
        execute_sql(db, "DELETE FROM decision_log WHERE pending = 0")
        commit_tx(db)
        return resolved_count

    if resolved_count <= max_resolved_entries:
        return 0

    to_delete = resolved_count - max_resolved_entries
    execute_sql(
        db,
        """
        DELETE FROM decision_log
        WHERE run_id IN (
            SELECT run_id
            FROM decision_log
            WHERE pending = 0
            ORDER BY resolved_at ASC
            LIMIT :to_delete
        )
        """,
        {"to_delete": to_delete},
    )
    commit_tx(db)
    return to_delete

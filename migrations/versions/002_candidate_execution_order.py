"""Add execution_order column to run_candidates with backfill.

Corresponds to the previous _migration_20260516_candidate_execution_order
in db.py. Idempotent — checks for column existence before ALTER.

Revision ID: 002
Revises: 001
Create Date: 2026-05-31
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "002"
down_revision: Union[str, None] = "001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    dialect_name = conn.dialect.name

    # ── Check if column already exists (idempotent) ──
    if dialect_name == "postgresql":
        result = conn.execute(
            sa.text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = 'run_candidates' "
                "AND column_name = 'execution_order'"
            )
        )
        if result.fetchone():
            return  # Already applied — skip
    else:
        result = conn.execute(sa.text("PRAGMA table_info('run_candidates')"))
        columns = {row[1] for row in result.fetchall()}
        if "execution_order" in columns:
            return  # Already applied — skip

    # ── Add column ──
    op.add_column(
        "run_candidates",
        sa.Column("execution_order", sa.Integer(), nullable=True),
    )

    # ── Backfill execution_order for historical candidates ──
    # Deterministic ordering: stage priority → created_at → candidate_id
    stage_order = {
        "generation": 10,
        "first_opinion": 20,
        "peer_review": 30,
        "debate": 40,
        "synthesis": 50,
        "verification": 60,
    }
    rows = conn.execute(
        sa.text(
            "SELECT candidate_id, run_id, stage, created_at "
            "FROM run_candidates WHERE execution_order IS NULL "
            "ORDER BY run_id, created_at, candidate_id"
        )
    ).fetchall()

    by_run: dict[str, list] = {}
    for row in rows:
        by_run.setdefault(row[1], []).append(row)  # row[1] = run_id

    for run_rows in by_run.values():
        run_rows.sort(
            key=lambda r: (
                stage_order.get(r[2], 999),  # r[2] = stage
                r[3] or "",                    # r[3] = created_at
                r[0] or "",                    # r[0] = candidate_id
            )
        )
        for idx, row in enumerate(run_rows, start=1):
            conn.execute(
                sa.text(
                    "UPDATE run_candidates SET execution_order = :eo "
                    "WHERE candidate_id = :cid"
                ),
                {"eo": idx, "cid": row[0]},
            )

    # ── Add index ──
    op.create_index(
        "idx_run_candidates_run_order",
        "run_candidates",
        ["run_id", "execution_order", "created_at", "candidate_id"],
        if_not_exists=True,
    )


def downgrade() -> None:
    conn = op.get_bind()
    dialect_name = conn.dialect.name

    # Check if index exists before dropping
    if dialect_name == "postgresql":
        result = conn.execute(
            sa.text(
                "SELECT indexname FROM pg_indexes "
                "WHERE tablename = 'run_candidates' "
                "AND indexname = 'idx_run_candidates_run_order'"
            )
        )
        if result.fetchone():
            op.drop_index("idx_run_candidates_run_order", table_name="run_candidates")
    else:
        # SQLite: just attempt drop (it's fine if it doesn't exist)
        op.drop_index("idx_run_candidates_run_order", table_name="run_candidates")

    # Check if column exists before dropping
    if dialect_name == "postgresql":
        result = conn.execute(
            sa.text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = 'run_candidates' "
                "AND column_name = 'execution_order'"
            )
        )
        if result.fetchone():
            op.drop_column("run_candidates", "execution_order")
    else:
        result = conn.execute(sa.text("PRAGMA table_info('run_candidates')"))
        columns = {row[1] for row in result.fetchall()}
        if "execution_order" in columns:
            op.drop_column("run_candidates", "execution_order")

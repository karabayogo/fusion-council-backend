"""Add quarantine state to provider_health and create provider_quarantine_events.

Persists streak-based quarantine decisions so model selection can exclude
degraded upstreams from the durable source of truth (DB), not from per-pod
in-memory state. Idempotent — safe to re-apply.

Revision ID: 004
Revises: 003
Create Date: 2026-06-02
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "004"
down_revision: Union[str, None] = "003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    dialect_name = conn.dialect.name

    # ── 1. Add 4 columns to provider_health (idempotent) ──
    new_columns = [
        ("consecutive_low_health_count", sa.Integer(), "0"),
        ("quarantined", sa.Integer(), "0"),
        ("quarantined_at", sa.Text(), None),
        ("quarantine_reason", sa.Text(), None),
    ]
    for col_name, col_type, col_default in new_columns:
        if dialect_name == "postgresql":
            result = conn.execute(
                sa.text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema='public' AND table_name='provider_health' "
                    "AND column_name=:c"
                ),
                {"c": col_name},
            )
            if result.fetchone():
                continue  # already added
        else:
            result = conn.execute(sa.text("PRAGMA table_info('provider_health')"))
            existing = {row[1] for row in result.fetchall()}
            if col_name in existing:
                continue
        if col_default is not None:
            conn.execute(
                sa.text(
                    f"ALTER TABLE provider_health ADD COLUMN {col_name} "
                    f"{col_type.compile(dialect=conn.dialect)} DEFAULT {col_default}"
                )
            )
        else:
            conn.execute(
                sa.text(
                    f"ALTER TABLE provider_health ADD COLUMN {col_name} "
                    f"{col_type.compile(dialect=conn.dialect)}"
                )
            )

    # ── 2. Create provider_quarantine_events (idempotent) ──
    conn.execute(
        sa.text(
            """
            CREATE TABLE IF NOT EXISTS provider_quarantine_events (
              id BIGSERIAL PRIMARY KEY,
              provider TEXT NOT NULL,
              provider_model TEXT NOT NULL,
              event_type TEXT NOT NULL,
              reason TEXT NOT NULL,
              health_score REAL,
              consecutive_low_health_count INTEGER,
              created_at TEXT NOT NULL
            )
            """
        )
    )
    # Index for fast lookups by upstream pair
    conn.execute(
        sa.text(
            "CREATE INDEX IF NOT EXISTS idx_provider_quarantine_events_pair "
            "ON provider_quarantine_events(provider, provider_model)"
        )
    )


def downgrade() -> None:
    # Drop the index + table; leave provider_health columns in place (a no-op downgrade is safer
    # for a long-lived system — a separate "down" migration would be needed to drop columns).
    conn = op.get_bind()
    conn.execute(sa.text("DROP INDEX IF EXISTS idx_provider_quarantine_events_pair"))
    conn.execute(sa.text("DROP TABLE IF EXISTS provider_quarantine_events"))

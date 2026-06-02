"""Reconcile provider_health with the live model catalog at migration time.

Removes any provider_health row whose (provider, provider_model) is not
present in the current model_catalog. Idempotent — safe to re-apply.

Revision ID: 003
Revises: 002
Create Date: 2026-06-02
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "003"
down_revision: Union[str, None] = "002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()

    # Idempotent: read model_catalog to know which (provider, provider_model) pairs are valid.
    # model_catalog is JSON-in-TEXT — we read all rows and parse in Python so this migration
    # is dialect-agnostic. (Avoids dialect-specific JSON functions.)
    rows = conn.execute(
        sa.text("SELECT enabled, provider, provider_model FROM model_catalog")
    ).fetchall()
    valid_pairs: set[tuple[str, str]] = set()
    for enabled, provider, provider_model in rows:
        if enabled and provider and provider_model:
            valid_pairs.add((provider, provider_model))

    # Read all provider_health rows and delete those not in the valid set.
    ph_rows = conn.execute(
        sa.text("SELECT provider, provider_model FROM provider_health")
    ).fetchall()
    deleted = 0
    for provider, provider_model in ph_rows:
        if (provider, provider_model) not in valid_pairs:
            conn.execute(
                sa.text("DELETE FROM provider_health WHERE provider = :p AND provider_model = :m"),
                {"p": provider, "m": provider_model},
            )
            deleted += 1

    print(f"[003_reconcile_provider_health] deleted {deleted} stale provider_health rows; {len(valid_pairs)} valid pairs retained")


def downgrade() -> None:
    # No schema change — this migration is a data reconciliation, not a schema change.
    # To re-introduce the deleted rows you would need to re-run the catalog load and
    # manually re-insert the missing rows. This is intentionally a no-op.
    pass

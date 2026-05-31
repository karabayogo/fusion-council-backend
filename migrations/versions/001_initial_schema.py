"""Initial schema — all base tables from schema.sql.

This migration creates the entire database schema from scratch, making
Alembic self-sufficient for both fresh installs and migrations.
All statements use IF NOT EXISTS for idempotency (safe on existing DBs).

Revision ID: 001
Revises: None
Create Date: 2026-05-31
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    dialect_name = conn.dialect.name
    is_pg = dialect_name == "postgresql"

    # ── runs ──
    op.create_table(
        "runs",
        sa.Column("run_id", sa.Text, primary_key=True),
        sa.Column("owner_token_hash", sa.Text, nullable=False),
        sa.Column("mode", sa.Text, nullable=False),
        sa.Column("prompt", sa.Text, nullable=False),
        sa.Column("system_prompt", sa.Text),
        sa.Column("requested_models_json", sa.Text),
        sa.Column("status", sa.Text, nullable=False),
        sa.Column("temperature", sa.Float, nullable=False),
        sa.Column("max_output_tokens", sa.Integer, nullable=False),
        sa.Column("deadline_seconds", sa.Integer, nullable=False),
        sa.Column("deadline_at", sa.Text, nullable=False),
        sa.Column("deadline_applied", sa.Integer, nullable=False, server_default="0"),
        sa.Column("degraded_reason", sa.Text),
        sa.Column("deadline_trigger_stage", sa.Text),
        sa.Column("metadata_json", sa.Text, nullable=False),
        sa.Column("current_stage", sa.Text),
        sa.Column("current_stage_message", sa.Text),
        sa.Column("progress_percent", sa.Float),
        sa.Column("models_planned", sa.Integer, nullable=False, server_default="0"),
        sa.Column("models_completed", sa.Integer, nullable=False, server_default="0"),
        sa.Column("models_failed", sa.Integer, nullable=False, server_default="0"),
        sa.Column("last_heartbeat_at", sa.Text),
        sa.Column("final_answer", sa.Text),
        sa.Column("final_summary", sa.Text),
        sa.Column("final_confidence", sa.Float),
        sa.Column("verification_json", sa.Text),
        sa.Column("error_code", sa.Text),
        sa.Column("error_message", sa.Text),
        sa.Column("created_at", sa.Text, nullable=False),
        sa.Column("started_at", sa.Text),
        sa.Column("finished_at", sa.Text),
        if_not_exists=True,
    )

    # ── run_events ──
    op.create_table(
        "run_events",
        sa.Column("event_id", sa.Text, primary_key=True),
        sa.Column("run_id", sa.Text, sa.ForeignKey("runs.run_id"), nullable=False),
        sa.Column("seq", sa.Integer, nullable=False),
        sa.Column("event_type", sa.Text, nullable=False),
        sa.Column("payload_json", sa.Text, nullable=False),
        sa.Column("created_at", sa.Text, nullable=False),
        if_not_exists=True,
    )
    op.create_index(
        "idx_run_events_run_seq",
        "run_events",
        ["run_id", "seq"],
        unique=True,
        if_not_exists=True,
    )

    # ── run_candidates ──
    op.create_table(
        "run_candidates",
        sa.Column("candidate_id", sa.Text, primary_key=True),
        sa.Column("run_id", sa.Text, sa.ForeignKey("runs.run_id"), nullable=False),
        sa.Column("alias", sa.Text, nullable=False),
        sa.Column("provider", sa.Text, nullable=False),
        sa.Column("provider_model", sa.Text, nullable=False),
        sa.Column("stage", sa.Text, nullable=False),
        sa.Column("status", sa.Text, nullable=False),
        sa.Column("execution_order", sa.Integer),
        sa.Column("latency_ms", sa.Integer),
        sa.Column("input_tokens", sa.Integer),
        sa.Column("output_tokens", sa.Integer),
        sa.Column("normalized_answer", sa.Text),
        sa.Column("raw_answer", sa.Text),
        sa.Column("score_json", sa.Text),
        sa.Column("error_code", sa.Text),
        sa.Column("error_message", sa.Text),
        sa.Column("created_at", sa.Text, nullable=False),
        sa.Column("updated_at", sa.Text, nullable=False),
        if_not_exists=True,
    )
    op.create_index(
        "idx_run_candidates_run_order",
        "run_candidates",
        ["run_id", "execution_order", "created_at", "candidate_id"],
        if_not_exists=True,
    )

    # ── schema_migrations ──
    op.create_table(
        "schema_migrations",
        sa.Column("version", sa.Text, primary_key=True),
        sa.Column("applied_at", sa.Text, nullable=False),
        if_not_exists=True,
    )

    # ── worker_state ──
    op.create_table(
        "worker_state",
        sa.Column("worker_id", sa.Text, primary_key=True),
        sa.Column("last_heartbeat_at", sa.Text, nullable=False),
        sa.Column("current_run_id", sa.Text),
        if_not_exists=True,
    )

    # ── model_catalog ──
    op.create_table(
        "model_catalog",
        sa.Column("alias", sa.Text, primary_key=True),
        sa.Column("provider", sa.Text, nullable=False),
        sa.Column("provider_model", sa.Text, nullable=False),
        sa.Column("family", sa.Text, nullable=False),
        sa.Column("tier", sa.Text, nullable=False),
        sa.Column("enabled", sa.Integer, nullable=False),
        sa.Column("validated_at", sa.Text),
        sa.Column("validation_error", sa.Text),
        if_not_exists=True,
    )

    # ── provider_health ──
    op.create_table(
        "provider_health",
        sa.Column("provider", sa.Text, nullable=False),
        sa.Column("provider_model", sa.Text, nullable=False),
        sa.Column("total_attempts", sa.Integer, nullable=False, server_default="0"),
        sa.Column("successes", sa.Integer, nullable=False, server_default="0"),
        sa.Column("failures", sa.Integer, nullable=False, server_default="0"),
        sa.Column("last_failure_at", sa.Text),
        sa.Column("last_success_at", sa.Text),
        sa.Column("avg_latency_ms", sa.Float, server_default="0"),
        sa.Column("health_score", sa.Float, server_default="1.0"),
        sa.Column("updated_at", sa.Text, nullable=False),
        sa.PrimaryKeyConstraint("provider", "provider_model"),
        if_not_exists=True,
    )

    # ── decision_log ──
    op.create_table(
        "decision_log",
        sa.Column("run_id", sa.Text, primary_key=True),
        sa.Column("prompt_hash", sa.Text, nullable=False),
        sa.Column("prompt", sa.Text, nullable=False),
        sa.Column("mode", sa.Text, nullable=False),
        sa.Column("final_answer", sa.Text, nullable=False),
        sa.Column("rating", sa.Text),
        sa.Column("outcome_raw", sa.Float),
        sa.Column("pending", sa.Integer, nullable=False, server_default="1"),
        sa.Column("reflection", sa.Text),
        sa.Column("created_at", sa.Text, nullable=False),
        sa.Column("resolved_at", sa.Text),
        if_not_exists=True,
    )
    op.create_index("idx_decision_log_pending", "decision_log", ["pending"], if_not_exists=True)
    op.create_index("idx_decision_log_prompt_hash", "decision_log", ["prompt_hash"], if_not_exists=True)

    # ── run_orchestration_state ──
    op.create_table(
        "run_orchestration_state",
        sa.Column("run_id", sa.Text, sa.ForeignKey("runs.run_id"), primary_key=True),
        sa.Column("thread_id", sa.Text, nullable=False),
        sa.Column("orchestrator_engine", sa.Text, nullable=False),
        sa.Column("orchestrator_mode", sa.Text, nullable=False),
        sa.Column("engine_version", sa.Text, nullable=False),
        sa.Column("orchestration_status", sa.Text, nullable=False),
        sa.Column("last_checkpoint_id", sa.Text),
        sa.Column("resume_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("last_error_code", sa.Text),
        sa.Column("last_error_message", sa.Text),
        sa.Column("created_at", sa.Text, nullable=False),
        sa.Column("updated_at", sa.Text, nullable=False),
        if_not_exists=True,
    )
    op.create_index(
        "idx_run_orch_state_engine_mode",
        "run_orchestration_state",
        ["orchestrator_engine", "orchestrator_mode", "orchestration_status"],
        if_not_exists=True,
    )

    # ── run_shadow_diff ──
    # Dialect-safe autoincrement: BIGSERIAL for PostgreSQL, INTEGER PRIMARY KEY AUTOINCREMENT for SQLite
    shadow_id_type = (
        sa.BigInteger().with_variant(sa.Integer(), "sqlite")
        if is_pg
        else sa.Integer
    )
    op.create_table(
        "run_shadow_diff",
        sa.Column(
            "id",
            shadow_id_type,
            primary_key=True,
            autoincrement=True,
        ),
        sa.Column("run_id", sa.Text, sa.ForeignKey("runs.run_id"), nullable=False),
        sa.Column("engine", sa.Text, nullable=False),
        sa.Column("final_status", sa.Text),
        sa.Column("final_answer_present", sa.Integer, nullable=False, server_default="0"),
        sa.Column("stage_count", sa.Integer),
        sa.Column("stage_order_match", sa.Integer),
        sa.Column("candidate_counts", sa.Text),
        sa.Column("error_codes", sa.Text),
        sa.Column("diff_summary", sa.Text),
        sa.Column("logged_at", sa.Text, nullable=False),
        if_not_exists=True,
    )
    op.create_index("idx_run_shadow_diff_run_id", "run_shadow_diff", ["run_id"], if_not_exists=True)
    op.create_index("idx_run_shadow_diff_logged_at", "run_shadow_diff", ["logged_at"], if_not_exists=True)


def downgrade() -> None:
    """Drop all tables in reverse dependency order."""
    op.drop_index("idx_run_shadow_diff_logged_at", table_name="run_shadow_diff")
    op.drop_index("idx_run_shadow_diff_run_id", table_name="run_shadow_diff")
    op.drop_table("run_shadow_diff")
    op.drop_index("idx_run_orch_state_engine_mode", table_name="run_orchestration_state")
    op.drop_table("run_orchestration_state")
    op.drop_index("idx_decision_log_pending", table_name="decision_log")
    op.drop_index("idx_decision_log_prompt_hash", table_name="decision_log")
    op.drop_table("decision_log")
    op.drop_table("provider_health")
    op.drop_table("model_catalog")
    op.drop_table("worker_state")
    op.drop_table("schema_migrations")
    op.drop_index("idx_run_candidates_run_order", table_name="run_candidates")
    op.drop_table("run_candidates")
    op.drop_index("idx_run_events_run_seq", table_name="run_events")
    op.drop_table("run_events")
    op.drop_table("runs")

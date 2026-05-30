"""
TDD RED test: Verify get_or_create_thread_id works with SQLAlchemy Session.

The shadow engine bug: new_session() returns a SQLAlchemy Session for PG,
but get_or_create_thread_id() uses await conn.fetch() — asyncpg API.
This crashes with: 'Session' object has no attribute 'fetch'

Fix: Rewrite get_or_create_thread_id to use execute_sql()/execute_sql_one()
from db.py so it works with both SQLAlchemy Session and sqlite3.Connection.
"""

import os
import pytest
from fusion_council_service import db as db_module


@pytest.fixture(autouse=True)
def _in_memory_sqlite(monkeypatch):
    """Force SQLite path so we get a sqlite3.Connection from new_session()."""
    monkeypatch.setenv("DATABASE_URL", "")
    monkeypatch.setenv("DATABASE_PATH", ":memory:")
    monkeypatch.setattr(db_module, "_is_postgresql", False)
    monkeypatch.setattr(db_module, "_engine", None)
    monkeypatch.setattr(db_module, "_SessionFactory", None)


@pytest.fixture
def fresh_db():
    """Return a fresh in-memory sqlite3.Connection with schema."""
    conn = db_module.new_session()
    db_module.initialize_schema(conn)
    yield conn
    conn.close()


class TestGetOrCreateThreadIdSqliteCompat:
    """Verify get_or_create_thread_id works with a sync DB handle."""

    @staticmethod
    def _insert_run(conn, run_id, mode="single"):
        """Insert a runs row using the canonical insert_run helper."""
        from fusion_council_service.domain.run_repository import insert_run
        from fusion_council_service.clock import utc_now_iso, utc_now_plus_seconds

        insert_run(
            db=conn, run_id=run_id, mode=mode, prompt="test",
            system_prompt=None, temperature=0.2, max_output_tokens=1000,
            deadline_seconds=60, deadline_at=utc_now_plus_seconds(60),
            owner_token_hash="hash", metadata_json="{}", requested_models_json=None,
            created_at=utc_now_iso(),
        )

    def test_fresh_run_returns_new_thread_id(self, fresh_db):
        """Fresh run (no prior orchestration state) → new thread_id."""
        from fusion_council_service.domain.orchestration.orchestration_checkpoint import (
            get_or_create_thread_id,
        )

        conn = fresh_db
        self._insert_run(conn, "run_test_001")

        config, is_resume = get_or_create_thread_id(conn, "run_test_001", "single")

        assert is_resume is False
        assert isinstance(config, dict)
        assert "thread_id" in config
        assert "checkpoint_namespace" in config
        assert config["checkpoint_namespace"] == "mode=single"
        # Thread ID is a valid UUID
        import uuid
        uuid.UUID(config["thread_id"])

    def test_resume_run_returns_existing_thread_id(self, fresh_db):
        """Run with existing 'resumed' orchestration state → resume=True."""
        from fusion_council_service.domain.orchestration.orchestration_checkpoint import (
            get_or_create_thread_id,
        )

        conn = fresh_db
        self._insert_run(conn, "run_test_002")
        # First call: fresh
        config1, is_resume1 = get_or_create_thread_id(conn, "run_test_002", "single")
        assert is_resume1 is False

        # Set orchestration_status to 'resumed' to simulate a mid-run restart
        db_module.execute_sql(
            conn,
            "UPDATE run_orchestration_state SET orchestration_status = 'resumed' WHERE run_id = :run_id",
            {"run_id": "run_test_002"},
        )
        db_module.commit_tx(conn)

        # Second call: should be resume
        config2, is_resume2 = get_or_create_thread_id(conn, "run_test_002", "single")
        assert is_resume2 is True
        assert config2["thread_id"] == config1["thread_id"]

    def test_completed_run_starts_fresh(self, fresh_db):
        """Run with 'succeeded' orchestration status → fresh, not resume."""
        from fusion_council_service.domain.orchestration.orchestration_checkpoint import (
            get_or_create_thread_id,
        )

        conn = fresh_db
        self._insert_run(conn, "run_test_003", "fusion")
        config1, _ = get_or_create_thread_id(conn, "run_test_003", "fusion")

        # Mark as succeeded
        db_module.execute_sql(
            conn,
            "UPDATE run_orchestration_state SET orchestration_status = 'succeeded' WHERE run_id = :run_id",
            {"run_id": "run_test_003"},
        )
        db_module.commit_tx(conn)

        config2, is_resume2 = get_or_create_thread_id(conn, "run_test_003", "fusion")
        assert is_resume2 is False
        # New thread_id because it's a fresh run
        assert config2["thread_id"] != config1["thread_id"]

    def test_runs_with_different_run_ids_have_different_thread_ids(self, fresh_db):
        """Each run_id gets a unique thread_id."""
        from fusion_council_service.domain.orchestration.orchestration_checkpoint import (
            get_or_create_thread_id,
        )

        conn = fresh_db
        self._insert_run(conn, "run_a", "council")
        self._insert_run(conn, "run_b", "council")
        config_a, _ = get_or_create_thread_id(conn, "run_a", "council")
        config_b, _ = get_or_create_thread_id(conn, "run_b", "council")

        assert config_a["thread_id"] != config_b["thread_id"]
        assert config_a["checkpoint_namespace"] == "mode=council"
        assert config_b["checkpoint_namespace"] == "mode=council"


class TestGetOrCreateThreadIdPostgresCompat:
    """Verify get_or_create_thread_id works with SQLAlchemy Session."""

    @pytest.fixture(autouse=True)
    def _force_pg(self, monkeypatch):
        """Simulate PostgreSQL path — new_session() returns Session."""
        monkeypatch.setenv("DATABASE_URL", "postgresql://test:test@localhost/testdb")
        monkeypatch.setenv("DATABASE_PATH", "")
        monkeypatch.setattr(db_module, "_is_postgresql", True)
        monkeypatch.setattr(db_module, "_engine", None)
        monkeypatch.setattr(db_module, "_SessionFactory", None)

    def test_get_or_create_thread_id_does_not_crash_with_session(self):
        """
        The exact scenario that causes the shadow bug:
        new_session() returns SQLAlchemy Session when PG is configured.
        get_or_create_thread_id must NOT call .fetch() on it.

        This test runs get_or_create_thread_id and verifies it doesn't
        raise AttributeError: 'Session' object has no attribute 'fetch'.
        If the fix is correct, the function uses execute_sql() instead
        of conn.fetch() and works with SQLAlchemy Session.
        """
        from sqlalchemy import create_engine, text
        from sqlalchemy.orm import sessionmaker

        from fusion_council_service.domain.orchestration.orchestration_checkpoint import (
            get_or_create_thread_id,
        )

        # Create an in-memory SQLite-backed SQLAlchemy engine for testing
        # This simulates the SQLAlchemy Session path
        engine = create_engine("sqlite:///:memory:", echo=False)
        SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
        session = SessionLocal()

        # Create run_orchestration_state table
        session.execute(text("""
            CREATE TABLE IF NOT EXISTS run_orchestration_state (
                run_id TEXT PRIMARY KEY,
                thread_id TEXT,
                orchestrator_engine TEXT,
                orchestrator_mode TEXT,
                engine_version TEXT,
                orchestration_status TEXT DEFAULT 'started',
                last_checkpoint_id TEXT,
                resume_count INTEGER DEFAULT 0,
                last_error_code TEXT,
                last_error_message TEXT,
                created_at TEXT,
                updated_at TEXT
            )
        """))
        session.commit()

        # This is the critical assertion: must not crash with AttributeError
        try:
            config, is_resume = get_or_create_thread_id(session, "run_sqlalchemy_test", "single")
        except AttributeError as e:
            if "fetch" in str(e):
                pytest.fail(
                    f"BUG NOT FIXED: get_or_create_thread_id still uses conn.fetch() "
                    f"on SQLAlchemy Session: {e}"
                )
            raise

        assert is_resume is False
        assert isinstance(config, dict)
        assert "thread_id" in config
        session.rollback()
        session.close()

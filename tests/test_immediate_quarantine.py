"""Regression tests for immediate quarantine on auth failures.

These tests lock in the fix for provider health issues:
- AUTH_FAILED (401/403) errors should trigger immediate quarantine
- This prevents wasting retries on permanent config issues
"""

import pytest
from fusion_council_service.domain.model_selection import (
    quarantine_on_auth_failure,
    _IMMEDIATE_QUARANTINE_ERROR_CODES,
)


def test_immediate_quarantine_error_codes():
    """Verify the error codes that trigger immediate quarantine."""
    assert "AUTH_FAILED" in _IMMEDIATE_QUARANTINE_ERROR_CODES
    assert "HTTP_401" in _IMMEDIATE_QUARANTINE_ERROR_CODES
    assert "HTTP_403" in _IMMEDIATE_QUARANTINE_ERROR_CODES


def test_quarantine_on_auth_failure_triggers_on_401(db_with_schema):
    """AUTH_FAILED should trigger immediate quarantine."""
    db = db_with_schema
    
    # Insert a provider_health row
    db.execute(
        "INSERT INTO provider_health (provider, provider_model, total_attempts, successes, failures, health_score) "
        "VALUES ('openai_compatible', 'qwen3.7-max', 5, 3, 2, 0.6)"
    )
    
    # Should quarantine
    result = quarantine_on_auth_failure(db, "openai_compatible", "qwen3.7-max", "AUTH_FAILED")
    assert result is True
    
    # Verify quarantined
    row = db.execute(
        "SELECT quarantined, quarantine_reason FROM provider_health WHERE provider = 'openai_compatible'"
    ).fetchone()
    assert row["quarantined"] == 1
    assert "immediate quarantine on auth failure" in row["quarantine_reason"]


def test_quarantine_on_auth_failure_skips_on_transient_error(db_with_schema):
    """Transient errors (not in immediate list) should NOT trigger immediate quarantine."""
    db = db_with_schema
    
    # Insert a provider_health row
    db.execute(
        "INSERT INTO provider_health (provider, provider_model, total_attempts, successes, failures, health_score) "
        "VALUES ('openai_compatible', 'qwen3.7-max', 5, 3, 2, 0.6)"
    )
    
    # Should NOT quarantine for TIMEOUT
    result = quarantine_on_auth_failure(db, "openai_compatible", "qwen3.7-max", "TIMEOUT")
    assert result is False
    
    # Verify NOT quarantined
    row = db.execute(
        "SELECT quarantined FROM provider_health WHERE provider = 'openai_compatible'"
    ).fetchone()
    assert row["quarantined"] == 0


def test_quarantine_on_auth_failure_skips_if_already_quarantined(db_with_schema):
    """Should not re-quarantine if already quarantined."""
    db = db_with_schema
    
    # Insert an already quarantined row
    db.execute(
        "INSERT INTO provider_health (provider, provider_model, total_attempts, successes, failures, health_score, quarantined) "
        "VALUES ('openai_compatible', 'qwen3.7-max', 5, 3, 2, 0.6, 1)"
    )
    
    # Should NOT re-quarantine (return False)
    result = quarantine_on_auth_failure(db, "openai_compatible", "qwen3.7-max", "AUTH_FAILED")
    assert result is False


def test_quarantine_creates_audit_event(db_with_schema):
 """Quarantine should create an audit event in provider_quarantine_events."""
 db = db_with_schema
 
 db.execute(
     "INSERT INTO provider_health (provider, provider_model, total_attempts, successes, failures, health_score) "
     "VALUES ('openai_compatible', 'qwen3.7-max', 5, 3, 2, 0.6)"
 )
 
 quarantine_on_auth_failure(db, "openai_compatible", "qwen3.7-max", "AUTH_FAILED")
 
 # Verify audit event
 row = db.execute(
     "SELECT event_type, reason FROM provider_quarantine_events WHERE provider = 'openai_compatible'"
 ).fetchone()
 assert row["event_type"] == "immediate_quarantine"
 assert "AUTH_FAILED" in row["reason"]


def test_quarantine_on_http_403(db_with_schema):
    """HTTP_403 should trigger immediate quarantine."""
    db = db_with_schema
    
    db.execute(
        "INSERT INTO provider_health (provider, provider_model, total_attempts, successes, failures, health_score) "
        "VALUES ('minimax_token_plan', 'MiniMax-Text-01', 3, 1, 2, 0.33)"
    )
    
    result = quarantine_on_auth_failure(db, "minimax_token_plan", "MiniMax-Text-01", "HTTP_403")
    assert result is True
    
    row = db.execute(
        "SELECT quarantined FROM provider_health WHERE provider = 'minimax_token_plan'"
    ).fetchone()
    assert row["quarantined"] == 1


# -----------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------

@pytest.fixture
def db_with_schema():
    """In-memory DB with schema for testing using raw sqlite3."""
    import sqlite3
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS provider_health (
            id INTEGER PRIMARY KEY,
            provider TEXT NOT NULL,
            provider_model TEXT NOT NULL,
            total_attempts INTEGER DEFAULT 0,
            successes INTEGER DEFAULT 0,
            failures INTEGER DEFAULT 0,
            health_score REAL DEFAULT 1.0,
            quarantined INTEGER DEFAULT 0,
            quarantine_reason TEXT,
            quarantined_at TEXT,
            consecutive_low_health_count INTEGER DEFAULT 0,
            last_failure_at TEXT,
            last_success_at TEXT,
            avg_latency_ms REAL DEFAULT 0,
            updated_at TEXT,
            UNIQUE(provider, provider_model)
        )
    """)
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS provider_quarantine_events (
            id INTEGER PRIMARY KEY,
            provider TEXT NOT NULL,
            provider_model TEXT NOT NULL,
            event_type TEXT NOT NULL,
            reason TEXT,
            health_score REAL,
            consecutive_low_health_count INTEGER,
            created_at TEXT NOT NULL
        )
    """)
    
    conn.commit()
    yield conn
    conn.close()

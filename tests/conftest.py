"""Test fixtures for fusion-council-service tests."""

import os
import sqlite3
from typing import Generator

import pytest

# Set test env before importing the app
os.environ.update({
    "DATABASE_PATH": ":memory:",
    "SERVICE_API_KEYS": "test-user-key,test-user-key-2",
    "SERVICE_ADMIN_API_KEYS": "test-admin-key",
    "MINIMAX_TOKEN_PLAN_API_KEY": "test-minimax-key",
    "OLLAMA_API_KEY": "test-ollama-key",
    "MINIMAX_ANTHROPIC_BASE_URL": "https://api.minimax.io/anthropic",
    "OLLAMA_BASE_URL": "https://ollama.com",
    "MODEL_CATALOG_PATH": os.path.join(os.path.dirname(__file__), "..", "config", "models.yaml"),
})


@pytest.fixture
def tmp_db() -> Generator[sqlite3.Connection, None, None]:
    """Create a temporary in-memory SQLite DB with schema."""
    from fusion_council_service.db import initialize_schema

    db = sqlite3.connect(":memory:", timeout=5)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA foreign_keys=ON")
    initialize_schema(db)
    yield db
    db.close()


@pytest.fixture
def mock_settings():
    """Return a Settings object with test values."""
    from fusion_council_service.config import Settings
    return Settings(
        DATABASE_PATH=":memory:",
        SERVICE_API_KEYS="test-user-key,test-user-key-2",
        SERVICE_ADMIN_API_KEYS="test-admin-key",
        MINIMAX_TOKEN_PLAN_API_KEY="test-minimax-key",
        OLLAMA_API_KEY="test-ollama-key",
        MINIMAX_ANTHROPIC_BASE_URL="https://api.minimax.io/anthropic",
        OLLAMA_BASE_URL="https://ollama.com",
        MODEL_CATALOG_PATH=os.environ["MODEL_CATALOG_PATH"],
    )


@pytest.fixture
def model_catalog(mock_settings, tmp_db):
    """Load the model catalog from YAML."""
    # Skip real provider validation in tests
    return _load_catalog_without_validation(mock_settings)


@pytest.fixture
def mock_provider_result():
    """Return a mock ProviderGenerateResult."""
    from fusion_council_service.domain.types import ProviderGenerateResult
    return ProviderGenerateResult(
        success=True,
        raw_text="Mock answer from test provider",
        error_code=None,
        error_message=None,
        latency_ms=500,
        input_tokens=50,
        output_tokens=100,
    )


@pytest.fixture
def mock_failed_provider_result():
    """Return a mock failed ProviderGenerateResult."""
    from fusion_council_service.domain.types import ProviderGenerateResult
    return ProviderGenerateResult(
        success=False,
        raw_text=None,
        error_code="AUTH_FAILED",
        error_message="Invalid API key",
        latency_ms=100,
        input_tokens=None,
        output_tokens=None,
    )


@pytest.fixture
def sample_run_params():
    """Return a dict of run creation parameters."""
    return {
        "mode": "single",
        "prompt": "What is 1+1?",
        "system_prompt": None,
        "temperature": 0.2,
        "max_output_tokens": 3000,
        "deadline_seconds": 60,
    }


@pytest.fixture
def auth_headers_user():
    return {"Authorization": "Bearer test-user-key"}


@pytest.fixture
def auth_headers_admin():
    return {"Authorization": "Bearer test-admin-key"}


def _load_catalog_without_validation(settings):
    """Load catalog without calling real providers."""
    from fusion_council_service.model_catalog import ModelCatalog, load_yaml_catalog
    models = load_yaml_catalog(settings.MODEL_CATALOG_PATH)
    return ModelCatalog(models)

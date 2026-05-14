"""Model catalog — load YAML config, validate providers at startup."""

import sqlite3
from pathlib import Path
from typing import Optional

import httpx
import yaml

from fusion_council_service.clock import utc_now_iso
from fusion_council_service.db import execute_sql, commit_tx, is_postgresql
from fusion_council_service.logging_utils import get_logger

logger = get_logger("fusion_council_service.model_catalog")

# Model selection is config-driven.  These role-preference lists define how
# enabled entries from config/models.yaml are ordered; they intentionally do
# not name provider/model aliases.  The catalog file remains the source of
# truth for which models exist and whether they are enabled.
SINGLE_ROLE_ORDER = ["primary", "backup", "synthesis", "reviewer", "verification", "creative"]
FUSION_ROLE_ORDER = ["primary", "reviewer", "synthesis", "creative", "verification", "backup"]
COUNCIL_ROLE_ORDER = ["primary", "reviewer", "creative", "synthesis", "verification", "backup"]


class ModelCatalog:
    """Holds the list of enabled models and provides lookup methods."""

    def __init__(self, models: list[dict]):
        self._models = {m["alias"]: m for m in models}

    def all_models(self) -> list[dict]:
        return list(self._models.values())

    def enabled_models(self) -> list[dict]:
        return [m for m in self.all_models() if m.get("enabled", False)]

    def get(self, alias: str) -> Optional[dict]:
        return self._models.get(alias)

    def is_model_enabled(self, alias: str) -> bool:
        m = self._models.get(alias)
        return m is not None and m.get("enabled", False)

    def __len__(self) -> int:
        return len(self._models)


def load_yaml_catalog(catalog_path: str) -> list[dict]:
    """Load model catalog from YAML file."""
    path = Path(catalog_path)
    if not path.exists():
        raise FileNotFoundError(f"Model catalog not found: {catalog_path}")
    with open(path) as f:
        data = yaml.safe_load(f)
    models = data.get("models", [])
    # Validate no duplicate aliases
    aliases = [m["alias"] for m in models]
    if len(aliases) != len(set(aliases)):
        raise ValueError("Duplicate model aliases in catalog")
    return models


def validate_minimax(api_key: str, base_url: str) -> None:
    """Validate MiniMax Token Plan access by making a trivial completion call.

    Set SKIP_PROVIDER_VALIDATION=1 to skip this check entirely (useful for CI,
    air-gapped environments, or when the upstream API is known-unavailable).
    """
    import os
    if os.environ.get("SKIP_PROVIDER_VALIDATION", "").strip() in ("1", "true", "yes"):
        logger.info("MiniMax validation skipped (SKIP_PROVIDER_VALIDATION=1)", event_type="model.validation_skipped")
        return

    import anthropic

    client = anthropic.Anthropic(
        base_url=base_url,
        api_key=api_key,
    )
    try:
        response = client.messages.create(
            model="MiniMax-M2.7",
            max_tokens=5,
            messages=[{"role": "user", "content": "Hi"}],
        )
        logger.info("MiniMax validation passed", event_type="model.validated")
    except anthropic.AuthenticationError as e:
        raise RuntimeError(f"MiniMax auth failed: {e}") from e
    except Exception as e:
        raise RuntimeError(f"MiniMax validation error: {e}") from e


def validate_ollama_models(api_key: str, base_url: str, expected_models: list[str]) -> dict[str, str]:
    """Validate Ollama cloud models by calling /api/tags.
    Returns a dict of {provider_model: validation_error} for any missing models.
    Set SKIP_PROVIDER_VALIDATION=1 to skip this check entirely.
    """
    import os
    if os.environ.get("SKIP_PROVIDER_VALIDATION", "").strip() in ("1", "true", "yes"):
        logger.info("Ollama validation skipped (SKIP_PROVIDER_VALIDATION=1)", event_type="model.validation_skipped")
        return {}
    errors = {}
    try:
        response = httpx.get(
            f"{base_url}/api/tags",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=10.0,
        )
        response.raise_for_status()
        available = {m["name"] for m in response.json().get("models", [])}
        # Also build a set of base names (without :tag suffix) for fuzzy matching
        available_bases = {n.split(":")[0] for n in available}
        for model_name in expected_models:
            base_name = model_name.split(":")[0]
            found = (model_name in available
                     or f"{model_name}:latest" in available
                     or base_name in available_bases)
            if not found:
                errors[model_name] = f"Model '{model_name}' not found in Ollama cloud"
        if not errors:
            logger.info("Ollama validation passed", event_type="model.validated")
    except httpx.HTTPStatusError as e:
        raise RuntimeError(f"Ollama /api/tags request failed: {e}") from e
    except Exception as e:
        raise RuntimeError(f"Ollama validation error: {e}") from e
    return errors


def validate_openai_compatible_models(api_key: str, base_url: str, expected_models: list[str], provider_label: str) -> dict[str, str]:
    """Validate OpenAI-compatible models by calling /models.
    Returns a dict of {provider_model: validation_error} for any missing models.
    Set SKIP_PROVIDER_VALIDATION=1 to skip this check entirely.
    """
    import os
    if os.environ.get("SKIP_PROVIDER_VALIDATION", "").strip() in ("1", "true", "yes"):
        logger.info(f"{provider_label} validation skipped (SKIP_PROVIDER_VALIDATION=1)", event_type="model.validation_skipped")
        return {}

    errors = {}
    try:
        response = httpx.get(
            f"{base_url.rstrip('/')}/models",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=10.0,
        )
        response.raise_for_status()
        data = response.json().get("data", [])
        available = {m.get("id") for m in data if m.get("id")}
        for model_name in expected_models:
            if model_name not in available:
                errors[model_name] = f"Model '{model_name}' not found in {provider_label} /models"
        if not errors:
            logger.info(f"{provider_label} validation passed", event_type="model.validated")
    except httpx.HTTPStatusError as e:
        raise RuntimeError(f"{provider_label} /models request failed: {e}") from e
    except Exception as e:
        raise RuntimeError(f"{provider_label} validation error: {e}") from e

    return errors


def load_and_validate_catalog(settings, db: Optional[sqlite3.Connection] = None) -> ModelCatalog:
    """Load the model catalog YAML, validate providers, persist to DB.
    Raises on validation failure so the app exits with a non-zero code.
    """
    models = load_yaml_catalog(settings.MODEL_CATALOG_PATH)

    # Validate MiniMax only if any minimax models are configured
    minimax_models = [m for m in models if m["provider"] == "minimax_token_plan"]
    if minimax_models:
        if not settings.minimax_api_key_effective:
            raise RuntimeError("MINIMAX_API_KEY is required when provider minimax_token_plan is configured")
        validate_minimax(settings.minimax_api_key_effective, settings.MINIMAX_ANTHROPIC_BASE_URL)

    # Validate Ollama only if any ollama models are configured
    ollama_models = [m for m in models if m["provider"] == "ollama_cloud"]
    ollama_provider_models = [m["provider_model"] for m in ollama_models]
    if ollama_provider_models:
        if not settings.OLLAMA_API_KEY:
            raise RuntimeError("OLLAMA_API_KEY is required when provider ollama_cloud is configured")
        ollama_errors = validate_ollama_models(settings.OLLAMA_API_KEY, settings.OLLAMA_BASE_URL, ollama_provider_models)
        if ollama_errors:
            for model_name, error in ollama_errors.items():
                logger.error(f"Ollama model validation failed: {error}")
            raise RuntimeError(f"Ollama model validation failed: {list(ollama_errors.values())}")

    # Validate OpenAI Codex-compatible only if configured and enabled
    openai_codex_models = [
        m["provider_model"]
        for m in models
        if m["provider"] == "openai_codex" and m.get("enabled", False)
    ]
    if openai_codex_models:
        if not settings.OPENAI_CODEX_API_KEY:
            raise RuntimeError("OPENAI_CODEX_API_KEY is required when provider openai_codex is configured")
        openai_codex_errors = validate_openai_compatible_models(
            settings.OPENAI_CODEX_API_KEY,
            settings.OPENAI_CODEX_BASE_URL,
            openai_codex_models,
            "OpenAI Codex",
        )
        if openai_codex_errors:
            for model_name, error in openai_codex_errors.items():
                logger.error(f"OpenAI Codex model validation failed: {error}")
            raise RuntimeError(f"OpenAI Codex model validation failed: {list(openai_codex_errors.values())}")

    # Validate OpenCode-Go-compatible only if configured and enabled
    opencode_go_models = [
        m["provider_model"]
        for m in models
        if m["provider"] == "opencode_go" and m.get("enabled", False)
    ]
    if opencode_go_models:
        if not settings.OPENCODE_GO_API_KEY:
            raise RuntimeError("OPENCODE_GO_API_KEY is required when provider opencode_go is configured")
        opencode_go_errors = validate_openai_compatible_models(
            settings.OPENCODE_GO_API_KEY,
            settings.OPENCODE_GO_BASE_URL,
            opencode_go_models,
            "OpenCode-Go",
        )
        if opencode_go_errors:
            for model_name, error in opencode_go_errors.items():
                logger.error(f"OpenCode-Go model validation failed: {error}")
            raise RuntimeError(f"OpenCode-Go model validation failed: {list(opencode_go_errors.values())}")

    # Persist to DB
    if db is not None:
        now = utc_now_iso()
        for m in models:
            if is_postgresql():
                execute_sql(
                    db,
                    """
                    INSERT INTO model_catalog
                        (alias, provider, provider_model, family, tier, enabled, validated_at, validation_error)
                    VALUES (:alias, :provider, :provider_model, :family, :tier, :enabled, :validated_at, NULL)
                    ON CONFLICT (alias) DO UPDATE SET
                        provider = EXCLUDED.provider, provider_model = EXCLUDED.provider_model,
                        family = EXCLUDED.family, tier = EXCLUDED.tier, enabled = EXCLUDED.enabled,
                        validated_at = EXCLUDED.validated_at, validation_error = NULL
                    """,
                    {
                        "alias": m["alias"], "provider": m["provider"],
                        "provider_model": m["provider_model"], "family": m["family"],
                        "tier": m["tier"], "enabled": 1 if m["enabled"] else 0,
                        "validated_at": now,
                    },
                )
            else:
                db.execute(
                    """
                    INSERT OR REPLACE INTO model_catalog
                        (alias, provider, provider_model, family, tier, enabled, validated_at, validation_error)
                    VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
                    """,
                    (m["alias"], m["provider"], m["provider_model"], m["family"],
                     m["tier"], 1 if m["enabled"] else 0, now),
                )
        commit_tx(db)

    return ModelCatalog(models)
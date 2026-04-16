"""Model catalog — load YAML config, validate providers at startup."""

import sqlite3
from pathlib import Path
from typing import Optional

import httpx
import yaml

from fusion_council_service.clock import utc_now_iso
from fusion_council_service.logging_utils import get_logger

logger = get_logger("fusion_council_service.model_catalog")

# Default model selection constants
FUSION_ACTIVE_TRIO = [
    "minimax-portal/MiniMax-M2.7",
    "ollama/glm-5.1:cloud",
    "ollama/qwen3.5:cloud",
]

FUSION_FALLBACK_QUEUE = [
    "ollama/kimi-k2.5:cloud",
    "ollama/minimax-m2.7:cloud",
]

COUNCIL_ACTIVE_TRIO = [
    "minimax-portal/MiniMax-M2.7",
    "ollama/glm-5.1:cloud",
    "ollama/qwen3.5:cloud",
]

COUNCIL_FALLBACK_QUEUE = [
    "ollama/kimi-k2.5:cloud",
    "ollama/minimax-m2.7:cloud",
]

SINGLE_DEFAULT_MODEL = "minimax-portal/MiniMax-M2.7"

# Synthesis model order
SYNTHESIS_MODEL_ORDER = [
    "ollama/qwen3.5:cloud",
    "minimax-portal/MiniMax-M2.7",
]

# Verification model order
VERIFICATION_MODEL_ORDER = [
    "ollama/glm-5.1:cloud",
    "ollama/kimi-k2.5:cloud",
    "minimax-portal/MiniMax-M2.7",
]

# Council synthesis model order
COUNCIL_SYNTHESIS_MODEL_ORDER = [
    "minimax-portal/MiniMax-M2.7",
    "ollama/qwen3.5:cloud",
]

# Council verification model order
COUNCIL_VERIFICATION_MODEL_ORDER = [
    "ollama/glm-5.1:cloud",
    "ollama/kimi-k2.5:cloud",
    "ollama/minimax-m2.7:cloud",
]


class ModelCatalog:
    """Holds the list of enabled models and provides lookup methods."""

    def __init__(self, models: list[dict]):
        self._models = {m["alias"]: m for m in models}

    def all_models(self) -> list[dict]:
        return list(self._models.values())

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
    """Validate MiniMax Token Plan access by making a trivial completion call."""
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
    """
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


def load_and_validate_catalog(settings, db: Optional[sqlite3.Connection] = None) -> ModelCatalog:
    """Load the model catalog YAML, validate providers, persist to DB.
    Raises on validation failure so the app exits with a non-zero code.
    """
    models = load_yaml_catalog(settings.MODEL_CATALOG_PATH)

    # Validate MiniMax
    minimax_models = [m for m in models if m["provider"] == "minimax_token_plan"]
    validate_minimax(settings.MINIMAX_TOKEN_PLAN_API_KEY, settings.MINIMAX_ANTHROPIC_BASE_URL)

    # Validate Ollama
    ollama_models = [m for m in models if m["provider"] == "ollama_cloud"]
    ollama_provider_models = [m["provider_model"] for m in ollama_models]
    ollama_errors = validate_ollama_models(settings.OLLAMA_API_KEY, settings.OLLAMA_BASE_URL, ollama_provider_models)
    if ollama_errors:
        for model_name, error in ollama_errors.items():
            logger.error(f"Ollama model validation failed: {error}")
        raise RuntimeError(f"Ollama model validation failed: {list(ollama_errors.values())}")

    # Persist to DB
    if db is not None:
        now = utc_now_iso()
        for m in models:
            db.execute(
                """
                INSERT OR REPLACE INTO model_catalog
                    (alias, provider, provider_model, family, tier, enabled, validated_at, validation_error)
                VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
                """,
                (m["alias"], m["provider"], m["provider_model"], m["family"],
                 m["tier"], 1 if m["enabled"] else 0, now),
            )
        db.commit()

    return ModelCatalog(models)
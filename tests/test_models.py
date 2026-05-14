"""Tests for model catalog."""

import os

from fusion_council_service.model_catalog import (
    ModelCatalog,
    load_yaml_catalog,
)


def test_load_yaml_catalog():
    path = os.path.join(os.path.dirname(__file__), "..", "config", "models.yaml")
    models = load_yaml_catalog(path)
    assert len(models) == 6
    aliases = [m["alias"] for m in models]
    assert "openai-codex/gpt-5.3-codex" in aliases
    assert "opencode-go/gpt-5.4" in aliases
    assert "minimax/MiniMax-M2.7" in aliases


def test_duplicate_alias_raises():
    from fusion_council_service.model_catalog import load_yaml_catalog
    import tempfile

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write("models:\n  - alias: dup\n    provider: a\n    provider_model: m1\n  - alias: dup\n    provider: b\n    provider_model: m2\n")
        tmp = f.name

    try:
        load_yaml_catalog(tmp)
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "Duplicate" in str(e)
    finally:
        os.unlink(tmp)


def test_model_catalog_get():
    path = os.path.join(os.path.dirname(__file__), "..", "config", "models.yaml")
    models = load_yaml_catalog(path)
    catalog = ModelCatalog(models)

    assert catalog.get("openai-codex/gpt-5.3-codex")["provider"] == "openai_codex"
    assert catalog.get("nonexistent") is None


def test_model_catalog_is_enabled():
    path = os.path.join(os.path.dirname(__file__), "..", "config", "models.yaml")
    models = load_yaml_catalog(path)
    catalog = ModelCatalog(models)

    assert catalog.is_model_enabled("openai-codex/gpt-5.3-codex") is True
    assert catalog.is_model_enabled("opencode-go/deepseek-v4-pro") is True
    assert catalog.is_model_enabled("nonexistent") is False


def test_model_catalog_len():
    path = os.path.join(os.path.dirname(__file__), "..", "config", "models.yaml")
    models = load_yaml_catalog(path)
    catalog = ModelCatalog(models)
    assert len(catalog) == 6

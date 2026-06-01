"""Tests for model catalog."""

import os

from fusion_council_service.domain.budget import select_models_for_mode
from fusion_council_service.model_catalog import (
    ModelCatalog,
    load_yaml_catalog,
)


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
    catalog = ModelCatalog([
        {"alias": "a", "provider": "p1", "provider_model": "m1", "enabled": True},
        {"alias": "b", "provider": "p2", "provider_model": "m2", "enabled": False},
    ])
    assert catalog.get("a")["provider"] == "p1"
    assert catalog.get("nonexistent") is None


def test_model_catalog_is_enabled():
    catalog = ModelCatalog([
        {"alias": "a", "provider": "p1", "provider_model": "m1", "enabled": True},
        {"alias": "b", "provider": "p2", "provider_model": "m2", "enabled": False},
    ])
    assert catalog.is_model_enabled("a") is True
    assert catalog.is_model_enabled("b") is False
    assert catalog.is_model_enabled("nonexistent") is False


def test_model_catalog_len():
    catalog = ModelCatalog([
        {"alias": "a", "provider": "p1", "provider_model": "m1", "enabled": True},
        {"alias": "b", "provider": "p2", "provider_model": "m2", "enabled": False},
    ])
    assert len(catalog) == 2


def test_mode_selection_is_config_driven_by_enabled_roles():
    catalog = ModelCatalog([
        {
            "alias": "disabled-primary",
            "provider": "p",
            "provider_model": "m1",
            "enabled": False,
            "role_bias": "primary",
        },
        {
            "alias": "yaml-reviewer",
            "provider": "p",
            "provider_model": "m2",
            "enabled": True,
            "role_bias": "reviewer",
        },
        {
            "alias": "yaml-primary",
            "provider": "p",
            "provider_model": "m3",
            "enabled": True,
            "role_bias": "primary",
        },
        {
            "alias": "yaml-synthesis",
            "provider": "p",
            "provider_model": "m4",
            "enabled": True,
            "role_bias": "synthesis",
        },
        {
            "alias": "yaml-backup",
            "provider": "p",
            "provider_model": "m5",
            "enabled": True,
            "role_bias": "backup",
        },
    ])

    assert [m["alias"] for m in select_models_for_mode("single", catalog)] == ["yaml-primary"]
    assert [m["alias"] for m in select_models_for_mode("fusion", catalog)] == [
        "yaml-primary",
        "yaml-reviewer",
        "yaml-synthesis",
    ]
    assert [m["alias"] for m in select_models_for_mode("council", catalog)] == [
        "yaml-primary",
        "yaml-reviewer",
        "yaml-backup",
    ]


def test_mode_selection_uses_models_yaml_not_hardcoded_aliases():
    catalog = ModelCatalog([
        {
            "alias": "custom/alpha",
            "provider": "custom_provider",
            "provider_model": "alpha",
            "enabled": True,
            "role_bias": "primary",
        },
        {
            "alias": "custom/beta",
            "provider": "custom_provider",
            "provider_model": "beta",
            "enabled": True,
            "role_bias": "reviewer",
        },
    ])

    assert [m["alias"] for m in select_models_for_mode("fusion", catalog)] == [
        "custom/alpha",
        "custom/beta",
    ]


def test_requested_models_are_filtered_through_catalog_enabled_flags():
    catalog = ModelCatalog([
        {"alias": "enabled", "provider": "p", "provider_model": "m1", "enabled": True},
        {"alias": "disabled", "provider": "p", "provider_model": "m2", "enabled": False},
    ])

    assert [
        m["alias"]
        for m in select_models_for_mode("fusion", catalog, requested_models=["disabled", "missing", "enabled"])
    ] == ["enabled"]


def test_council_selection_prefers_distinct_provider_models_for_quorum():
    catalog = ModelCatalog([
        {"alias": "primary-a", "provider": "p1", "provider_model": "m1", "enabled": True, "role_bias": "primary"},
        {"alias": "reviewer-b", "provider": "p1", "provider_model": "m2", "enabled": True, "role_bias": "reviewer"},
        {"alias": "creative-dup-a", "provider": "p1", "provider_model": "m1", "enabled": True, "role_bias": "creative"},
        {"alias": "synthesis-c", "provider": "p2", "provider_model": "m3", "enabled": True, "role_bias": "synthesis"},
    ])

    assert [m["alias"] for m in select_models_for_mode("council", catalog)] == [
        "primary-a",
        "reviewer-b",
        "synthesis-c",
    ]

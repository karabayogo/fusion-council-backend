"""W1 regression tests for the validate_minimax catalog-driven rewrite.

The pre-W1 implementation hardcodes `model="MiniMax-M2.7"` even though the
active catalog entry is `MiniMax-M3`. After W1, the function takes a
list of enabled MiniMax provider models and probes each one.

The W4 spec changes validate_minimax's signature to accept
`enabled_minimax_provider_models: list[str]`. The dumber LLM must not
break the existing signature compat.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest

from fusion_council_service.model_catalog import validate_minimax


def test_validate_minimax_probes_first_enabled_model(monkeypatch: pytest.MonkeyPatch) -> None:
    """validate_minimax must use the first enabled provider_model, not a hardcoded literal."""
    monkeypatch.setenv("SKIP_PROVIDER_VALIDATION", "")
    captured: dict = {}

    class _FakeAnthropic:
        def __init__(self, base_url: str, api_key: str) -> None:
            self.base_url = base_url
            self.api_key = api_key

        class messages:
            @staticmethod
            def create(model: str, **kwargs):
                captured["model"] = model
                captured["kwargs"] = kwargs
                # Return a minimal message-shaped object
                return MagicMock()

    with patch("anthropic.Anthropic", _FakeAnthropic):
        validate_minimax(
            api_key="test-key",
            base_url="https://api.minimax.io/anthropic",
            enabled_minimax_provider_models=["MiniMax-M3"],
        )

    assert captured.get("model") == "MiniMax-M3", (
        f"validate_minimax must probe MiniMax-M3 (the active catalog entry), "
        f"not a hardcoded M2.7. Got: {captured.get('model')!r}"
    )


def test_validate_minimax_probes_all_enabled_models(monkeypatch: pytest.MonkeyPatch) -> None:
    """When multiple MiniMax models are enabled, each must be probed."""
    monkeypatch.setenv("SKIP_PROVIDER_VALIDATION", "")
    captured: list[str] = []

    class _FakeAnthropic:
        def __init__(self, base_url: str, api_key: str) -> None:
            pass

        class messages:
            @staticmethod
            def create(model: str, **kwargs):
                captured.append(model)
                return MagicMock()

    with patch("anthropic.Anthropic", _FakeAnthropic):
        validate_minimax(
            api_key="test-key",
            base_url="https://api.minimax.io/anthropic",
            enabled_minimax_provider_models=["MiniMax-M3", "MiniMax-M4"],
        )

    assert captured == ["MiniMax-M3", "MiniMax-M4"]


def test_validate_minimax_skips_when_no_enabled_models(monkeypatch: pytest.MonkeyPatch) -> None:
    """Empty enabled_models list means no API call."""
    monkeypatch.setenv("SKIP_PROVIDER_VALIDATION", "")
    called = {"flag": False}

    class _FakeAnthropic:
        def __init__(self, base_url: str, api_key: str) -> None:
            pass

        class messages:
            @staticmethod
            def create(model: str, **kwargs):
                called["flag"] = True
                return MagicMock()

    with patch("anthropic.Anthropic", _FakeAnthropic):
        validate_minimax(
            api_key="test-key",
            base_url="https://api.minimax.io/anthropic",
            enabled_minimax_provider_models=[],
        )

    assert called["flag"] is False, "validate_minimax must not call the API for an empty enabled list"


def test_validate_minimax_honors_skip_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    """SKIP_PROVIDER_VALIDATION=1 must skip the call entirely."""
    monkeypatch.setenv("SKIP_PROVIDER_VALIDATION", "1")
    called = {"flag": False}

    class _FakeAnthropic:
        def __init__(self, base_url: str, api_key: str) -> None:
            pass

        class messages:
            @staticmethod
            def create(model: str, **kwargs):
                called["flag"] = True
                return MagicMock()

    with patch("anthropic.Anthropic", _FakeAnthropic):
        validate_minimax(
            api_key="test-key",
            base_url="https://api.minimax.io/anthropic",
            enabled_minimax_provider_models=["MiniMax-M3"],
        )

    assert called["flag"] is False, "validate_minimax must skip the API call when SKIP_PROVIDER_VALIDATION=1"

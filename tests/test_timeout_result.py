"""Regression tests for the W4 timeout result helper.

The helper `build_timeout_result(effective_timeout, run_id)` is a small factory
that constructs a `ProviderGenerateResult` for a timed-out provider call.
Before W4 lands, this module does not exist — so the import test will fail
first (RED). After W4 lands, the shape and determinism tests should pass.
"""

from __future__ import annotations


def test_build_timeout_result_module_imports() -> None:
    """The new timeout_result module must be importable from the canonical path."""
    from fusion_council_service.domain.timeout_result import (  # noqa: F401
        build_timeout_result,
    )


def test_build_timeout_result_returns_correct_shape() -> None:
    """All ProviderGenerateResult fields must be set per the W4 spec."""
    from fusion_council_service.domain.timeout_result import build_timeout_result

    r = build_timeout_result(effective_timeout=600, run_id="run_test_123")
    assert r.success is False
    assert r.raw_text is None
    assert r.error_code == "PROVIDER_TIMEOUT"
    assert r.error_message == "Provider call timed out after 600s"
    assert r.latency_ms == 600_000
    assert r.input_tokens is None
    assert r.output_tokens is None


def test_build_timeout_result_latency_is_timeout_seconds_times_1000() -> None:
    """latency_ms must be effective_timeout * 1000 (seconds to ms)."""
    from fusion_council_service.domain.timeout_result import build_timeout_result

    r = build_timeout_result(effective_timeout=42, run_id="run_x")
    assert r.latency_ms == 42_000


def test_build_timeout_result_is_deterministic_for_same_args() -> None:
    """Two calls with the same args must produce equal results (dataclass equality)."""
    from fusion_council_service.domain.timeout_result import build_timeout_result

    r1 = build_timeout_result(100, "run_a")
    r2 = build_timeout_result(100, "run_a")
    assert r1 == r2


def test_build_timeout_result_uses_double_quoted_constant() -> None:
    """The literal 'PROVIDER_TIMEOUT' must be the double-quoted form per W4 spec.

    The single-quoted form in worker_loop.py:595 is normalized to the
    double-quoted form. The test pins this so a future agent who switches
    to f-strings or constants cannot accidentally change the literal.
    """
    from fusion_council_service.domain import timeout_result as tr

    src = open(tr.__file__).read()
    # Double-quoted literal must appear in the file at least once.
    assert '"PROVIDER_TIMEOUT"' in src, (
        "W4 spec requires double-quoted 'PROVIDER_TIMEOUT' in the helper. "
        "Switching to a constant or single-quoted form would break the test."
    )

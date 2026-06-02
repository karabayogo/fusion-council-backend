"""Shared factory for PROVIDER_TIMEOUT ProviderGenerateResult.

W4 spec §8.5.1: There are exactly two executable timeout sites in
worker_loop.py (line 595 single-quoted, line 674 double-quoted). Both call
this helper instead of constructing the result inline. Behavior is
identical to the pre-refactor code.

The literal 'PROVIDER_TIMEOUT' is intentionally double-quoted to match
the dominant form (1 of 2 sites in the pre-refactor code) and to pin the
canonical value as a string constant. A regression test in
tests/test_timeout_result.py pins this to the double-quoted form.
"""

from __future__ import annotations

from fusion_council_service.domain.types import ProviderGenerateResult


def build_timeout_result(effective_timeout: int, run_id: str) -> ProviderGenerateResult:
    """Construct the ProviderGenerateResult for a provider call that timed out.

    Args:
        effective_timeout: the timeout in seconds that was applied to the call
        run_id: the run_id (currently unused; kept for future logging/telemetry)

    Returns:
        ProviderGenerateResult with success=False, error_code="PROVIDER_TIMEOUT",
        latency_ms=effective_timeout * 1000, all other fields None/0.
    """
    return ProviderGenerateResult(
        success=False,
        raw_text=None,
        error_code="PROVIDER_TIMEOUT",
        error_message=f"Provider call timed out after {effective_timeout}s",
        latency_ms=effective_timeout * 1000,
        input_tokens=None,
        output_tokens=None,
    )

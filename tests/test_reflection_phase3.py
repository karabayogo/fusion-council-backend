"""Phase 3 regression tests for reflection prompt builder and generation."""

from fusion_council_service.domain.reflection import (
    build_reflection_prompt,
    generate_reflection,
)
from fusion_council_service.domain.types import ProviderGenerateResult


class FakeRegistry:
    def __init__(self, results):
        self._results = list(results)
        self.requests = []

    def generate(self, request):
        self.requests.append(request)
        if self._results:
            return self._results.pop(0)
        return ProviderGenerateResult(
            success=False,
            raw_text=None,
            error_code="NO_MORE_RESULTS",
            error_message="exhausted",
            latency_ms=0,
            input_tokens=None,
            output_tokens=None,
        )


def test_build_reflection_prompt_contains_required_sections():
    prompt = build_reflection_prompt(
        prompt="How should I sequence pension drawdown?",
        final_answer="Start with taxable assets.",
        rating="helpful",
        outcome_raw=4.0,
    )

    assert "ORIGINAL QUESTION" in prompt
    assert "FINAL ANSWER GENERATED" in prompt
    assert "USER FEEDBACK" in prompt
    assert "Write exactly 2-4 sentences" in prompt


def test_generate_reflection_uses_alias_prefix_mapping_for_minimax():
    registry = FakeRegistry([
        ProviderGenerateResult(
            success=True,
            raw_text="The answer was mostly correct. Next time include explicit assumptions.",
            error_code=None,
            error_message=None,
            latency_ms=10,
            input_tokens=20,
            output_tokens=25,
        )
    ])

    reflection = generate_reflection(
        prompt="Q",
        final_answer="A",
        rating="helpful",
        outcome_raw=4.0,
        provider_registry=registry,
        backup_role_alias="minimax/MiniMax-M2.7",
    )

    assert reflection.startswith("The answer was mostly correct")
    assert len(registry.requests) == 1
    req = registry.requests[0]
    assert req.provider == "minimax_token_plan"
    assert req.provider_model == "MiniMax-M2.7"


def test_generate_reflection_retries_then_succeeds():
    registry = FakeRegistry([
        ProviderGenerateResult(
            success=False,
            raw_text=None,
            error_code="HTTP_500",
            error_message="boom",
            latency_ms=4,
            input_tokens=None,
            output_tokens=None,
        ),
        ProviderGenerateResult(
            success=True,
            raw_text="Use tighter compliance citations in future runs.",
            error_code=None,
            error_message=None,
            latency_ms=9,
            input_tokens=11,
            output_tokens=12,
        ),
    ])

    reflection = generate_reflection(
        prompt="Q",
        final_answer="A",
        rating="partial",
        outcome_raw=3.0,
        provider_registry=registry,
        backup_role_alias="opencode-go/kimi-k2.6",
        max_retries=2,
    )

    assert "compliance citations" in reflection
    assert len(registry.requests) == 2
    assert registry.requests[0].provider == "opencode_go"


def test_generate_reflection_returns_empty_when_retries_exhausted():
    registry = FakeRegistry([
        ProviderGenerateResult(
            success=False,
            raw_text=None,
            error_code="TIMEOUT",
            error_message="t",
            latency_ms=4,
            input_tokens=None,
            output_tokens=None,
        ),
        ProviderGenerateResult(
            success=False,
            raw_text=None,
            error_code="TIMEOUT",
            error_message="t",
            latency_ms=4,
            input_tokens=None,
            output_tokens=None,
        ),
    ])

    reflection = generate_reflection(
        prompt="Q",
        final_answer="A",
        rating="not_helpful",
        outcome_raw=1.0,
        provider_registry=registry,
        backup_role_alias="openai-codex/gpt-5.3-codex",
        max_retries=2,
    )

    assert reflection == ""


def test_generate_reflection_truncates_to_500_chars():
    registry = FakeRegistry([
        ProviderGenerateResult(
            success=True,
            raw_text="x" * 900,
            error_code=None,
            error_message=None,
            latency_ms=4,
            input_tokens=2,
            output_tokens=100,
        ),
    ])

    reflection = generate_reflection(
        prompt="Q",
        final_answer="A",
        rating="helpful",
        outcome_raw=5.0,
        provider_registry=registry,
    )

    assert len(reflection) == 500
    assert reflection.endswith("...")

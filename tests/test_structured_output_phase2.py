"""Phase 2 regression tests for structured-output fallback utility."""

from pydantic import BaseModel

from fusion_council_service.domain.types import ProviderGenerateRequest, ProviderGenerateResult
from fusion_council_service.domain.structured_output import (
    _build_freetext_request,
    _build_structured_request,
    invoke_structured_or_freetext,
)


class ExampleSchema(BaseModel):
    answer: str
    confidence: float


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
            error_message="no more responses",
            latency_ms=0,
            input_tokens=None,
            output_tokens=None,
        )


def _request() -> ProviderGenerateRequest:
    return ProviderGenerateRequest(
        alias="opencode-go/deepseek-v4-pro",
        provider="opencode_go",
        provider_model="deepseek-v4-pro",
        system_prompt="Return useful output.",
        user_prompt="Summarize the market outlook.",
        max_output_tokens=250,
        temperature=0.2,
    )


def test_provider_generate_request_structured_fields_default_to_none():
    req = _request()
    assert req.json_schema is None
    assert req.response_format is None


def test_build_structured_request_attaches_schema_and_suffix():
    req = _request()

    built = _build_structured_request(req, ExampleSchema, " JSON only.")

    assert built.alias == req.alias
    assert built.provider == req.provider
    assert built.provider_model == req.provider_model
    assert built.system_prompt.endswith(" JSON only.")
    assert built.json_schema == ExampleSchema.model_json_schema()
    assert built.response_format is not None
    assert built.response_format.get("type") == "json_object"


def test_build_freetext_request_removes_structured_metadata():
    req = _request()

    built = _build_freetext_request(req, " JSON only.")

    assert "IMPORTANT: Respond with valid JSON only." in built.user_prompt
    assert built.json_schema is None
    assert built.response_format is None


def test_invoke_structured_or_freetext_accepts_valid_structured_result():
    registry = FakeRegistry([
        ProviderGenerateResult(
            success=True,
            raw_text='{"answer":"ok","confidence":0.9}',
            error_code=None,
            error_message=None,
            latency_ms=5,
            input_tokens=10,
            output_tokens=12,
        ),
    ])

    result = invoke_structured_or_freetext(_request(), registry, ExampleSchema, max_retries=2)

    assert result.success is True
    assert result.raw_text is not None
    assert len(registry.requests) == 1
    assert registry.requests[0].json_schema is not None


def test_invoke_structured_or_freetext_falls_back_to_freetext_when_structured_invalid():
    registry = FakeRegistry([
        ProviderGenerateResult(
            success=True,
            raw_text="not json",
            error_code=None,
            error_message=None,
            latency_ms=5,
            input_tokens=10,
            output_tokens=12,
        ),
        ProviderGenerateResult(
            success=True,
            raw_text='{"answer":"fallback","confidence":0.7}',
            error_code=None,
            error_message=None,
            latency_ms=5,
            input_tokens=10,
            output_tokens=12,
        ),
    ])

    result = invoke_structured_or_freetext(_request(), registry, ExampleSchema, max_retries=2)

    assert result.success is True
    assert "fallback" in (result.raw_text or "")
    assert len(registry.requests) == 2
    assert registry.requests[1].json_schema is None


def test_invoke_structured_or_freetext_returns_last_result_if_all_attempts_fail():
    final = ProviderGenerateResult(
        success=False,
        raw_text=None,
        error_code="HTTP_500",
        error_message="provider error",
        latency_ms=5,
        input_tokens=None,
        output_tokens=None,
    )
    registry = FakeRegistry([
        ProviderGenerateResult(
            success=True,
            raw_text="still not json",
            error_code=None,
            error_message=None,
            latency_ms=5,
            input_tokens=10,
            output_tokens=12,
        ),
        ProviderGenerateResult(
            success=True,
            raw_text="also bad",
            error_code=None,
            error_message=None,
            latency_ms=5,
            input_tokens=10,
            output_tokens=12,
        ),
        final,
    ])

    result = invoke_structured_or_freetext(_request(), registry, ExampleSchema, max_retries=2)

    assert result.error_code == "HTTP_500"
    assert result.success is False

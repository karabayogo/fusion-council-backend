"""Phase 2 regression tests for structured-output passthrough in OpenAI-compatible provider."""

from fusion_council_service.domain.types import ProviderGenerateRequest
from fusion_council_service.providers.openai_compatible import OpenAICompatibleProvider


class _FakeResponse:
    def __init__(self, status_code=200, body=None):
        self.status_code = status_code
        self._body = body or {
            "choices": [{"message": {"content": "ok"}}],
            "usage": {"prompt_tokens": 10, "completion_tokens": 12},
        }
        self.text = ""

    def raise_for_status(self):
        return None

    def json(self):
        return self._body


def test_openai_compatible_provider_sends_response_format(monkeypatch):
    captured = {}

    def fake_post(url, json, headers, timeout):
        captured["url"] = url
        captured["json"] = json
        captured["headers"] = headers
        captured["timeout"] = timeout
        return _FakeResponse()

    monkeypatch.setattr("fusion_council_service.providers.openai_compatible.httpx.post", fake_post)

    provider = OpenAICompatibleProvider(api_key="k", base_url="https://example.test/v1")
    request = ProviderGenerateRequest(
        alias="a",
        provider="opencode_go",
        provider_model="deepseek-v4-pro",
        system_prompt="system",
        user_prompt="user",
        max_output_tokens=100,
        temperature=0.2,
        response_format={"type": "json_object", "schema": {"type": "object"}},
    )

    result = provider.generate(request)

    assert result.success is True
    assert captured["json"]["response_format"]["type"] == "json_object"


def test_openai_compatible_provider_uses_json_schema_when_response_format_absent(monkeypatch):
    captured = {}

    def fake_post(url, json, headers, timeout):
        captured["json"] = json
        return _FakeResponse()

    monkeypatch.setattr("fusion_council_service.providers.openai_compatible.httpx.post", fake_post)

    provider = OpenAICompatibleProvider(api_key="k", base_url="https://example.test/v1")
    request = ProviderGenerateRequest(
        alias="a",
        provider="opencode_go",
        provider_model="deepseek-v4-pro",
        system_prompt=None,
        user_prompt="user",
        max_output_tokens=100,
        temperature=0.2,
        json_schema={"type": "object", "properties": {"answer": {"type": "string"}}},
    )

    result = provider.generate(request)

    assert result.success is True
    response_format = captured["json"]["response_format"]
    assert response_format["type"] == "json_schema"
    assert response_format["json_schema"]["schema"]["type"] == "object"

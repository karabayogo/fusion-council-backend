"""OpenAI-compatible provider client using /chat/completions via httpx."""

import time

import httpx

from fusion_council_service.domain.types import ProviderGenerateRequest, ProviderGenerateResult


class OpenAICompatibleProvider:
    """Calls OpenAI-compatible chat/completions endpoint using Bearer auth."""

    def __init__(self, api_key: str, base_url: str):
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")

    def generate(self, request: ProviderGenerateRequest) -> ProviderGenerateResult:
        start = time.monotonic()
        try:
            messages = []
            if request.system_prompt:
                messages.append({"role": "system", "content": request.system_prompt})
            messages.append({"role": "user", "content": request.user_prompt})

            payload = {
                "model": request.provider_model,
                "messages": messages,
                "temperature": request.temperature,
                "max_tokens": request.max_output_tokens,
            }

            if request.response_format:
                payload["response_format"] = request.response_format
            elif request.json_schema:
                payload["response_format"] = {
                    "type": "json_schema",
                    "json_schema": {
                        "name": "structured_response",
                        "strict": True,
                        "schema": request.json_schema,
                    },
                }

            response = httpx.post(
                f"{self._base_url}/chat/completions",
                json=payload,
                headers={
                    "Authorization": f"Bearer {self._api_key}",
                    "Content-Type": "application/json",
                },
                timeout=300.0,
            )
            latency_ms = int((time.monotonic() - start) * 1000)

            if response.status_code == 401:
                return ProviderGenerateResult(
                    success=False,
                    raw_text=None,
                    error_code="AUTH_FAILED",
                    error_message="OpenAI-compatible API key rejected",
                    latency_ms=latency_ms,
                    input_tokens=None,
                    output_tokens=None,
                )

            # Explicitly classify 4xx/5xx before raise_for_status swallows them
            if response.status_code >= 400:
                body = response.text[:500] if response.text else "(empty)"
                return ProviderGenerateResult(
                    success=False,
                    raw_text=None,
                    error_code=f"HTTP_{response.status_code}",
                    error_message=f"Provider returned {response.status_code}: {body}",
                    latency_ms=latency_ms,
                    input_tokens=None,
                    output_tokens=None,
                )

            response.raise_for_status()
            data = response.json()

            choices = data.get("choices", [])
            raw_text = ""
            if choices:
                raw_text = choices[0].get("message", {}).get("content", "")

            usage = data.get("usage", {})
            input_tokens = usage.get("prompt_tokens")
            output_tokens = usage.get("completion_tokens")

            return ProviderGenerateResult(
                success=True,
                raw_text=raw_text,
                error_code=None,
                error_message=None,
                latency_ms=latency_ms,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
            )
        except httpx.TimeoutException:
            latency_ms = int((time.monotonic() - start) * 1000)
            return ProviderGenerateResult(
                success=False,
                raw_text=None,
                error_code="TIMEOUT",
                error_message="OpenAI-compatible request timed out",
                latency_ms=latency_ms,
                input_tokens=None,
                output_tokens=None,
            )
        except Exception as e:
            latency_ms = int((time.monotonic() - start) * 1000)
            return ProviderGenerateResult(
                success=False,
                raw_text=None,
                error_code="PROVIDER_ERROR",
                error_message=str(e),
                latency_ms=latency_ms,
                input_tokens=None,
                output_tokens=None,
            )

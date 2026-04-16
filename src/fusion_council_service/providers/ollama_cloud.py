"""Ollama cloud provider client using httpx directly (no ollama SDK)."""

import time
from typing import Optional

import httpx

from fusion_council_service.domain.types import ProviderGenerateRequest, ProviderGenerateResult
from fusion_council_service.logging_utils import get_logger

logger = get_logger("fusion_council_service.providers.ollama")


class OllamaCloudProvider:
    """Calls Ollama cloud /api/chat endpoint using Bearer auth."""

    def __init__(self, api_key: str, base_url: str = "https://ollama.com"):
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")

    def generate(self, request: ProviderGenerateRequest) -> ProviderGenerateResult:
        start = time.monotonic()
        try:
            payload = {
                "model": request.provider_model,
                "messages": [],
                "stream": False,
                "options": {
                    "temperature": request.temperature,
                    "num_predict": request.max_output_tokens,
                },
            }
            if request.system_prompt:
                payload["messages"].append({"role": "system", "content": request.system_prompt})
            payload["messages"].append({"role": "user", "content": request.user_prompt})

            response = httpx.post(
                f"{self._base_url}/api/chat",
                json=payload,
                headers={"Authorization": f"Bearer {self._api_key}"},
                timeout=300.0,
            )
            latency_ms = int((time.monotonic() - start) * 1000)

            if response.status_code == 401:
                return ProviderGenerateResult(
                    success=False, raw_text=None,
                    error_code="AUTH_FAILED", error_message="Ollama API key rejected",
                    latency_ms=latency_ms, input_tokens=None, output_tokens=None,
                )

            response.raise_for_status()
            data = response.json()

            raw_text = data.get("message", {}).get("content", "")
            # Ollama doesn't always return token counts
            input_tokens = data.get("prompt_eval_count")
            output_tokens = data.get("eval_count")

            return ProviderGenerateResult(
                success=True, raw_text=raw_text,
                error_code=None, error_message=None,
                latency_ms=latency_ms, input_tokens=input_tokens, output_tokens=output_tokens,
            )

        except httpx.TimeoutException:
            latency_ms = int((time.monotonic() - start) * 1000)
            return ProviderGenerateResult(
                success=False, raw_text=None,
                error_code="TIMEOUT", error_message="Ollama request timed out",
                latency_ms=latency_ms, input_tokens=None, output_tokens=None,
            )
        except Exception as e:
            latency_ms = int((time.monotonic() - start) * 1000)
            return ProviderGenerateResult(
                success=False, raw_text=None,
                error_code="PROVIDER_ERROR", error_message=str(e),
                latency_ms=latency_ms, input_tokens=None, output_tokens=None,
            )
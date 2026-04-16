"""MiniMax Token Plan provider client using Anthropic SDK with custom base URL."""

import time
from typing import Optional

import anthropic

from fusion_council_service.domain.types import ProviderGenerateRequest, ProviderGenerateResult
from fusion_council_service.logging_utils import get_logger

logger = get_logger("fusion_council_service.providers.minimax")


class MiniMaxTokenPlanProvider:
    """Calls MiniMax via Anthropic-compatible SDK with custom base URL."""

    def __init__(self, api_key: str, base_url: str = "https://api.minimax.io/anthropic"):
        self._api_key = api_key
        self._base_url = base_url
        self._client = anthropic.Anthropic(
            base_url=base_url,
            api_key=api_key,
        )

    def generate(self, request: ProviderGenerateRequest) -> ProviderGenerateResult:
        start = time.monotonic()
        try:
            messages = []
            if request.system_prompt:
                # Anthropic SDK passes system as a separate param
                pass

            kwargs = {
                "model": request.provider_model,
                "max_tokens": request.max_output_tokens,
                "temperature": request.temperature,
                "messages": [{"role": "user", "content": request.user_prompt}],
            }
            if request.system_prompt:
                kwargs["system"] = request.system_prompt

            response = self._client.messages.create(**kwargs)
            latency_ms = int((time.monotonic() - start) * 1000)

            raw_text = response.content[0].text if response.content else ""
            input_tokens = response.usage.input_tokens if response.usage else None
            output_tokens = response.usage.output_tokens if response.usage else None

            return ProviderGenerateResult(
                success=True, raw_text=raw_text,
                error_code=None, error_message=None,
                latency_ms=latency_ms, input_tokens=input_tokens, output_tokens=output_tokens,
            )

        except anthropic.AuthenticationError as e:
            latency_ms = int((time.monotonic() - start) * 1000)
            return ProviderGenerateResult(
                success=False, raw_text=None,
                error_code="AUTH_FAILED", error_message=f"MiniMax auth failed: {e}",
                latency_ms=latency_ms, input_tokens=None, output_tokens=None,
            )
        except anthropic.APITimeoutError:
            latency_ms = int((time.monotonic() - start) * 1000)
            return ProviderGenerateResult(
                success=False, raw_text=None,
                error_code="TIMEOUT", error_message="MiniMax request timed out",
                latency_ms=latency_ms, input_tokens=None, output_tokens=None,
            )
        except Exception as e:
            latency_ms = int((time.monotonic() - start) * 1000)
            return ProviderGenerateResult(
                success=False, raw_text=None,
                error_code="PROVIDER_ERROR", error_message=str(e),
                latency_ms=latency_ms, input_tokens=None, output_tokens=None,
            )
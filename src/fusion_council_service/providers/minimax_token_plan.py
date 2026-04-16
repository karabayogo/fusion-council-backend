"""MiniMax Token Plan provider client using Anthropic SDK with custom base URL."""

import time
from typing import Optional

import anthropic

from fusion_council_service.domain.types import ProviderGenerateRequest, ProviderGenerateResult
from fusion_council_service.logging_utils import get_logger

logger = get_logger("fusion_council_service.providers.minimax")

# MiniMax-M2.7 returns ThinkingBlock + TextBlock via the Anthropic API.
# The thinking budget is carved out of max_tokens, so low max_tokens values
# (e.g. 50) can result in the entire budget being consumed by thinking
# with nothing left for the actual text response. We enforce a minimum
# max_tokens of 256 to ensure room for both thinking and text output.
MINIMAX_MIN_MAX_TOKENS = 256

# Maximum retries when thinking consumes the entire token budget
MAX_THINKING_RETRIES = 1


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

            # Enforce minimum max_tokens to leave room for text after thinking
            effective_max_tokens = max(request.max_output_tokens, MINIMAX_MIN_MAX_TOKENS)

            kwargs = {
                "model": request.provider_model,
                "max_tokens": effective_max_tokens,
                "temperature": request.temperature,
                "messages": [{"role": "user", "content": request.user_prompt}],
            }
            if request.system_prompt:
                kwargs["system"] = request.system_prompt

            response = self._client.messages.create(**kwargs)
            latency_ms = int((time.monotonic() - start) * 1000)

            # Anthropic SDK may return ThinkingBlock + TextBlock; extract last TextBlock
            text_blocks = [b for b in response.content if getattr(b, 'text', None) is not None]
            raw_text = text_blocks[-1].text if text_blocks else ""

            # If thinking consumed the entire budget (stop_reason=max_tokens, no text),
            # retry once with double the max_tokens
            if not raw_text and response.stop_reason == "max_tokens" and MAX_THINKING_RETRIES > 0:
                logger.warning(
                    f"MiniMax thinking consumed all {effective_max_tokens} tokens, "
                    f"retrying with {effective_max_tokens * 2}"
                )
                kwargs["max_tokens"] = effective_max_tokens * 2
                response = self._client.messages.create(**kwargs)
                latency_ms = int((time.monotonic() - start) * 1000)
                text_blocks = [b for b in response.content if getattr(b, 'text', None) is not None]
                raw_text = text_blocks[-1].text if text_blocks else ""

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
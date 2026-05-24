"""MiniMax Token Plan provider client using Anthropic SDK with custom base URL."""

import time

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

            response = self._stream_generate(kwargs)
            latency_ms = int((time.monotonic() - start) * 1000)

            input_tokens = response.get("input_tokens")
            output_tokens = response.get("output_tokens")

            return ProviderGenerateResult(
                success=True, raw_text=response["text"],
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

    def _stream_generate(self, kwargs: dict) -> dict:
        """Execute a streaming Anthropic messages.stream() call and accumulate results.

        MiniMax's API requires streaming for operations that may exceed 10 minutes.
        We use the context-manager pattern to iterate events, extract text deltas,
        and retrieve the final Message for usage stats and stop_reason.  If thinking
        consumed the entire budget (stop_reason=max_tokens, no text), we retry once
        with double max_tokens.
        """
        accumulated_text = []
        input_tokens = None
        output_tokens = None
        stop_reason = None

        with self._client.messages.stream(**kwargs) as stream:
            for event in stream:
                # MessageDeltaEvent carries usage stats (including output_tokens) and
                # the final stop_reason when the stream completes
                if hasattr(event, "type") and event.type == "message_delta":
                    stop_reason = getattr(event.delta, "stop_reason", None)
                    if event.usage:
                        input_tokens = event.usage.input_tokens
                        output_tokens = event.usage.output_tokens
                # ContentBlockDeltaEvent carries incremental content deltas
                elif hasattr(event, "type") and event.type == "content_block_delta":
                    delta = getattr(event, "delta", None)
                    if delta is None:
                        continue
                    delta_type = getattr(delta, "type", None)
                    if delta_type == "text_delta":
                        text = getattr(delta, "text", "")
                        if text:
                            accumulated_text.append(text)
                    elif delta_type == "thinking_delta":
                        # Thinking blocks are internal; accumulate for potential logging
                        thinking = getattr(delta, "thinking", "")
                        if thinking:
                            accumulated_text.append(f"[thinking: {thinking[:50]}...]")

            # Retrieve the final parsed message to confirm stop_reason and content blocks
            final_message = stream.get_final_message()
            if final_message is not None:
                stop_reason = final_message.stop_reason
                if final_message.usage:
                    input_tokens = final_message.usage.input_tokens
                    output_tokens = final_message.usage.output_tokens

        # If thinking consumed the entire budget (stop_reason=max_tokens, no text),
        # retry once with double the max_tokens using streaming
        text_so_far = "".join(accumulated_text)
        if not text_so_far and stop_reason == "max_tokens" and MAX_THINKING_RETRIES > 0:
            logger.warning(
                f"MiniMax thinking consumed all {kwargs['max_tokens']} tokens, "
                f"retrying with {kwargs['max_tokens'] * 2}"
            )
            kwargs["max_tokens"] = kwargs["max_tokens"] * 2
            return self._stream_generate(kwargs)

        return {
            "text": text_so_far,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
        }
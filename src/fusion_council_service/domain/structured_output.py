"""Structured-output invocation with freetext fallback."""

import json
from typing import Type

from fusion_council_service.domain.types import ProviderGenerateRequest, ProviderGenerateResult
from fusion_council_service.logging_utils import get_logger
from fusion_council_service.providers.registry import ProviderRegistry

logger = get_logger("structured_output")


def _parse_and_validate(raw_text: str, response_model: Type) -> bool:
    """Return True when raw_text is valid JSON matching response_model."""
    payload = json.loads(raw_text)
    response_model.model_validate(payload)
    return True


def invoke_structured_or_freetext(
    request: ProviderGenerateRequest,
    registry: ProviderRegistry,
    response_model: Type,
    max_retries: int = 2,
    system_suffix: str = " Respond with valid JSON only, no markdown, no explanation.",
) -> ProviderGenerateResult:
    """Try structured output first, then fallback to freetext+JSON parse."""
    attempts = max(1, max_retries)
    structured_request = _build_structured_request(request, response_model, system_suffix)
    last_result: ProviderGenerateResult | None = None

    for attempt in range(attempts):
        structured_result = registry.generate(structured_request)
        last_result = structured_result
        if structured_result.success and structured_result.raw_text:
            try:
                _parse_and_validate(structured_result.raw_text, response_model)
                logger.info(
                    "Structured output parse succeeded",
                    event_type="structured.success",
                    attempt=attempt + 1,
                )
                return structured_result
            except Exception as exc:
                logger.warning(
                    f"Structured output parse failed on attempt {attempt + 1}: {exc}",
                    event_type="structured.parse_failed",
                )

        if attempt < attempts - 1:
            freetext_request = _build_freetext_request(request, system_suffix)
            freetext_result = registry.generate(freetext_request)
            last_result = freetext_result
            if freetext_result.success and freetext_result.raw_text:
                try:
                    _parse_and_validate(freetext_result.raw_text, response_model)
                    logger.info(
                        "Freetext fallback parse succeeded",
                        event_type="structured.fallback_success",
                        attempt=attempt + 1,
                    )
                    return freetext_result
                except Exception as exc:
                    logger.warning(
                        f"Freetext parse failed on attempt {attempt + 1}: {exc}",
                        event_type="structured.fallback_failed",
                    )

    if last_result is None:
        return ProviderGenerateResult(
            success=False,
            raw_text=None,
            error_code="STRUCTURED_OUTPUT_NO_ATTEMPTS",
            error_message="No structured-output attempts were executed",
            latency_ms=0,
            input_tokens=None,
            output_tokens=None,
        )

    logger.error("invoke_structured_or_freetext exhausted all retries", event_type="structured.exhausted")
    return last_result


def _build_structured_request(
    request: ProviderGenerateRequest,
    response_model: Type,
    system_suffix: str,
) -> ProviderGenerateRequest:
    """Attach schema metadata to a provider request."""
    suffix = system_suffix if system_suffix.startswith(" ") else f" {system_suffix}"
    return ProviderGenerateRequest(
        alias=request.alias,
        provider=request.provider,
        provider_model=request.provider_model,
        system_prompt=(request.system_prompt or "") + suffix,
        user_prompt=request.user_prompt,
        max_output_tokens=request.max_output_tokens,
        temperature=request.temperature,
        json_schema=response_model.model_json_schema(),
        response_format={"type": "json_object", "schema": response_model.model_json_schema()},
    )


def _build_freetext_request(
    request: ProviderGenerateRequest,
    system_suffix: str,
) -> ProviderGenerateRequest:
    """Build plain-text request with explicit JSON-only instructions."""
    suffix = system_suffix if system_suffix.startswith(" ") else f" {system_suffix}"
    return ProviderGenerateRequest(
        alias=request.alias,
        provider=request.provider,
        provider_model=request.provider_model,
        system_prompt=(request.system_prompt or "") + suffix,
        user_prompt=request.user_prompt + "\n\nIMPORTANT: Respond with valid JSON only.",
        max_output_tokens=request.max_output_tokens,
        temperature=request.temperature,
        json_schema=None,
        response_format=None,
    )

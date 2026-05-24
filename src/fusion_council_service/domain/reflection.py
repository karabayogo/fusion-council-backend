"""Reflection prompt builder and generator for decision outcome learning."""

from __future__ import annotations

from typing import Optional, Tuple

from fusion_council_service.domain.types import ProviderGenerateRequest
from fusion_council_service.logging_utils import get_logger
from fusion_council_service.providers.registry import ProviderRegistry

logger = get_logger("reflection")


_ALIAS_PROVIDER_PREFIX = {
    "minimax": "minimax_token_plan",
    "opencode-go": "opencode_go",
    "openai-codex": "openai_codex",
    "ollama": "ollama_cloud",
}


def build_reflection_prompt(
    prompt: str,
    final_answer: str,
    rating: str,
    outcome_raw: float,
) -> str:
    """Build a domain-agnostic reflection prompt for 2-4 sentence lessons."""
    rating_desc = {
        "helpful": "positively received by the user",
        "not_helpful": "not helpful according to the user",
        "partial": "partially helpful per the user",
    }.get(rating, "rated by the user")

    return f"""You are a meta-analyst reviewing a deliberative AI system's answer.

ORIGINAL QUESTION:
{(prompt or '')[:1000]}

FINAL ANSWER GENERATED:
{(final_answer or '')[:2000]}

USER FEEDBACK:
Rating: {rating} ({rating_desc}), numeric score: {outcome_raw}/5

YOUR TASK:
Write exactly 2-4 sentences of plain prose (no bullets, no headers, no markdown).

Cover in order:
1. What was correct or incorrect about this answer?
2. What one specific improvement should the system remember for similar questions?

Be specific and terse. Your output will be stored verbatim and re-read by future deliberative runs."""


def _resolve_provider_model_from_alias(
    alias: str,
    provider_override: Optional[str] = None,
    provider_model_override: Optional[str] = None,
) -> Tuple[Optional[str], Optional[str]]:
    """Resolve provider/provider_model from a catalog-style alias.

    Expected alias shape: <prefix>/<provider_model>, e.g. minimax/MiniMax-M2.7
    """
    if provider_override and provider_model_override:
        return provider_override, provider_model_override

    if "/" not in alias:
        return provider_override, provider_model_override

    prefix, provider_model = alias.split("/", 1)
    provider = _ALIAS_PROVIDER_PREFIX.get(prefix)
    if provider is None:
        return provider_override, provider_model_override

    if provider_override and provider_override != provider:
        return provider_override, provider_model_override or provider_model

    return provider, provider_model_override or provider_model


def generate_reflection(
    prompt: str,
    final_answer: str,
    rating: str,
    outcome_raw: float,
    provider_registry: ProviderRegistry,
    backup_role_alias: str = "minimax/MiniMax-M2.7",
    max_retries: int = 2,
    provider_override: Optional[str] = None,
    provider_model_override: Optional[str] = None,
) -> str:
    """Generate reflection text via ProviderRegistry.

    Returns empty string when all attempts fail or alias cannot be resolved.
    """
    provider, provider_model = _resolve_provider_model_from_alias(
        backup_role_alias,
        provider_override=provider_override,
        provider_model_override=provider_model_override,
    )
    if not provider or not provider_model:
        logger.error(
            "Reflection alias could not be resolved to provider/model",
            event_type="reflection.resolve_failed",
            backup_role_alias=backup_role_alias,
            provider_override=provider_override,
            provider_model_override=provider_model_override,
        )
        return ""

    reflection_prompt = build_reflection_prompt(prompt, final_answer, rating, outcome_raw)
    attempts = max(1, max_retries)

    for attempt in range(attempts):
        request = ProviderGenerateRequest(
            alias=backup_role_alias,
            provider=provider,
            provider_model=provider_model,
            system_prompt="You are a concise meta-analyst. Respond with 2-4 sentences only.",
            user_prompt=reflection_prompt,
            max_output_tokens=500,
            temperature=0.3,
        )
        result = provider_registry.generate(request)

        if result.success and result.raw_text:
            reflection = result.raw_text.strip()
            if len(reflection) > 500:
                reflection = reflection[:497] + "..."
            logger.info(
                "Reflection generated",
                event_type="reflection.generated",
                attempt=attempt + 1,
                run_reflection_len=len(reflection),
            )
            return reflection

        logger.warning(
            "Reflection attempt failed",
            event_type="reflection.failed",
            attempt=attempt + 1,
            error_code=result.error_code,
        )

    logger.error("Reflection generation exhausted all retries", event_type="reflection.exhausted")
    return ""

"""Provider registry — maps provider name to client instance."""

from typing import Dict

from fusion_council_service.domain.types import ProviderGenerateRequest, ProviderGenerateResult
from fusion_council_service.providers.minimax_token_plan import MiniMaxTokenPlanProvider
from fusion_council_service.providers.ollama_cloud import OllamaCloudProvider


class ProviderRegistry:
    """Holds initialized provider clients, dispatches generate() calls."""

    def __init__(self):
        self._providers: Dict[str, object] = {}

    def register(self, name: str, client: object) -> None:
        self._providers[name] = client

    def get(self, name: str):
        return self._providers.get(name)

    def generate(self, request: ProviderGenerateRequest) -> ProviderGenerateResult:
        """Dispatch a generate request to the appropriate provider."""
        provider = self._providers.get(request.provider)
        if provider is None:
            return ProviderGenerateResult(
                success=False, raw_text=None,
                error_code="UNKNOWN_PROVIDER",
                error_message=f"No provider registered for '{request.provider}'",
                latency_ms=0, input_tokens=None, output_tokens=None,
            )
        return provider.generate(request)


def build_provider_registry(settings) -> ProviderRegistry:
    """Build and return a provider registry with configured clients."""
    registry = ProviderRegistry()

    # MiniMax Token Plan via Anthropic SDK
    registry.register(
        "minimax_token_plan",
        MiniMaxTokenPlanProvider(
            api_key=settings.MINIMAX_API_KEY,
            base_url=settings.MINIMAX_ANTHROPIC_BASE_URL,
        ),
    )

    # Ollama Cloud via httpx
    registry.register(
        "ollama_cloud",
        OllamaCloudProvider(
            api_key=settings.OLLAMA_API_KEY,
            base_url=settings.OLLAMA_BASE_URL,
        ),
    )

    return registry
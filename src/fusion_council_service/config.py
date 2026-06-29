"""Configuration via environment variables using pydantic-settings."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    APP_NAME: str = "fusion-council-service"
    APP_ENV: str = "development"
    HOST: str = "0.0.0.0"
    PORT: int = 8080
    # Database: either DATABASE_URL (PostgreSQL) or DATABASE_PATH (SQLite)
    DATABASE_URL: str = ""
    DATABASE_PATH: str = ""
    SERVICE_API_KEYS: str
    SERVICE_ADMIN_API_KEYS: str

    # Provider credentials
    MINIMAX_API_KEY: str = ""
    # Backward-compat env/arg name kept for existing deployments/tests
    MINIMAX_TOKEN_PLAN_API_KEY: str = ""
    MINIMAX_ANTHROPIC_BASE_URL: str = "https://api.minimax.io/anthropic"
    OLLAMA_API_KEY: str = ""
    OLLAMA_BASE_URL: str = "https://ollama.com"

    OPENAI_CODEX_API_KEY: str = ""
    OPENAI_CODEX_BASE_URL: str = "https://api.openai.com/v1"
    OPENCODE_GO_API_KEY: str = ""
    OPENCODE_GO_BASE_URL: str = "https://opencode.ai/zen/go/v1"

    WORKER_POLL_INTERVAL_MS: int = 1000
    WORKER_HEARTBEAT_INTERVAL_MS: int = 5000
    MAX_PARALLEL_MODEL_CALLS: int = 3
    SYNC_TIMEOUT_SECONDS: int = 120
    SSE_POLL_INTERVAL_MS: int = 500
    MODEL_CATALOG_PATH: str = "./config/models.yaml"
    DECISION_LOG_MAX_ENTRIES: int = 500
    REFLECTION_ROLE_ALIAS: str = "minimax/MiniMax-M2.7"
    ORCHESTRATOR_ENGINE: str = "legacy"
    ORCHESTRATOR_LANGGRAPH_MODES: str = ""
    LANGGRAPH_CHECKPOINT_ENABLED: bool = False
    LANGGRAPH_CHECKPOINT_DB_URL: str = ""
    LANGGRAPH_THREAD_NAMESPACE: str = "fusion-council"
    LANGGRAPH_ENGINE_VERSION: str = "v1"

    # Public base URL for client-facing responses. When set, create_run()
    # and respond_sync() build absolute status_url values from this. When
    # unset, the routes return relative paths (e.g. /v1/runs/{id}) so the
    # internal HOST=0.0.0.0 bind address never leaks to clients.
    # Strategic fix from RCA-6 of run-page live-streaming plan.
    PUBLIC_BASE_URL: str = ""

    # Per-stage max output token caps (RCA-4). Defaults follow the
    # run-page live-streaming implementation plan (2026-06-29):
    #   first_opinion: 1200, peer_review: 800, debate: 800,
    #   synthesis: 1200, verification: 400.
    # Stage code clamps to min(run.max_output_tokens, STAGE_TOKEN_CAPS[stage])
    # so the user's run-level cap is always respected.
    # JSON shape makes it env-tunable via STAGE_TOKEN_CAPS env var or
    # GitOps values override.
    STAGE_TOKEN_CAPS: str = (
        '{"first_opinion": 1200, "peer_review": 800, '
        '"debate": 800, "synthesis": 1200, "verification": 400}'
    )

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "extra": "ignore"}

    @property
    def minimax_api_key_effective(self) -> str:
        return self.MINIMAX_API_KEY or self.MINIMAX_TOKEN_PLAN_API_KEY

    @property
    def service_api_keys(self) -> list[str]:
        return [k.strip() for k in self.SERVICE_API_KEYS.split(",") if k.strip()]

    @property
    def service_admin_api_keys(self) -> list[str]:
        return [k.strip() for k in self.SERVICE_ADMIN_API_KEYS.split(",") if k.strip()]

    @property
    def stage_token_caps(self) -> dict[str, int]:
        """Parse the STAGE_TOKEN_CAPS JSON string into a dict.

        Returns sensible defaults if the value is malformed (defensive: we
        never want a config typo to crash a production run).
        """
        defaults: dict[str, int] = {
            "first_opinion": 1200,
            "peer_review": 800,
            "debate": 800,
            "synthesis": 1200,
            "verification": 400,
        }
        raw = (self.STAGE_TOKEN_CAPS or "").strip()
        if not raw:
            return defaults
        try:
            import json
            parsed = json.loads(raw)
            if not isinstance(parsed, dict):
                return defaults
            out: dict[str, int] = {}
            for k, v in parsed.items():
                try:
                    out[str(k)] = int(v)
                except (TypeError, ValueError):
                    continue
            # Merge with defaults so missing keys get safe values
            for k, v in defaults.items():
                out.setdefault(k, v)
            return out
        except Exception:
            return defaults

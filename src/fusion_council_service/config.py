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

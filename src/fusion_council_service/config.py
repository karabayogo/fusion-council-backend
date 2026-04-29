"""Configuration via environment variables using pydantic-settings."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    APP_NAME: str = "fusion-council-service"
    APP_ENV: str = "development"
    HOST: str = "0.0.0.0"
    PORT: int = 8080
    DATABASE_PATH: str
    SERVICE_API_KEYS: str
    SERVICE_ADMIN_API_KEYS: str
    MINIMAX_API_KEY: str
    MINIMAX_ANTHROPIC_BASE_URL: str = "https://api.minimax.io/anthropic"
    MINIMAX_TOKEN_PLAN_API_KEY: str = ""
    OLLAMA_API_KEY: str
    OLLAMA_BASE_URL: str = "https://ollama.com"
    WORKER_POLL_INTERVAL_MS: int = 1000
    WORKER_HEARTBEAT_INTERVAL_MS: int = 5000
    MAX_PARALLEL_MODEL_CALLS: int = 3
    SYNC_TIMEOUT_SECONDS: int = 120
    SSE_POLL_INTERVAL_MS: int = 500
    MODEL_CATALOG_PATH: str = "./config/models.yaml"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}

    @property
    def service_api_keys(self) -> list[str]:
        return [k.strip() for k in self.SERVICE_API_KEYS.split(",") if k.strip()]

    @property
    def service_admin_api_keys(self) -> list[str]:
        return [k.strip() for k in self.SERVICE_ADMIN_API_KEYS.split(",") if k.strip()]

"""Entry point for running the worker directly: python -m fusion_council_service.domain.worker_loop"""

import signal
import sys
import os

# Add src to path for development
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from dotenv import load_dotenv
load_dotenv()

from fusion_council_service.config import Settings
from fusion_council_service.db import open_db_connection, initialize_schema
from fusion_council_service.domain.worker_loop import Worker
from fusion_council_service.logging_utils import setup_logging, get_logger
from fusion_council_service.model_catalog import load_and_validate_catalog
from fusion_council_service.providers.registry import build_provider_registry

setup_logging()
logger = get_logger("fusion_council_service")

# Load settings from environment
settings = Settings(
    DATABASE_PATH=os.environ.get("DATABASE_PATH", "./data/fusion_council.db"),
    SERVICE_API_KEYS=os.environ.get("SERVICE_API_KEYS", "worker-only"),
    SERVICE_ADMIN_API_KEYS=os.environ.get("SERVICE_ADMIN_API_KEYS", ""),
    MINIMAX_TOKEN_PLAN_API_KEY=os.environ.get("MINIMAX_TOKEN_PLAN_API_KEY", ""),
    OLLAMA_API_KEY=os.environ.get("OLLAMA_API_KEY", ""),
    MINIMAX_ANTHROPIC_BASE_URL=os.environ.get("MINIMAX_ANTHROPIC_BASE_URL", "https://api.minimax.io/anthropic"),
    OLLAMA_BASE_URL=os.environ.get("OLLAMA_BASE_URL", "https://ollama.com"),
)


def main() -> None:
    logger.info("Starting Fusion Council worker", event_type="worker.start")

    db = open_db_connection(settings.DATABASE_PATH)
    initialize_schema(db)

    catalog = load_and_validate_catalog(settings, db)
    logger.info(f"Model catalog loaded: {len(catalog)} models")

    registry = build_provider_registry(settings)

    worker = Worker(
        db_path=settings.DATABASE_PATH,
        registry=registry,
        catalog=catalog,
        poll_interval_ms=settings.WORKER_POLL_INTERVAL_MS,
        heartbeat_interval_ms=settings.WORKER_HEARTBEAT_INTERVAL_MS,
    )

    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down")
        worker.stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    worker.run()


if __name__ == "__main__":
    main()
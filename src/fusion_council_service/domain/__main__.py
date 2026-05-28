"""Entry point for running the worker directly: python -m fusion_council_service.domain.worker_loop"""

import signal
import sys
import os
import asyncio

# Add src to path for development
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from dotenv import load_dotenv
from fusion_council_service.config import Settings
from fusion_council_service.db import new_session, initialize_schema
from fusion_council_service.domain.worker_loop import Worker
from fusion_council_service.logging_utils import setup_logging, get_logger
from fusion_council_service.model_catalog import load_and_validate_catalog
from fusion_council_service.providers.registry import build_provider_registry
from fusion_council_service.startup import run_shutdown_sync, run_startup_sync

setup_logging()
logger = get_logger("fusion_council_service")

# Load settings from environment
settings = Settings(
    DATABASE_URL=os.environ.get("DATABASE_URL", ""),
    DATABASE_PATH=os.environ.get("DATABASE_PATH", "./data/fusion_council.db"),
    SERVICE_API_KEYS=os.environ.get("SERVICE_API_KEYS", "worker-only"),
    SERVICE_ADMIN_API_KEYS=os.environ.get("SERVICE_ADMIN_API_KEYS", ""),
    MINIMAX_API_KEY=os.environ.get("MINIMAX_API_KEY", ""),
    OLLAMA_API_KEY=os.environ.get("OLLAMA_API_KEY", ""),
    MINIMAX_ANTHROPIC_BASE_URL=os.environ.get("MINIMAX_ANTHROPIC_BASE_URL", "https://api.minimax.io/anthropic"),
    OLLAMA_BASE_URL=os.environ.get("OLLAMA_BASE_URL", "https://ollama.com"),
    ORCHESTRATOR_ENGINE=os.environ.get("ORCHESTRATOR_ENGINE", "legacy"),
    ORCHESTRATOR_LANGGRAPH_MODES=os.environ.get("ORCHESTRATOR_LANGGRAPH_MODES", ""),
    LANGGRAPH_CHECKPOINT_ENABLED=os.environ.get("LANGGRAPH_CHECKPOINT_ENABLED", "false"),
    LANGGRAPH_CHECKPOINT_DB_URL=os.environ.get("LANGGRAPH_CHECKPOINT_DB_URL", ""),
    LANGGRAPH_THREAD_NAMESPACE=os.environ.get("LANGGRAPH_THREAD_NAMESPACE", "fusion-council"),
    LANGGRAPH_ENGINE_VERSION=os.environ.get("LANGGRAPH_ENGINE_VERSION", "v1"),
)


def main() -> None:
    load_dotenv()  # noqa: E402 (must be before Settings())
    logger.info("Starting Fusion Council worker", event_type="worker.start")

    db = new_session()
    initialize_schema(db)

    catalog = load_and_validate_catalog(settings, db)
    logger.info(f"Model catalog loaded: {len(catalog)} models")

    registry = build_provider_registry(settings)

    worker = Worker(
        db_path=settings.DATABASE_PATH,
        db_url=settings.DATABASE_URL,
        registry=registry,
        catalog=catalog,
        poll_interval_ms=settings.WORKER_POLL_INTERVAL_MS,
        heartbeat_interval_ms=settings.WORKER_HEARTBEAT_INTERVAL_MS,
        orchestrator_engine=settings.ORCHESTRATOR_ENGINE,
        orchestrator_langgraph_modes=settings.ORCHESTRATOR_LANGGRAPH_MODES,
        langgraph_thread_namespace=settings.LANGGRAPH_THREAD_NAMESPACE,
        langgraph_engine_version=settings.LANGGRAPH_ENGINE_VERSION,
    )

    # Worker process has its own lifecycle — initialize optional checkpointer here,
    # not only in FastAPI lifespan.
    run_startup_sync(settings)

    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down")
        worker.stop()
        # Wait for active run to finish (preStop gives us 240s, use 200s to be safe)
        if worker._current_run_task and not worker._current_run_task.done():
            logger.info("Waiting for active run to complete before exiting...")
            try:
                loop = asyncio.get_event_loop()
                loop.run_until_complete(
                    asyncio.wait_for(worker._current_run_task, timeout=200)
                )
                logger.info("Active run completed, exiting cleanly")
            except asyncio.TimeoutError:
                logger.warning("Active run did not complete within 200s timeout, forcing exit")
        sys.exit(0)

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    try:
        worker.run()
    finally:
        run_shutdown_sync()


if __name__ == "__main__":
    main()

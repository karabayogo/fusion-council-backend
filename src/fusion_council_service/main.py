"""Fusion Council Service — FastAPI main entry point."""

import os
from contextlib import asynccontextmanager
from typing import Optional

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse

# OpenTelemetry imports
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry import trace

from fusion_council_service import metrics as app_metrics
from fusion_council_service.api.routes import init_api, router
from fusion_council_service.clock import utc_now_iso
from fusion_council_service.config import Settings
from fusion_council_service.db import initialize_schema, new_session
from fusion_council_service.logging_utils import setup_logging, get_logger
from fusion_council_service.model_catalog import load_and_validate_catalog
from fusion_council_service.providers.registry import build_provider_registry
from fusion_council_service.startup import run_shutdown, run_startup

# Load .env file before any settings access
load_dotenv()

setup_logging()
logger = get_logger("fusion_council_service")

# OpenTelemetry service name
OTEL_SERVICE_NAME = "fusion-council-api"

# Set up OpenTelemetry tracer provider with service name resource.
# Console exporting is enabled by default for runtime visibility, but tests and
# non-console deployments can set OTEL_TRACES_EXPORTER=none to avoid noisy span dumps.
_tracer_provider = TracerProvider(
    resource=Resource.create({"service.name": OTEL_SERVICE_NAME})
)
if os.environ.get("OTEL_TRACES_EXPORTER", "console").lower() not in {"none", "false", "off"}:
    _tracer_provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
trace.set_tracer_provider(_tracer_provider)

# Global state
_settings: Optional[Settings] = None
_catalog = None
_registry = None


def get_settings() -> Settings:
    if _settings is None:
        raise RuntimeError("Settings not initialized")
    return _settings


def get_catalog():
    return _catalog


def get_registry():
    return _registry


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _settings, _catalog, _registry

    logger.info("Starting fusion-council-service", event_type="app.start")

    # Load settings from environment
    _settings = Settings(
        DATABASE_URL=os.environ.get("DATABASE_URL", ""),
        DATABASE_PATH=os.environ.get("DATABASE_PATH", "./data/fusion_council.db"),
        SERVICE_API_KEYS=os.environ.get("SERVICE_API_KEYS", ""),
        SERVICE_ADMIN_API_KEYS=os.environ.get("SERVICE_ADMIN_API_KEYS", ""),
        MINIMAX_API_KEY=os.environ.get("MINIMAX_API_KEY", ""),
        MINIMAX_TOKEN_PLAN_API_KEY=os.environ.get("MINIMAX_TOKEN_PLAN_API_KEY", ""),
        OLLAMA_API_KEY=os.environ.get("OLLAMA_API_KEY", ""),
        OPENAI_CODEX_API_KEY=os.environ.get("OPENAI_CODEX_API_KEY", ""),
        OPENCODE_GO_API_KEY=os.environ.get("OPENCODE_GO_API_KEY", ""),
        MINIMAX_ANTHROPIC_BASE_URL=os.environ.get("MINIMAX_ANTHROPIC_BASE_URL", "https://api.minimax.io/anthropic"),
        OLLAMA_BASE_URL=os.environ.get("OLLAMA_BASE_URL", "https://ollama.com"),
        OPENAI_CODEX_BASE_URL=os.environ.get("OPENAI_CODEX_BASE_URL", "https://api.openai.com/v1"),
        OPENCODE_GO_BASE_URL=os.environ.get("OPENCODE_GO_BASE_URL", "https://opencode.ai/zen/go/v1"),
        APP_ENV=os.environ.get("APP_ENV", "development"),
        HOST=os.environ.get("HOST", "0.0.0.0"),
        PORT=int(os.environ.get("PORT", "8080")),
        WORKER_POLL_INTERVAL_MS=int(os.environ.get("WORKER_POLL_INTERVAL_MS", "1000")),
        WORKER_HEARTBEAT_INTERVAL_MS=int(os.environ.get("WORKER_HEARTBEAT_INTERVAL_MS", "5000")),
        MAX_PARALLEL_MODEL_CALLS=int(os.environ.get("MAX_PARALLEL_MODEL_CALLS", "3")),
        SSE_POLL_INTERVAL_MS=int(os.environ.get("SSE_POLL_INTERVAL_MS", "500")),
        MODEL_CATALOG_PATH=os.environ.get("MODEL_CATALOG_PATH", "./config/models.yaml"),
        ORCHESTRATOR_ENGINE=os.environ.get("ORCHESTRATOR_ENGINE", "legacy"),
        ORCHESTRATOR_LANGGRAPH_MODES=os.environ.get("ORCHESTRATOR_LANGGRAPH_MODES", ""),
        LANGGRAPH_CHECKPOINT_ENABLED=os.environ.get("LANGGRAPH_CHECKPOINT_ENABLED", "false"),
        LANGGRAPH_CHECKPOINT_DB_URL=os.environ.get("LANGGRAPH_CHECKPOINT_DB_URL", ""),
        LANGGRAPH_THREAD_NAMESPACE=os.environ.get("LANGGRAPH_THREAD_NAMESPACE", "fusion-council"),
        LANGGRAPH_ENGINE_VERSION=os.environ.get("LANGGRAPH_ENGINE_VERSION", "v1"),
    )

    # Validate required settings — accept either DATABASE_URL or DATABASE_PATH
    db_url = getattr(_settings, "DATABASE_URL", "")
    db_path = getattr(_settings, "DATABASE_PATH", "")
    if not db_url and not db_path:
        logger.error("Missing required environment variable: DATABASE_URL or DATABASE_PATH")
        raise RuntimeError("Missing required environment variable: DATABASE_URL or DATABASE_PATH")
    if not getattr(_settings, "SERVICE_API_KEYS", None):
        logger.error("No SERVICE_API_KEYS configured")
        raise RuntimeError("SERVICE_API_KEYS must contain at least one key")

    logger.info(f"API keys loaded: <{len(_settings.service_api_keys)} service keys>, <{len(_settings.service_admin_api_keys)} admin keys>")
    logger.info(
        f"Orchestrator engine: {_settings.ORCHESTRATOR_ENGINE}, checkpoint: {_settings.LANGGRAPH_CHECKPOINT_ENABLED}"
    )

    # Initialize DB
    db = new_session()
    initialize_schema(db)

    # Load and validate model catalog
    _catalog = load_and_validate_catalog(_settings, db)
    logger.info(f"Model catalog loaded: {len(_catalog)} models", event_type="catalog.loaded")

    # W1: reconcile stale provider_health rows against the fresh catalog.
    # Run AFTER load_and_validate_catalog returns (so the catalog is fresh)
    # and BEFORE run_startup (so dispatch sees a clean provider_health table).
    from fusion_council_service.domain.model_selection import reconcile_provider_health_with_catalog
    _reconciled = reconcile_provider_health_with_catalog(db, _catalog)
    if _reconciled:
        logger.info(
            f"reconciled {_reconciled} stale provider_health rows at API startup",
            event_type="provider_health.reconciled_at_startup",
        )

    # Build provider registry
    _registry = build_provider_registry(_settings)
    logger.info("Provider registry built", event_type="providers.ready")

    # Init API routes with DB + provider registry reference
    init_api(_settings, _registry)

    # Optional LangGraph checkpointer bootstrap (non-fatal on failure).
    await run_startup(_settings)

    logger.info(f"Fusion Council Service ready — APP_ENV={_settings.APP_ENV}", event_type="app.ready")

    yield

    await run_shutdown()
    logger.info("Shutting down fusion-council-service", event_type="app.stop")


app = FastAPI(
    title="fusion-council-service",
    version="0.1.0",
    description="Multi-LLM orchestration backend — fusion council engine",
    lifespan=lifespan,
)

# Instrument FastAPI with OpenTelemetry
FastAPIInstrumentor.instrument_app(app, tracer_provider=_tracer_provider)

# CORS for local dev
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)


@app.get("/healthz")
async def healthz():
    return {"ok": True, "timestamp": utc_now_iso()}


@app.get("/readyz")
async def readyz():
    """Readiness probe — checks DB and catalog are initialized."""
    if _catalog is None or _registry is None:
        return JSONResponse({"ok": False, "reason": "not_initialized"}, status_code=503)
    return {"ok": True, "catalog_models": len(_catalog), "timestamp": utc_now_iso()}


@app.get("/metrics")
async def metrics():
    """Prometheus text metrics endpoint."""
    if _settings is None:
        raise RuntimeError("Settings not initialized")
    return PlainTextResponse(
        app_metrics.render_prometheus(
            app_env=_settings.APP_ENV,
            catalog_models=len(_catalog) if _catalog else 0,
        ),
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)

"""Fusion Council Service — FastAPI main entry point."""

import logging
import os
import sqlite3
from contextlib import asynccontextmanager
from typing import Optional

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from fusion_council_service.api.routes import init_api, router
from fusion_council_service.clock import utc_now_iso
from fusion_council_service.config import Settings
from fusion_council_service.db import initialize_schema
from fusion_council_service.logging_utils import setup_logging, get_logger
from fusion_council_service.model_catalog import load_and_validate_catalog
from fusion_council_service.providers.registry import build_provider_registry

# Load .env file before any settings access
load_dotenv()

setup_logging()
logger = get_logger("fusion_council_service")

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
        DATABASE_PATH=os.environ.get("DATABASE_PATH", "./data/fusion_council.db"),
        SERVICE_API_KEYS=os.environ.get("SERVICE_API_KEYS", ""),
        SERVICE_ADMIN_API_KEYS=os.environ.get("SERVICE_ADMIN_API_KEYS", ""),
        MINIMAX_TOKEN_PLAN_API_KEY=os.environ.get("MINIMAX_TOKEN_PLAN_API_KEY", ""),
        OLLAMA_API_KEY=os.environ.get("OLLAMA_API_KEY", ""),
        MINIMAX_ANTHROPIC_BASE_URL=os.environ.get("MINIMAX_ANTHROPIC_BASE_URL", "https://api.minimax.io/anthropic"),
        OLLAMA_BASE_URL=os.environ.get("OLLAMA_BASE_URL", "https://ollama.com"),
        APP_ENV=os.environ.get("APP_ENV", "development"),
        HOST=os.environ.get("HOST", "0.0.0.0"),
        PORT=int(os.environ.get("PORT", "8080")),
        WORKER_POLL_INTERVAL_MS=int(os.environ.get("WORKER_POLL_INTERVAL_MS", "1000")),
        WORKER_HEARTBEAT_INTERVAL_MS=int(os.environ.get("WORKER_HEARTBEAT_INTERVAL_MS", "5000")),
        MAX_PARALLEL_MODEL_CALLS=int(os.environ.get("MAX_PARALLEL_MODEL_CALLS", "3")),
        SSE_POLL_INTERVAL_MS=int(os.environ.get("SSE_POLL_INTERVAL_MS", "500")),
        MODEL_CATALOG_PATH=os.environ.get("MODEL_CATALOG_PATH", "./config/models.yaml"),
    )

    # Validate required settings
    required = ["DATABASE_PATH", "SERVICE_API_KEYS", "MINIMAX_TOKEN_PLAN_API_KEY", "OLLAMA_API_KEY"]
    for key in required:
        val = getattr(_settings, key, None)
        if not val:
            logger.error(f"Missing required environment variable: {key}")
            raise RuntimeError(f"Missing required environment variable: {key}")

    if not _settings.service_api_keys:
        logger.error("No SERVICE_API_KEYS configured")
        raise RuntimeError("SERVICE_API_KEYS must contain at least one key")

    logger.info(f"API keys loaded: <{len(_settings.service_api_keys)} service keys>, <{len(_settings.service_admin_api_keys)} admin keys>")

    # Initialize DB
    from fusion_council_service.db import open_db_connection
    db = open_db_connection(_settings.DATABASE_PATH)
    initialize_schema(db)

    # Load and validate model catalog
    _catalog = load_and_validate_catalog(_settings, db)
    logger.info(f"Model catalog loaded: {len(_catalog)} models", event_type="catalog.loaded")

    # Build provider registry
    _registry = build_provider_registry(_settings)
    logger.info("Provider registry built", event_type="providers.ready")

    # Init API routes with DB
    init_api(_settings)

    logger.info(f"Fusion Council Service ready — APP_ENV={_settings.APP_ENV}", event_type="app.ready")

    yield

    logger.info("Shutting down fusion-council-service", event_type="app.stop")


app = FastAPI(
    title="fusion-council-service",
    version="0.1.0",
    description="Multi-LLM orchestration backend — fusion council engine",
    lifespan=lifespan,
)

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
    """Basic metrics endpoint."""
    if _settings is None:
        raise RuntimeError("Settings not initialized")
    return {
        "app_env": _settings.APP_ENV,
        "catalog_models": len(_catalog) if _catalog else 0,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
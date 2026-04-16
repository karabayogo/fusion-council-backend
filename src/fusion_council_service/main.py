"""Fusion Council Service — main entry point."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from fusion_council_service.logging_utils import setup_logging

setup_logging()

logger = logging.getLogger("fusion_council_service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting fusion-council-service")
    yield
    logger.info("Shutting down")


app = FastAPI(title="fusion-council-service", version="0.1.0", lifespan=lifespan)


@app.get("/healthz")
async def healthz():
    return {"ok": True}
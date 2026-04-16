# Fusion Council Service

Standalone multi-LLM orchestration service. Runs multiple frontier models in parallel, synthesizes their answers, verifies for contradictions, and returns a rigorously reviewed result.

## Quick Start

```bash
# 1. Install
pip install -e ".[dev]"

# 2. Copy env template
cp .env.example .env
# Edit .env with real API keys

# 3. Initialize DB
python -m fusion_council_service.scripts.init_db

# 4. Start API (terminal 1)
make run-api

# 5. Start worker (terminal 2)
make run-worker

# 6. Run smoke test
make smoke
```

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `SERVICE_API_KEYS` | Yes | Comma-separated bearer tokens for API access |
| `SERVICE_ADMIN_API_KEYS` | Yes | Comma-separated admin tokens |
| `MINIMAX_TOKEN_PLAN_API_KEY` | Yes | MiniMax Token Plan API key |
| `MINIMAX_ANTHROPIC_BASE_URL` | No | MiniMax Anthropic-compatible endpoint (default: `https://api.minimax.io/anthropic`) |
| `OLLAMA_API_KEY` | Yes | Ollama Pro API key |
| `OLLAMA_BASE_URL` | No | Ollama API host (default: `https://ollama.com`) |
| `DATABASE_PATH` | Yes | SQLite database file path (must be local filesystem) |
| `APP_ENV` | No | Environment name (default: `development`) |
| `HOST` | No | API bind host (default: `0.0.0.0`) |
| `PORT` | No | API bind port (default: `8080`) |

## Curl Examples

```bash
# Create a single-mode run
curl -X POST http://localhost:8080/v1/runs \
  -H "Authorization: Bearer dev-key-1" \
  -H "Content-Type: application/json" \
  -d '{"mode":"single","prompt":"What is 1+1?"}'

# Create a fusion run (synchronous helper)
curl -X POST http://localhost:8080/v1/respond \
  -H "Authorization: Bearer dev-key-1" \
  -H "Content-Type: application/json" \
  -d '{"mode":"fusion","prompt":"Explain tradeoffs of X vs Y","wait_timeout_seconds":60}'
```

## Database

v1 uses **SQLite** with WAL mode on local disk. See [deployment docs](docs/deployment.md) for rationale and PostgreSQL upgrade triggers.

## Architecture

- **API process**: FastAPI + Uvicorn, serves HTTP, writes jobs to SQLite, streams SSE events
- **Worker process**: Polls DB for queued jobs, executes model orchestration, writes results back

Both processes share a local SQLite database via WAL mode.
.PHONY: install run-api run-worker test smoke help lint

help:
	@echo "fusion-council-service Makefile"
	@echo "  install      Install dependencies and dev dependencies"
	@echo "  run-api       Start the API server"
	@echo "  run-worker    Start the background worker"
	@echo "  test          Run all tests"
	@echo "  smoke         Run smoke test"
	@echo "  lint          Run ruff linter"
	@echo "  build         Build Docker image locally"
	@echo "  compose-up    Start API + worker via docker compose"

install:
	pip install -e ".[dev]"

run-api:
	uvicorn fusion_council_service.main:app --host 0.0.0.0 --port 8080 --reload

run-worker:
	python -m fusion_council_service.domain.worker_loop

test:
	pytest tests/ -q

smoke:
	python -m fusion_council_service.scripts.smoke_test

lint:
	ruff check src/ tests/ --select=E,F --ignore=E501

build:
	docker build -t fusion-council-api:test .

compose-up:
	docker compose up --build

compose-down:
	docker compose down
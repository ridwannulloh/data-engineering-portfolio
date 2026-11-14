.PHONY: install up down migrate load transform stats ops taxi taxi-range taxi-demo dashboard dashboard-docker dashboard-docker-down test test-integration lint

MONTH ?= 2024-01

install:
	uv sync

up:
	docker compose up -d

down:
	docker compose down

migrate:
	uv run ch-etl migrate

load:
	uv run ch-etl load-sample --rows 200000

transform:
	uv run ch-etl transform

stats:
	uv run ch-etl stats

ops:
	uv run ch-etl ops

taxi:
	uv run ch-etl load-taxi --month $(MONTH)

taxi-range:
	uv run ch-etl load-taxi-range --start 2024-01 --end current

taxi-demo:
	uv run ch-etl taxi-demo --month $(MONTH) --output reports/taxi-$(MONTH).json

dashboard:
	uv run ch-etl dashboard

dashboard-docker:
	docker compose --profile dashboard up -d --build dashboard

dashboard-docker-down:
	docker compose --profile dashboard stop dashboard

test:
	uv run pytest -q

test-integration:
	CH_ETL_INTEGRATION=1 uv run pytest -q -m integration

lint:
	uv run ruff check . && uv run ruff format --check .

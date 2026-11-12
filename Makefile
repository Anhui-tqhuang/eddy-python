TEST_DB_URL ?= postgres://localhost:5432/eddy_test?sslmode=disable

.PHONY: sync test test-db lint fmt build

sync:
	uv sync

# Unit tests only; the rest skip without a database.
test:
	uv run pytest -m unit

# Everything, against a local Postgres. Needs Go, which creates River's tables
# and plays the Go side of the interop tests.
test-db:
	EDDY_TEST_DATABASE_URL=$(TEST_DB_URL) uv run pytest --cov=eddy_pg --cov-report=term-missing

lint:
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy

fmt:
	uv run ruff check --fix .
	uv run ruff format .

build:
	uv build

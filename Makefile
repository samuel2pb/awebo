.PHONY: help install lint fmt typecheck check test test-fast test-prop test-mem test-int cov clean build hooks

help:
	@echo "awebo dev targets:"
	@echo "  install    sync dev environment via uv"
	@echo "  hooks      install pre-commit hooks (lint + format + mypy)"
	@echo "  lint       ruff check (incl. ANN — every def must be typed)"
	@echo "  fmt        ruff format + autofix"
	@echo "  typecheck  mypy strict on src + tests"
	@echo "  check      lint + format-check + typecheck (CI gate)"
	@echo "  test       full pytest run (excludes property + memory + integration)"
	@echo "  test-prop  Hypothesis property-based tests"
	@echo "  test-mem   pytest-memray allocation tests"
	@echo "  test-int   integration tests (moto-backed)"
	@echo "  cov        pytest with coverage"
	@echo "  build      uv build distributables"

install:
	uv sync --all-extras

lint:
	uv run ruff check src tests

fmt:
	uv run ruff format src tests
	uv run ruff check --fix src tests

typecheck:
	uv run mypy

check: lint typecheck
	uv run ruff format --check src tests

hooks:
	uv run pre-commit install

test:
	uv run pytest -m "not property and not memory and not integration and not slow"

test-fast: test

test-prop:
	uv run pytest -m property

test-mem:
	uv run pytest --memray -m memory

test-int:
	uv run pytest -m integration

cov:
	uv run pytest --cov=awebo --cov-report=term-missing --cov-report=xml

build:
	uv build

clean:
	rm -rf dist build .pytest_cache .ruff_cache .mypy_cache htmlcov coverage.xml
	find . -type d -name __pycache__ -exec rm -rf {} +

# Convenience targets

TOOL := uv run

.PHONY: sync
sync:
	uv sync --all-extras


# Linting targets

.PHONY: fix-format
format:
	$(TOOL) ruff format

.PHONY: fix-lint
lint:
	$(TOOL) ruff check --fix


# Test targets

.PHONY: pytest
pytest:
	$(TOOL) pytest

.PHONY: mypy
mypy:
	$(TOOL) mypy

.PHONY: lint
lint-check:
	$(TOOL) ruff check
	$(TOOL) ruff format --check

.PHONY: test
test: pytest mypy lint


# Build targets (used from CI)

.PHONY: build
build:
	uv build

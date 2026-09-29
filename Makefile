package := grevling


# Convenience targets

.PHONY: sync
sync:
	uv sync


# Linting targets

.PHONY: format
format:
	uv run ruff format

.PHONY: lint
lint:
	uv run ruff check --fix


# Test targets

.PHONY: pytest
pytest:
	uv run pytest

.PHONY: pyrefly
pyrefly:
	uv run pyrefly check

.PHONY: lint-check
lint-check:
	uv run ruff check
	uv run ruff format --check

.PHONY: test
test: pytest pyrefly lint-check


# Build targets (used from CI)

.PHONY: build
build:
	uv build


# Documentation targets

.PHONY: docs-serve
docs-serve: .docs-venv
	rm -rf .cache
	.docs-venv/bin/zensical serve

.PHONY: docs-clean
docs-clean:
	rm -rf .docs-venv .cache site

.docs-venv:
	uv venv .docs-venv
	uv pip install --python .docs-venv/bin/python zensical

.PHONY: lint format test

package := grevling

lint:
	ruff check $(package)
	ruff format --check $(package)

format:
	ruff format $(package)

pytest:
	pytest

mypy:
	mypy $(package)

test: mypy lint pytest

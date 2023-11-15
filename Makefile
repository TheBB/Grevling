.PHONY: lint format pytest myypy test install wheel sdist build

package := grevling

lint:
	poetry run ruff check $(package)
	poetry run ruff format --check $(package)

format:
	poetry run ruff format $(package)

pytest:
	poetry run pytest

mypy:
	poetry run mypy $(package)

test: mypy lint pytest

install:
	poetry install --with matplotlib,plotly,dev

wheel:
	poetry build -f wheel

sdist:
	poetry build -f sdict

build: wheel sdist

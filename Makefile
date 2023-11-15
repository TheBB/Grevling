.PHONY: lint

package := grevling

lint:
	ruff check $(package)
	ruff format --check $(package)

format:
	ruff format $(package)

install:
	uv sync --all-extras

test:
	uv run --all-extras pytest

lint:
	uv run ruff check .

fix:
	uv run ruff check . --fix

format:
	uv run ruff format .

check: format fix

build:
	uv build

clean:
	rm -rf dist

local-install: clean build
	pip install ./dist/obiba_opal-*.tar.gz 

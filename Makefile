install:
	uv sync --all-extras

test:
	uv run --all-extras pytest

build:
	uv build

clean:
	rm -rf dist

local-install:
	pip install ./dist/obiba_opal-*.tar.gz 

venv:
	uv sync --all-extras

test: venv
	uv run pytest tests/ -v -s

build: venv
	uv build

clean:
	rm -rf dist/
	rm -rf .venv/
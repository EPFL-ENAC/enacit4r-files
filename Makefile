venv:
	uv sync --all-extras

test: venv
	uv run pytest tests/ -v

build: venv
	uv build

clean:
	rm -rf dist/
	rm -rf .venv/
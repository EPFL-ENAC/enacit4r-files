venv:
	uv sync --all-extras

test: venv
	uv run pytest tests/

build: venv
	uv build

clean:
	rm -rf dist/
	rm -rf .venv/
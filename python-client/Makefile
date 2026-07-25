.PHONY: install test lint fmt typecheck checkall pre-commit

install:
	uv sync

test:
	uv run pytest -q

lint:
	uv run ruff check .

fmt:
	uv run ruff format .

typecheck:
	uv run pyright

checkall: fmt lint typecheck test

pre-commit:
	uv run pre-commit run --all-files

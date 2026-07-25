.PHONY: install dev build typecheck lint fmt fmt-check test preview

install:
	bun install
	cd ../ts-client && bun install && bun run build

dev:
	bun run dev

build:
	bun run build

typecheck:
	bun run typecheck

lint:
	bun run lint

fmt:
	bun run fmt

fmt-check:
	bun run fmt-check

test:
	bun run test

preview:
	bun run preview

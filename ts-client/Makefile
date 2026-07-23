.PHONY: build test test-integration lint fmt fmt-check typecheck checkall install

install:
	bun install

build:
	bun run build

fmt:
	bun run fmt

fmt-check:
	bun run fmt-check

lint:
	bun run lint

typecheck:
	bun run typecheck

test:
	bun run test

test-integration:
	bun run test:integration

checkall: fmt-check lint typecheck test build

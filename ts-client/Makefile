.PHONY: build test test-integration test-bun lint fmt fmt-check typecheck checkall install

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

# Manual, opt-in gate: the reactive client under the bun runtime (no cookie
# jar) — setToken + connect + one mutate against a real server. Needs
# RTDB_TEST_SERVER_URL + RTDB_TEST_ADMIN_KEY; provisions and deletes its own db.
test-bun:
	bun scripts/bun-ws-smoke.ts

checkall: fmt-check lint typecheck test build

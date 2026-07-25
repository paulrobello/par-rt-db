COMPOSE_DEV = docker compose -f docker-compose.dev.yml
export RTDB_TEST_DATABASE_URL ?= postgres://rtdb:rtdb@127.0.0.1:55434/rtdb
DEPLOY_HOST = root@lenny2.par-com.net
DEPLOY_PATH = /docker/par-rt-db

.PHONY: build test lint fmt fmt-check typecheck checkall dev-db-up dev-db-down \
	pre-commit pre-commit-update ts-client-build ts-client-install dashboard-install \
	python-client-install python-client-test python-client-lint python-client-fmt \
	python-client-typecheck python-client-checkall deploy

# The dashboard's typecheck/build resolve `@par-rt-db/client` from ts-client's
# gitignored `dist/` (workspace link + exports.types). Build it first so the
# gate never fails on a fresh or stale checkout.
ts-client-build:
	cd ts-client && bun run build

build: ts-client-build
	cd server && cargo build
	cd rust-client && cargo build --all-features
	cd dashboard && bun run build

fmt:
	cd server && cargo fmt --all
	cd ts-client && bun run fmt
	cd rust-client && cargo fmt --all
	cd dashboard && bun run fmt
	cd python-client && uv run ruff format .

fmt-check:
	cd server && cargo fmt --all -- --check
	cd ts-client && bun run fmt-check
	cd rust-client && cargo fmt --all -- --check
	cd dashboard && bun run fmt-check
	cd python-client && uv run ruff format --check .

lint:
	cd server && cargo clippy --all-targets --all-features -- -D warnings
	cd ts-client && bun run lint
	cd rust-client && cargo clippy --all-targets --all-features -- -D warnings
	cd dashboard && bun run lint
	cd python-client && uv run ruff check .

typecheck: ts-client-build
	cd server && cargo check --all-targets
	cd ts-client && bun run typecheck
	cd rust-client && cargo check --all-targets --all-features
	cd dashboard && bun run typecheck
	cd python-client && uv run pyright

dev-db-up:
	$(COMPOSE_DEV) up -d --wait

dev-db-down:
	$(COMPOSE_DEV) down

test: dev-db-up
	cd server && cargo test
	cd ts-client && bun run test
	cd rust-client && cargo test --all-features
	cd dashboard && bun run test
	cd python-client && uv run pytest -q

ts-client-install:
	cd ts-client && bun install

dashboard-install:
	bun install
	cd dashboard && bun install

python-client-install:
	cd python-client && uv sync --extra dev

python-client-test:
	cd python-client && uv run pytest -q

python-client-lint:
	cd python-client && uv run ruff check .

python-client-fmt:
	cd python-client && uv run ruff format .

python-client-typecheck:
	cd python-client && uv run pyright

python-client-checkall: python-client-fmt python-client-lint python-client-typecheck python-client-test

checkall: fmt-check lint typecheck test

pre-commit:
	pre-commit run --all-files

pre-commit-update:
	pre-commit autoupdate

deploy: checkall
	rsync -az --delete --filter=':- .gitignore' --exclude .git/ \
		./ $(DEPLOY_HOST):$(DEPLOY_PATH)/
	ssh $(DEPLOY_HOST) 'cd $(DEPLOY_PATH) && docker compose up -d --build && docker compose ps'
	ssh $(DEPLOY_HOST) 'curl -fsS http://127.0.0.1:8300/healthz'
	@echo
	curl -fsS https://rtdb.pardev.net/healthz
	@echo

COMPOSE_DEV = docker compose -f docker-compose.dev.yml
export RTDB_TEST_DATABASE_URL ?= postgres://rtdb:rtdb@127.0.0.1:55434/rtdb
DEPLOY_HOST = root@lenny2.par-com.net
DEPLOY_PATH = /docker/par-rt-db

.PHONY: build test lint fmt fmt-check typecheck checkall dev-db-up dev-db-down \
	pre-commit pre-commit-update ts-client-install dashboard-install deploy

build:
	cd server && cargo build
	cd ts-client && bun run build
	cd rust-client && cargo build --all-features
	cd dashboard && bun run build

fmt:
	cd server && cargo fmt --all
	cd ts-client && bun run fmt
	cd rust-client && cargo fmt --all
	cd dashboard && bun run fmt

fmt-check:
	cd server && cargo fmt --all -- --check
	cd ts-client && bun run fmt-check
	cd rust-client && cargo fmt --all -- --check
	cd dashboard && bun run fmt-check

lint:
	cd server && cargo clippy --all-targets --all-features -- -D warnings
	cd ts-client && bun run lint
	cd rust-client && cargo clippy --all-targets --all-features -- -D warnings
	cd dashboard && bun run lint

typecheck:
	cd server && cargo check --all-targets
	cd ts-client && bun run typecheck
	cd rust-client && cargo check --all-targets --all-features
	cd dashboard && bun run typecheck

dev-db-up:
	$(COMPOSE_DEV) up -d --wait

dev-db-down:
	$(COMPOSE_DEV) down

test: dev-db-up
	cd server && cargo test
	cd ts-client && bun run test
	cd rust-client && cargo test --all-features
	cd dashboard && bun run test

ts-client-install:
	cd ts-client && bun install

dashboard-install:
	bun install
	cd dashboard && bun install

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

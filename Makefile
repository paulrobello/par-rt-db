COMPOSE_DEV = docker compose -f docker-compose.dev.yml
export RTDB_TEST_DATABASE_URL ?= postgres://rtdb:rtdb@127.0.0.1:55434/rtdb

.PHONY: build test lint fmt fmt-check typecheck checkall dev-db-up dev-db-down pre-commit pre-commit-update

build:
	cd server && cargo build

fmt:
	cd server && cargo fmt --all

fmt-check:
	cd server && cargo fmt --all -- --check

lint:
	cd server && cargo clippy --all-targets --all-features -- -D warnings

typecheck:
	cd server && cargo check --all-targets

dev-db-up:
	$(COMPOSE_DEV) up -d --wait

dev-db-down:
	$(COMPOSE_DEV) down

test: dev-db-up
	cd server && cargo test

checkall: fmt-check lint typecheck test

pre-commit:
	pre-commit run --all-files

pre-commit-update:
	pre-commit autoupdate

COMPOSE_DEV = docker compose -f docker-compose.dev.yml
export RTDB_TEST_DATABASE_URL ?= postgres://rtdb:rtdb@127.0.0.1:55434/rtdb
DEPLOY_HOST = root@lenny2.par-com.net
DEPLOY_PATH = /docker/par-rt-db

.PHONY: build test lint fmt fmt-check typecheck checkall dev-db-up dev-db-down \
	pre-commit pre-commit-update client-install deploy

build:
	cd server && cargo build
	cd client && bun run build

fmt:
	cd server && cargo fmt --all
	cd client && bun run fmt

fmt-check:
	cd server && cargo fmt --all -- --check
	cd client && bun run fmt-check

lint:
	cd server && cargo clippy --all-targets --all-features -- -D warnings
	cd client && bun run lint

typecheck:
	cd server && cargo check --all-targets
	cd client && bun run typecheck

dev-db-up:
	$(COMPOSE_DEV) up -d --wait

dev-db-down:
	$(COMPOSE_DEV) down

test: dev-db-up
	cd server && cargo test
	cd client && bun run test

client-install:
	cd client && bun install

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

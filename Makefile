COMPOSE_DEV = docker compose -f docker-compose.dev.yml
export RTDB_TEST_DATABASE_URL ?= postgres://rtdb:rtdb@127.0.0.1:55434/rtdb
DEPLOY_HOST ?= root@docker-host.example.com
DEPLOY_PATH = /docker/par-rt-db
# Short sha of the working-tree HEAD, baked into /healthz on deploy. Passed as
# a shell env on the remote `docker compose up` (shell env overrides .env) so
# the build arg — and thus the deployed binary's git_commit label — always tracks
# the commit being deployed, without touching docker-host's .env.
DEPLOY_COMMIT := $(shell git rev-parse --short HEAD)

# swift-client needs the Swift 6 toolchain; this repo's gate only carries one
# on Darwin (the ubuntu CI image has no Swift). Every swift line in the
# aggregate sweeps is guarded by this test: it runs on the Mac (and the
# macos-latest CI job), and echoes a loud skip on Linux so the ubuntu gate
# stays green without silently dropping the package.
SWIFT_OS := $(shell uname -s)
SWIFT_SKIP := @echo "Skipping swift-client (non-Darwin host)"
SWIFT_IF_DARWIN = $(if $(filter Darwin,$(SWIFT_OS)),cd swift-client && $(1),$(SWIFT_SKIP))

.PHONY: build test lint fmt fmt-check typecheck checkall dev-db-up dev-db-down dev-db-clean \
	pre-commit pre-commit-update ts-client-build ts-client-install dashboard-install \
	dashboard-test \
	python-client-install python-client-test python-client-lint python-client-fmt \
	python-client-typecheck python-client-checkall rust-client-check-features rtdb-cli deploy \
	env-drift-check dockerfile-stub-check cli-docs cli-docs-check \
	swift-client-build swift-client-test swift-client-lint swift-client-fmt \
	swift-client-fmt-check swift-client-typecheck swift-client-checkall

# The dashboard's typecheck/build resolve `@par-rt-db/client` from ts-client's
# gitignored `dist/` (workspace link + exports.types). Build it first so the
# gate never fails on a fresh or stale checkout.
ts-client-build:
	cd ts-client && bun run build

build: ts-client-build
	cd core && cargo build
	cd server && cargo build
	cd rust-client && cargo build --all-features
	cd cli && cargo build --all-features
	cd dashboard && bun run build
	$(call SWIFT_IF_DARWIN,swift build)

fmt:
	cd server && cargo fmt --all
	cd ts-client && bun run fmt
	cd rust-client && cargo fmt --all
	cd cli && cargo fmt --all
	cd dashboard && bun run fmt
	cd python-client && uv run ruff format .
	$(call SWIFT_IF_DARWIN,swiftformat .)

fmt-check:
	cd server && cargo fmt --all -- --check
	cd ts-client && bun run fmt-check
	cd rust-client && cargo fmt --all -- --check
	cd cli && cargo fmt --all -- --check
	cd dashboard && bun run fmt-check
	cd python-client && uv run ruff format --check .
	$(call SWIFT_IF_DARWIN,swiftformat --lint .)

lint:
	cd core && cargo clippy --all-targets --all-features -- -D warnings
	cd server && cargo clippy --all-targets --all-features -- -D warnings
	cd ts-client && bun run lint
	cd rust-client && cargo clippy --all-targets --all-features -- -D warnings
	cd cli && cargo clippy --all-targets --all-features -- -D warnings
	cd dashboard && bun run lint
	cd python-client && uv run ruff check .
	$(call SWIFT_IF_DARWIN,swiftlint --strict)

typecheck: ts-client-build
	cd core && cargo check --all-targets --all-features
	cd server && cargo check --all-targets
	cd ts-client && bun run typecheck
	cd rust-client && cargo check --all-targets --all-features
	cd cli && cargo check --all-targets --all-features
	cd dashboard && bun run typecheck
	cd python-client && uv run pyright
	$(call SWIFT_IF_DARWIN,swift build)

dev-db-up:
	$(COMPOSE_DEV) up -d --wait

dev-db-down:
	$(COMPOSE_DEV) down

# Drop leaked test schemas (db_t<uuid-v7>) from the dev rtdb DB. Tests create a
# database per test and don't drop it, so the dev DB bloats over time; run this
# periodically. Requires psql on PATH and the dev Postgres up (make dev-db-up).
dev-db-clean:
	psql "$(RTDB_TEST_DATABASE_URL)" -f scripts/dev-db-clean.sql

test: dev-db-up
	cd core && cargo test
	cd server && cargo test
	cd ts-client && bun run test
	cd rust-client && cargo test --all-features
	cd cli && cargo test --all-features
	cd dashboard && bun run test
	cd python-client && uv run pytest -q
	$(call SWIFT_IF_DARWIN,swift test)

ts-client-install:
	cd ts-client && bun install

dashboard-install:
	bun install
	cd dashboard && bun install

dashboard-test:
	cd dashboard && bun run test

python-client-install:
	# `--all-extras` installs the optional `http` (httpx) and `ws` (websockets)
	# dependencies alongside the default `dev` group, so pyright can resolve the
	# imports those surfaces use during `make python-client-typecheck`.
	cd python-client && uv sync --all-extras

python-client-test:
	cd python-client && uv run pytest -q

python-client-lint:
	cd python-client && uv run ruff check .

python-client-fmt:
	cd python-client && uv run ruff format .

python-client-typecheck:
	cd python-client && uv run pyright

python-client-checkall: python-client-fmt python-client-lint python-client-typecheck python-client-test

# Darwin-guarded (see SWIFT_IF_DARWIN at the top): `swift build` doubles as
# typecheck — the Swift compiler has no separate check-only surface in SPM.
swift-client-build:
	$(call SWIFT_IF_DARWIN,swift build)

swift-client-test:
	$(call SWIFT_IF_DARWIN,swift test)

swift-client-lint:
	$(call SWIFT_IF_DARWIN,swiftlint --strict)

swift-client-fmt:
	$(call SWIFT_IF_DARWIN,swiftformat .)

# Check-only twin of swift-client-fmt: the gate fails on unformatted Swift
# instead of silently applying the format. Must run before the applying fmt in
# swift-client-checkall — a check after the apply could never fail — matching
# the root checkall, which carries fmt-check with no apply step at all.
swift-client-fmt-check:
	$(call SWIFT_IF_DARWIN,swiftformat --lint .)

swift-client-typecheck:
	$(call SWIFT_IF_DARWIN,swift build)

swift-client-checkall: swift-client-fmt-check swift-client-fmt swift-client-lint swift-client-typecheck swift-client-test

# ARC-110: verify the rust-client library AND its test targets compile under
# every meaningful feature combination, not only --all-features. The [[test]]
# required-features in rust-client/Cargo.toml gate the test binaries; this loop
# catches a regression where a test reintroduces an ungated feature import.
# Uses --manifest-path so each iteration is independent of shell cwd.
rust-client-check-features:
	@set -e; \
	for feats in "" "http" "ws" "admin" "in_memory" "http,ws" "http,in_memory" "http,ws,admin,in_memory"; do \
		if [ -z "$$feats" ]; then \
			echo "=== rust-client: cargo check --all-targets (no features) ==="; \
			cargo check --manifest-path rust-client/Cargo.toml --all-targets --no-default-features; \
		else \
			echo "=== rust-client: cargo check --all-targets --features '$$feats' ==="; \
			cargo check --manifest-path rust-client/Cargo.toml --all-targets --no-default-features --features "$$feats"; \
		fi; \
	done
	@echo "=== rust-client: cargo doc --all-features (deny warnings) ==="
	RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features --manifest-path rust-client/Cargo.toml

rtdb-cli:
	cd cli && cargo build --release

# ENH-025: regenerate the cli/README.md command reference (the
# cli-reference:begin/end marker region) from the CLI's own clap definitions.
cli-docs:
	cd cli && cargo run --quiet --bin gen-cli-docs -- README.md

# Gate half: regenerate a copy of the README and diff it against the committed
# one — any difference means the documented reference is stale.
cli-docs-check:
	@tmpdir=$$(mktemp -d); \
	cp cli/README.md "$$tmpdir/README.md"; \
	if (cd cli && cargo run --quiet --bin gen-cli-docs -- "$$tmpdir/README.md") \
		&& diff -u cli/README.md "$$tmpdir/README.md"; then \
		rm -rf "$$tmpdir"; \
	else \
		status=$$?; rm -rf "$$tmpdir"; \
		echo "cli/README.md command reference is stale — run 'make cli-docs' and commit the result" >&2; \
		exit $$status; \
	fi

# Fails when a documented RTDB_* var isn't forwarded to the container by
# docker-compose.yml (compose's `environment:` block is an explicit allowlist,
# so a .env-only key silently does nothing).
env-drift-check:
	./scripts/env-drift-check.sh

# ARC-011: a `[[test]]` declared in a non-server workspace member must have a
# stub in the Dockerfile's dependency layer, or `make deploy` fails at cargo's
# manifest parse — a break `checkall` could not otherwise see.
dockerfile-stub-check:
	./scripts/dockerfile-stub-check.sh

checkall: env-drift-check dockerfile-stub-check cli-docs-check fmt-check lint typecheck test rust-client-check-features

pre-commit:
	pre-commit run --all-files

pre-commit-update:
	pre-commit autoupdate

deploy: checkall
	rsync -az --delete --filter=':- .gitignore' --exclude .git/ \
		./ $(DEPLOY_HOST):$(DEPLOY_PATH)/
	ssh $(DEPLOY_HOST) 'cd $(DEPLOY_PATH) && RTDB_BUILD_COMMIT=$(DEPLOY_COMMIT) docker compose up -d --build && docker compose ps'
	ssh $(DEPLOY_HOST) 'curl -fsS http://127.0.0.1:8300/healthz'
	@echo
	curl -fsS https://rtdb.example.com/healthz
	@echo

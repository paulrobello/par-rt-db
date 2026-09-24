COMPOSE_DEV = docker compose -f docker-compose.dev.yml
export RTDB_TEST_DATABASE_URL ?= postgres://rtdb:rtdb@127.0.0.1:55434/rtdb
DEPLOY_HOST ?= root@docker-host.example.com
DEPLOY_PATH = /docker/par-rt-db
# Short sha of the working-tree HEAD, baked into /healthz on deploy. Passed as
# a shell env on the remote `docker compose up` (shell env overrides .env) so
# the build arg — and thus the deployed binary's git_commit label — always tracks
# the commit being deployed, without touching docker-host's .env.
DEPLOY_COMMIT := $(shell git rev-parse --short HEAD)
# Gate cache key for checkall-cached. A clean tree keys on its committed
# content hash (exact reuse); a DIRTY tree gets a unique-per-invocation key so
# uncommitted working-tree content can never reuse — or poison — a cached
# verdict (deploys normally ship committed trees; gating a dirty tree always
# runs the full gate).
GATE_KEY := $(shell key=$$(git rev-parse 'HEAD^{tree}' 2>/dev/null); test -z "$$(git status --porcelain 2>/dev/null)" || key="$$key-dirty-$$(date +%s)"; echo "$$key")
GATE_STAMP = target/.gate-tree

# swift-client needs the Swift 6 toolchain; this repo's gate only carries one
# on Darwin (the ubuntu CI image has no Swift). Every swift line in the
# aggregate sweeps is guarded by this test: it runs on the Mac (and the
# macos-latest CI job), and echoes a loud skip on Linux so the ubuntu gate
# stays green without silently dropping the package.
SWIFT_OS := $(shell uname -s)
SWIFT_SKIP := @echo "Skipping swift-client (non-Darwin host)"
SWIFT_IF_DARWIN = $(if $(filter Darwin,$(SWIFT_OS)),cd swift-client && $(1),$(SWIFT_SKIP))

.PHONY: clean build test lint fmt fmt-check typecheck checkall checkall-cached dev-db-up dev-db-down dev-db-clean \
	pre-commit pre-commit-update ts-client-build ts-client-install dashboard-install \
	dashboard-test \
	python-client-install python-client-test python-client-lint python-client-fmt \
	python-client-typecheck python-client-checkall rust-client-check-features rtdb-cli deploy \
	env-drift-check dockerfile-stub-check backup-persistence-check cli-docs cli-docs-check \
	rust-client-doc ts-client-doc python-client-doc swift-client-doc docs-api \
	go-client-install go-client-fmt go-client-fmt-check go-client-lint go-client-test \
	go-client-typecheck go-client-checkall \
	swift-client-build swift-client-test swift-client-lint swift-client-fmt \
	swift-client-fmt-check swift-client-typecheck swift-client-checkall \
	bench-micro bench bench-baseline \
	grind-start grind-start-anthropic grind-start-zai grind-start-grok grind-start-codex \
	grind-start-omp grind-stop grind-clean-logs


# The dashboard's typecheck/build resolve `@par-rt-db/client` from ts-client's
# gitignored `dist/` (workspace link + exports.types). Build it first so the
# gate never fails on a fresh or stale checkout.
ts-client-build:
	cd ts-client && bun run build

# Remove generated build output while preserving installed dependencies.
clean:
	cargo clean
	rm -rf ts-client/dist dashboard/dist dashboard/node_modules/.cache swift-client/.build
build: ts-client-build go-client-install
	cd core && cargo build
	cd server && cargo build
	cd rust-client && cargo build --all-features
	cd cli && cargo build --all-features
	cd dashboard && bun run build
	$(call SWIFT_IF_DARWIN,swift build)

fmt:
	cargo fmt --all
	cd ts-client && bun run fmt
	cd dashboard && bun run fmt
	cd python-client && uv run ruff format .
	cd go-client && gofmt -w .
	$(call SWIFT_IF_DARWIN,swiftformat .)

fmt-check:
	cargo fmt --all -- --check
	cd ts-client && bun run fmt-check
	cd dashboard && bun run fmt-check
	cd python-client && uv run ruff format --check .
	cd go-client && test -z "$$(gofmt -l .)" || { echo 'gofmt needed:'; gofmt -l .; exit 1; }
	$(call SWIFT_IF_DARWIN,swiftformat --lint .)

# ARC-014: one workspace-level clippy invocation instead of four per-crate
# `cd X && cargo clippy` invocations. --all-features was already applied to
# every crate individually (core/server/rust-client/cli), so this is a
# behavior-preserving consolidation, not a new feature combination.
lint:
	cargo clippy --workspace --all-targets --all-features -- -D warnings
	cd ts-client && bun run lint
	cd dashboard && bun run lint
	cd python-client && uv run ruff check .
	cd go-client && go vet ./...
	cd go-client && go vet -tags live ./...
	$(call SWIFT_IF_DARWIN,swiftlint --strict)

# ARC-014: workspace-level typecheck. The former `cargo check --workspace
# --all-targets --all-features` here was pure duplication: `make lint` runs
# clippy over the identical scope, and clippy is check + lints, so it already
# fails on everything check would fail on. What remains is the non-Rust
# typecheckers plus the Swift build, which clippy does not cover.
typecheck: ts-client-build
	cd ts-client && bun run typecheck
	cd dashboard && bun run typecheck
	cd python-client && uv run pyright
	cd go-client && go vet ./...
	$(call SWIFT_IF_DARWIN,swift build)

dev-db-up:
	$(COMPOSE_DEV) up -d --wait
	@leaked=$$(psql "$(RTDB_TEST_DATABASE_URL)" -t -A -c \
		"SELECT count(*) FROM rtdb_auth.databases WHERE name ~ '^t[0-9a-f]{32}$$'"); \
	if [ "$$leaked" -gt 200 ]; then \
		echo "dev-db: $$leaked leaked test databases (> 200) — registry walks stall merge_test; running scoped clean"; \
		psql "$(RTDB_TEST_DATABASE_URL)" -f scripts/dev-db-clean.sql; \
	fi

dev-db-down:
	$(COMPOSE_DEV) down

# Drop leaked test schemas (db_t<uuid-v7>) from the dev rtdb DB. Tests create a
# database per test and don't drop it, so the dev DB bloats over time; run this
# periodically. Requires psql on PATH and the dev Postgres up (make dev-db-up).
dev-db-clean:
	psql "$(RTDB_TEST_DATABASE_URL)" -f scripts/dev-db-clean.sql

# ARC-014: one workspace-level `cargo test` instead of four per-crate
# invocations. --all-features is REQUIRED, not optional: rust-client declares
# six [[test]] targets behind `required-features` (golden_vector,
# query_combinations, semantics_corpus, hot_config_test, ws_integration,
# http_integration). Without the flag cargo silently SKIPS those targets, which
# would disable the wire-corpus parity enforcement that every client mirror
# depends on. The flag also enables the server's `otel` feature, which only
# compiles the OTLP layer — RTDB_OTEL_ENABLED still gates it at runtime, so a
# feature-compiled test binary makes zero OTLP calls.
# nextest runs each test in its own process with per-test retry quotas, so a
# known-flaky test doesn't force a full gate rerun (retries configured in
# .config/nextest.toml). Falls back to `cargo test` when the binary is absent
# (CI installs it; see ci.yml). Doctests are skipped by nextest, but the
# workspace's doc tests are all `ignore`-marked (7 ignored, 0 run under
# `cargo test --doc`), so nothing is lost.
NEXTEST := $(shell command -v cargo-nextest >/dev/null 2>&1 && echo yes)
test: dev-db-up
ifeq ($(NEXTEST),yes)
	cargo nextest run --workspace --all-features
else
	cargo test --workspace --all-features
endif
	cd ts-client && bun run test
	cd dashboard && bun run test
	cd python-client && uv run pytest -q
	cd go-client && go test ./...
	$(call SWIFT_IF_DARWIN,swift test)

go-client-install:
	cd go-client && go mod download

ts-client-install:
	cd ts-client && bun install
	cd ts-client/docs-toolchain && bun install --frozen-lockfile
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

# gofmt is Go's formatter; `go vet` doubles as the typecheck analog (it
# type-checks every package). The dep guard re-asserts the stdlib-only rule:
# coder/websocket may only appear in wsclient's dependency tree.
go-client-fmt:
	cd go-client && gofmt -w .

go-client-fmt-check:
	cd go-client && test -z "$$(gofmt -l .)" || { echo 'gofmt needed:'; gofmt -l .; exit 1; }

go-client-lint:
	cd go-client && go vet ./...
	cd go-client && go vet -tags live ./...
	cd go-client && go list -deps . ./wire ./dsl ./errors ./httpclient ./inmemory | grep -q coder/websocket && { echo 'stdlib-only package transitively imports coder/websocket'; exit 1; } || true

go-client-typecheck: go-client-install
	cd go-client && go vet ./...

go-client-test:
	cd go-client && go test ./...

go-client-checkall: go-client-fmt-check go-client-lint go-client-typecheck go-client-test

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


rust-client-doc:
	RUSTDOCFLAGS="-D warnings -D rustdoc::broken_intra_doc_links" cargo doc --no-deps --all-features --manifest-path rust-client/Cargo.toml

ts-client-doc:
	cd ts-client && bun run doc

python-client-doc:
	cd python-client && uv run --all-extras pdoc par_rt_db -o docs-api --docformat google

swift-client-doc:
	$(call SWIFT_IF_DARWIN,mkdir -p docs-api && swift package --allow-writing-to-directory docs-api generate-documentation --target ParRtDbClient --output-path docs-api/ParRtDbClient.doccarchive --transform-for-static-hosting --hosting-base-path par-rt-db/swift --warnings-as-errors)

docs-api: rust-client-doc ts-client-doc python-client-doc swift-client-doc

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

backup-persistence-check:
	./scripts/backup-persistence-check.sh

checkall: env-drift-check dockerfile-stub-check backup-persistence-check cli-docs-check docs-api fmt-check lint typecheck test rust-client-check-features
	@mkdir -p target && echo "$(GATE_KEY)" > $(GATE_STAMP)

# Content-addressed gate reuse for deploys: `checkall` stamps the gate key
# (committed tree hash, clean trees only) into target/.gate-tree on every green
# run, and `deploy` reuses that verdict when the key matches. A cached gate
# cannot go stale-wrong: any edit to any tracked file changes the tree hash or
# makes the tree dirty (unique key, never cached) and forces a full re-gate.
# `make checkall` always runs the full gate and refreshes the stamp.
checkall-cached:
	@if [ -f "$(GATE_STAMP)" ] && [ "$$(cat "$(GATE_STAMP)")" = "$(GATE_KEY)" ]; then \
		echo "gate: cached green for $(GATE_KEY) — run 'make checkall' to force"; \
	else \
		$(MAKE) checkall; \
	fi

# ENH-033: criterion micro-benchmarks over the pure hot paths (server) and the
# in-memory engine (rust-client). No Postgres, no server process. Deliberately
# NOT part of `checkall` — too slow for the PR gate; `--all-targets` in
# `typecheck`/`lint` already keeps these compiling. HTML reports land under
# target/criterion/*/report/index.html.
bench-micro:
	cargo bench --manifest-path server/Cargo.toml
	cargo bench --manifest-path rust-client/Cargo.toml --features in_memory

# ENH-033: black-box load benchmark. Starts real rtdb-server process(es)
# against the dev Postgres, drives them with scripts/bench/load.ts, then
# unconditionally tears the server(s) down — the `trap ... EXIT` fires on
# success, failure, or the `timeout` below killing the load script, so a
# crashed run never leaves an rtdb-server process behind (verify with
# `pgrep -f rtdb-server`).
#
# Two instances (RTDB_MULTI_INSTANCE=true, same RTDB_DATABASE_URL) so
# scenario (c) can measure forward round-trip latency. Which of the two wins
# the ownership advisory lock is a race (committer/lease.rs) — this target
# does not track it, so it deliberately omits `--owner-pid` and scenario (c)
# reports forward-latency only, not takeover time (see load.ts --help and
# CONTRIBUTING.md's Benchmarks section).
bench: dev-db-up ts-client-build
	@set -e; \
	export RTDB_DATABASE_URL='postgres://rtdb:rtdb@127.0.0.1:55434/rtdb'; \
	export RTDB_ADMIN_KEY="$$(openssl rand -hex 32)"; \
	export RTDB_PUBLIC_URL='http://localhost:8300'; \
	SERVER1_PID=""; SERVER2_PID=""; \
	cleanup() { \
		[ -n "$$SERVER1_PID" ] && kill "$$SERVER1_PID" 2>/dev/null; \
		[ -n "$$SERVER2_PID" ] && kill "$$SERVER2_PID" 2>/dev/null; \
		wait "$$SERVER1_PID" "$$SERVER2_PID" 2>/dev/null; \
		true; \
	}; \
	trap cleanup EXIT; \
	echo "=== bench: building rtdb-server (release) ==="; \
	cargo build --release --manifest-path server/Cargo.toml --bin rtdb-server; \
	echo "=== bench: starting server on :8300 (owner or shadow) ==="; \
	RTDB_PORT=8300 RTDB_MULTI_INSTANCE=true ./target/release/rtdb-server & \
	SERVER1_PID=$$!; \
	echo "=== bench: starting server on :8301 (owner or shadow) ==="; \
	RTDB_PORT=8301 RTDB_MULTI_INSTANCE=true ./target/release/rtdb-server & \
	SERVER2_PID=$$!; \
	for port in 8300 8301; do \
		echo "=== bench: waiting for :$$port/healthz ==="; \
		ok=0; \
		for i in $$(seq 1 30); do \
			if curl -fsS "http://127.0.0.1:$$port/healthz" >/dev/null 2>&1; then ok=1; break; fi; \
			sleep 1; \
		done; \
		[ "$$ok" = 1 ] || { echo "server on :$$port never became healthy" >&2; exit 1; }; \
	done; \
	echo "=== bench: running load scenarios (5 min deadline) ==="; \
	timeout 300 bun run scripts/bench/load.ts --admin-key "$$RTDB_ADMIN_KEY"

# ENH-033: human-run only — deliberately overwrites the committed
# bench/baseline.json. Never invoked by CI or checkall.
bench-baseline: bench
	@sha=$$(git rev-parse --short HEAD); \
	result="bench/results/$$sha.json"; \
	[ -f "$$result" ] || { echo "bench-baseline: expected $$result, not found" >&2; exit 1; }; \
	cp "$$result" bench/baseline.json; \
	echo "bench-baseline: wrote bench/baseline.json from $$result"

pre-commit:
	pre-commit run --all-files

pre-commit-update:
	pre-commit autoupdate

deploy: checkall-cached
	rsync -az --delete --filter=':- .gitignore' --exclude .git/ \
		./ $(DEPLOY_HOST):$(DEPLOY_PATH)/
	ssh $(DEPLOY_HOST) 'cd $(DEPLOY_PATH) && BUILDER=par-rt-db-builder && if ! docker buildx inspect "$$BUILDER" >/dev/null 2>&1; then docker buildx create --name "$$BUILDER" --driver docker-container --driver-opt default-load=true --driver-opt cpu-quota=400000 --driver-opt cpu-period=100000 --buildkitd-config "$(DEPLOY_PATH)/deploy/buildkitd.toml"; fi && docker buildx use "$$BUILDER" && RTDB_BUILD_COMMIT=$(DEPLOY_COMMIT) BUILDX_BUILDER="$$BUILDER" docker compose up -d --build && docker compose ps'
	ssh $(DEPLOY_HOST) 'curl -fsS http://127.0.0.1:8300/healthz'
	@echo
	curl -fsS https://rtdb.pardev.net/healthz
	@echo

# ==== Grind loop (~/Repos/par-grind) ========================================
# The full grind target set, pasted from par-grind's examples/Makefile.grind.
# Targets are project-agnostic — per-project knobs (prompt, model, clean
# command, post-iter check) live in the project's .grind.local.json; every
# GRIND_* env var overrides a config key. See the par-grind README's config
# table. Pasted rather than included so `make help` output stays clean
# (include + a grep-based help target renders file-prefixed lines).
#
# Invoked via `bash` on purpose: `ps` then shows `bash …/grind.sh <repo>`,
# the command shape grind's overseer recipes and probes match on. A
# shebang-direct invocation puts the script at a different ps field and reads
# as grind GONE to them.
GRIND_SH := $(HOME)/Repos/par-grind/grind.sh

# grind-start runs the z.ai backend at effort high (2026-09-12);
# grind-start-zai keeps the zai arm's default effort (xhigh).
grind-start: ## Run the grind loop against this repo (foreground; z.ai backend, effort high)
	GRIND_BACKEND=zai GRIND_EFFORT=high bash $(GRIND_SH) "$(CURDIR)"

grind-start-anthropic: ## Run the grind loop against the Anthropic backend (foreground; model opus[1m], effort high)
	GRIND_BACKEND=anthropic GRIND_MODEL=opus[1m] GRIND_EFFORT=high bash $(GRIND_SH) "$(CURDIR)"

grind-start-zai: ## Run the grind loop against the z.ai backend (foreground; needs ZAI_API_KEY)
	GRIND_BACKEND=zai bash $(GRIND_SH) "$(CURDIR)"

grind-start-grok: ## Run the grind loop against the Grok backend (foreground; needs grok CLI login)
	GRIND_BACKEND=grok bash $(GRIND_SH) "$(CURDIR)"

grind-start-codex: ## Run the grind loop against the Codex backend (foreground; needs codex CLI login)
	GRIND_BACKEND=codex bash $(GRIND_SH) "$(CURDIR)"

# The omp arm is the only one with NO default model: OMP v18 applies
# --provider during explicit model resolution, so grind.sh derives omp's
# --provider from the exact model id (glm-5.3/glm-5.3-flash -> zai,
# gpt-5.6-terra/gpt-5.6-luna -> openai-codex, grok-4.6 -> xai-oauth).
# Refuse to launch without one rather than guess a provider.
grind-start-omp: ## Run the grind loop against the OMP backend (foreground; GRIND_MODEL required — provider derived from the exact model id)
	@test -n "$(GRIND_MODEL)" || { echo "grind-start-omp: GRIND_MODEL is required, e.g. GRIND_MODEL=glm-5.3 — the omp arm derives omp's --provider from the exact model id (glm-5.3/glm-5.3-flash->zai, gpt-5.6-terra/gpt-5.6-luna->openai-codex, grok-4.6->xai-oauth)" >&2; exit 2; }
	GRIND_BACKEND=omp GRIND_MODEL="$(GRIND_MODEL)" bash $(GRIND_SH) "$(CURDIR)"

grind-stop: ## Ask a running grind loop to stop after the current iteration
	@touch .exit-grind
	@echo "wrote .exit-grind — the loop stops after the current iteration finishes"

# .exit-grind is checked only at the top of each iteration: a stop takes
# effect once the in-flight session ends, and grind removes the file itself
# on exit — a grind-stop with nothing running just clears a stale sentinel.
# grind-start runs in the FOREGROUND (it owns the terminal); grind-stop runs
# from a second shell.

# 2*/ matches only the date-named run dirs — the latest symlink, the run.jsonl
# ledger, and stdout logs live at the root and never match. The newest 2 by
# mtime are kept (the active run is always newest, and the latest symlink's
# own target is excluded from removal as a belt-and-braces guard).
grind-clean-logs: ## Delete all but the newest 2 grind run dirs (latest symlink, run.jsonl kept)
	@latest=$$(readlink .grind-logs/latest 2>/dev/null | xargs basename 2>/dev/null); \
	old=$$(ls -dt .grind-logs/2*/ 2>/dev/null | tail -n +3 | sed 's:/$$::'); \
	[ -n "$$old" ] || { echo "grind-clean-logs: nothing to remove (<= 2 run dirs)"; exit 0; }; \
	if [ -n "$$latest" ]; then old=$$(echo "$$old" | grep -v "/$$latest$$"); fi; \
	[ -n "$$old" ] || { echo "grind-clean-logs: nothing to remove"; exit 0; }; \
	echo "$$old" | sed 's/^/  removing /'; \
	echo "$$old" | xargs rm -rf; \
	echo "grind-clean-logs: $$(ls -d .grind-logs/2*/ 2>/dev/null | wc -l | tr -d ' ') run dir(s) remain"

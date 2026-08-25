# ── Server build stage ───────────────────────────────────────────────────────
# Compiles the release binary. The server is a member of the root cargo
# workspace (ARC-117): its Cargo.toml references [workspace.dependencies] in
# the root Cargo.toml, and the single lockfile lives at the workspace root,
# so the builder needs both root manifests and all three member manifests for
# `dep.workspace = true` resolution and `--locked`. Only the server binary is
# built; rust-client/cli sources stay stubbed for the dep-cache layer and are
# never compiled (--bin rtdb-server selects server alone). The shared target/
# lands at the workspace root (/build/target).
#
# ARC-120: `rust:bookworm` (rolling tag) replaces the previous `1.90-bookworm`
# pin so the base image tracks current stable; the rust-toolchain.toml file
# copied just below is the authoritative pin and rustup (shipped in the image)
# honors it. The previous 1.90 pin had drifted from CI's `stable` and from
# local toolchains, risking green-CI / broken-image divergence.
FROM rust:bookworm AS builder
WORKDIR /build
COPY rust-toolchain.toml ./
COPY Cargo.toml Cargo.lock ./
COPY core/Cargo.toml core/Cargo.toml
COPY server/Cargo.toml server/Cargo.toml
COPY rust-client/Cargo.toml rust-client/Cargo.toml
COPY cli/Cargo.toml cli/Cargo.toml
# ARC-004: `core` is a path dependency of the server, so its REAL source has to
# be present for the dependency layer to compile — a stub lib.rs would make the
# server's `par_rt_db_core::…` paths unresolvable. It is tiny and changes
# rarely, so copying it here costs no meaningful cache invalidation.
COPY core/src core/src
# ENH-028 phase 2: core/src/query_combinations.rs embeds this file at compile
# time via include_str!, so it must be present alongside core/src in this
# dependency layer or `cargo build` fails with "No such file or directory"
# even though core/src itself is there.
COPY wire-corpus/query-combinations.json wire-corpus/query-combinations.json
# Dependency layer: compile the whole dependency tree once, cached unless
# Cargo.toml or Cargo.lock change. Throwaway main/lib so each member package
# parses, then build the server deps without the real source (next layer).
# The rust-client [[test]] entries (ARC-110) name specific test files, which
# cargo validates at manifest-parse time even when building a different member's
# --bin — so the stub layer must create empty placeholders for them.
#
# ARC-011: this list is no longer maintained by memory. `make checkall` runs
# scripts/dockerfile-stub-check.sh, which diffs the workspace's declared
# [[test]] targets against the paths named here and fails on a missing stub.
# (The server's own [[test]] needs no stub: cargo skips test-target validation
# for the member it is building — measured, see the script's header.)
RUN mkdir -p server/src rust-client/src rust-client/tests cli/src cli/tests \
    && echo 'fn main() {}' > server/src/main.rs \
    && echo '' > rust-client/src/lib.rs \
    && echo 'fn main() {}' > cli/src/main.rs \
    && touch rust-client/tests/golden_vector.rs rust-client/tests/query_combinations.rs \
              rust-client/tests/semantics_corpus.rs rust-client/tests/hot_config_test.rs \
              rust-client/tests/ws_integration.rs rust-client/tests/http_integration.rs \
    && touch cli/tests/live.rs \
    && cargo build --release --locked --manifest-path server/Cargo.toml --bin rtdb-server \
    && rm -rf server/src
# Bake the live git sha into /healthz. The build context has no .git (rsync
# excludes it), so build.rs falls back to this arg; "unknown" if unset.
# Declared AFTER the dependency layer so a per-deploy commit change only
# recompiles the app crate, not the entire dependency tree.
ARG RTDB_BUILD_COMMIT=unknown
ENV RTDB_BUILD_COMMIT=${RTDB_BUILD_COMMIT}
COPY server/build.rs server/build.rs
COPY server/src ./server/src
# ARC-120: `server/tests` is intentionally NOT copied into the release-builder
# stage. `--bin rtdb-server` only compiles the binary, never the test targets,
# so the tests dir is dead weight here AND a test-only edit would invalidate
# this COPY layer's cache, forcing a full re-compile of the release binary.
RUN cargo build --release --locked --manifest-path server/Cargo.toml --bin rtdb-server

# ── Dashboard build stage ────────────────────────────────────────────────────
# Bundles the operator console SPA (Vite + React + TS) and the @par-rt-db/client
# SDK it vendors via the bun workspace. Output: dashboard/dist, served
# same-origin by the server from RTDB_STATIC_DIR (Phase 6 static hosting).
FROM oven/bun:1-debian AS dashboard
WORKDIR /build
# Workspace manifests + lockfile first so dependency install is layer-cached.
COPY package.json bun.lock ./
COPY ts-client/package.json ts-client/package.json
COPY dashboard/package.json dashboard/package.json
# `--ignore-scripts`: the dashboard's `prepare` runs `build:sdk`, which needs
# ts-client source not copied until the next layer. The explicit
# `bun run build` below already builds the SDK, so skipping lifecycle scripts
# here keeps the layer-cached install independent of source.
RUN bun install --frozen-lockfile --ignore-scripts
COPY ts-client ./ts-client
COPY dashboard ./dashboard
RUN cd dashboard && bun run build

# ── Runtime stage ────────────────────────────────────────────────────────────
# Minimal image: the release binary, CA roots (GitHub OAuth TLS), the SPA, and
# `pg_dump` (postgresql-client) for the optional managed-backup scheduler
# (RTDB_BACKUP_ENABLED). The backup task self-logs and continues if pg_dump is
# absent, but shipping it here means flipping the env flag is sufficient to
# turn backups on — no image rebuild needed.
FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates postgresql-client \
    && rm -rf /var/lib/apt/lists/* \
    # SEC-136: run the server as a non-root, non-login, no-home system user.
    # Port 8300 > 1024 so no CAP_NET_BIND_SERVICE is needed; the binary and the
    # SPA dir are world-readable/executable; runtime writes go to /tmp and the
    # backup dir, both provided as tmpfs in docker-compose (read_only rootfs).
    && groupadd --system --gid 10001 rtdb \
    && useradd --system --uid 10001 --gid rtdb --home-dir /app --no-create-home --shell /usr/sbin/nologin rtdb
COPY --from=builder /build/target/release/rtdb-server /usr/local/bin/rtdb-server
COPY --from=dashboard /build/dashboard/dist /app/dashboard-dist
RUN chown -R rtdb:rtdb /app
ENV RTDB_PORT=8300
# Serve the bundled SPA same-origin. Override (e.g. to "") to make it API-only.
ENV RTDB_STATIC_DIR=/app/dashboard-dist
EXPOSE 8300
USER rtdb
ENTRYPOINT ["/usr/local/bin/rtdb-server"]

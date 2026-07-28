# ── Server build stage ───────────────────────────────────────────────────────
# Compiles the release binary.
FROM rust:1.90-bookworm AS builder
WORKDIR /build/server
# Dependency layer: compile the whole dependency tree once, cached unless
# Cargo.toml or Cargo.lock change. A throwaway main.rs lets cargo resolve and
# build the deps without the real source (which arrives in the next layer).
COPY server/Cargo.toml server/Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs \
    && cargo build --release --locked --bin rtdb-server \
    && rm -rf src
# Bake the live git sha into /healthz. The build context has no .git (rsync
# excludes it), so build.rs falls back to this arg; "unknown" if unset.
# Declared AFTER the dependency layer so a per-deploy commit change only
# recompiles the app crate, not the entire dependency tree.
ARG RTDB_BUILD_COMMIT=unknown
ENV RTDB_BUILD_COMMIT=${RTDB_BUILD_COMMIT}
COPY server/build.rs ./
COPY server/src ./src
COPY server/tests ./tests
RUN cargo build --release --locked --bin rtdb-server

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
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/server/target/release/rtdb-server /usr/local/bin/rtdb-server
COPY --from=dashboard /build/dashboard/dist /app/dashboard-dist
ENV RTDB_PORT=8300
# Serve the bundled SPA same-origin. Override (e.g. to "") to make it API-only.
ENV RTDB_STATIC_DIR=/app/dashboard-dist
EXPOSE 8300
ENTRYPOINT ["/usr/local/bin/rtdb-server"]

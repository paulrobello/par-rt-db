# Build stage — compiles the release binary.
FROM rust:1.90-bookworm AS builder
WORKDIR /build/server
# Bake the live git sha into /healthz. The build context has no .git (rsync
# excludes it), so build.rs falls back to this arg; "unknown" if unset.
ARG RTDB_BUILD_COMMIT=unknown
ENV RTDB_BUILD_COMMIT=${RTDB_BUILD_COMMIT}
COPY server/Cargo.toml server/Cargo.lock ./
COPY server/build.rs ./
COPY server/src ./src
COPY server/tests ./tests
RUN cargo build --release --locked --bin rtdb-server

# Runtime stage — minimal image with just the binary + CA roots (for GitHub OAuth TLS).
FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/server/target/release/rtdb-server /usr/local/bin/rtdb-server
ENV RTDB_PORT=8300
EXPOSE 8300
ENTRYPOINT ["/usr/local/bin/rtdb-server"]

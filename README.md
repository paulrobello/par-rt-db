# par-rt-db

A Convex-inspired realtime document database server, written in Rust on axum/tokio with
Postgres storage and WebSocket-based subscriptions. See
[`docs/superpowers/specs`](docs/superpowers/specs) for the approved design spec.

## Quickstart

```bash
make dev-db-up   # start the dev Postgres container (127.0.0.1:55434)
make test        # run the server test suite
```

## Configuration

The server reads its configuration from environment variables:

| Variable                    | Required | Default                        |
|------------------------------|----------|---------------------------------|
| `RTDB_PORT`                  | no       | `8300`                          |
| `RTDB_DATABASE_URL`          | yes      | —                                |
| `RTDB_ADMIN_KEY`             | yes      | —                                |
| `RTDB_PUBLIC_URL`            | no       | `http://localhost:8300`         |
| `RTDB_ALLOWED_ORIGINS`       | no       | empty (comma-separated list)    |
| `RTDB_GITHUB_CLIENT_ID`      | no       | none                             |
| `RTDB_GITHUB_CLIENT_SECRET`  | no       | none                             |
| `RTDB_GITHUB_BASE_URL`       | no       | `https://github.com`            |
| `RTDB_GITHUB_API_URL`        | no       | `https://api.github.com`        |
| `RTDB_SESSION_TTL_DAYS`      | no       | `30`                             |

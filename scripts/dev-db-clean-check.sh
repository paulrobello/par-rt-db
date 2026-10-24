#!/usr/bin/env bash
# Self-check for scripts/dev-db-clean.sql's merge-fixture section: seeds
# throwaway rows matching the EXACT fixture shapes from merge_test.rs, runs
# the real clean script, asserts zero fixture rows remain, and asserts a
# non-matching sentinel (user + session) survives the sweep untouched.
# Requires the dev Postgres up (make dev-db-up) and psql on PATH.
set -euo pipefail

: "${RTDB_TEST_DATABASE_URL:=postgres://rtdb:rtdb@127.0.0.1:55434/rtdb}"
SQL_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/dev-db-clean.sql"

suffix="$(uuidgen | tr -d '-' | tr 'A-F' 'a-f')"  # 32 lowercase hex
stale_db="stale7${suffix:0:10}"                   # ^stale[0-9]+[0-9a-f]{10}$
anon_stale="anon_stale_${suffix:0:16}"            # ^anon2?_stale_[0-9a-f]{15,16}$
real_user="real_${suffix}"                        # ^real_[0-9a-f]{32}$
sentinel="zzcleanchk${suffix:0:20}"               # matches no clean pattern
now_ms="$(date +%s)000"

psql "$RTDB_TEST_DATABASE_URL" -v ON_ERROR_STOP=1 -q <<SQL
BEGIN;
INSERT INTO rtdb_auth.databases (name, created_at) VALUES ('${stale_db}', ${now_ms});
INSERT INTO rtdb_auth.users (id, login, anonymous, created_at) VALUES
    ('${anon_stale}', 'anonymous', TRUE, ${now_ms}),
    ('${real_user}', 'github', FALSE, ${now_ms}),
    ('${sentinel}', 'github', FALSE, ${now_ms});
INSERT INTO rtdb_auth.sessions (token_hash, user_id, expires_at, created_at) VALUES
    ('chk${suffix}', '${anon_stale}', ${now_ms} + 86400000, ${now_ms}),
    ('chk${suffix:0:16}sentinel', '${sentinel}', ${now_ms} + 86400000, ${now_ms});
COMMIT;
SQL

cleanup_sentinels() {
  psql "$RTDB_TEST_DATABASE_URL" -q <<SQL || true
DELETE FROM rtdb_auth.sessions
 WHERE token_hash IN ('chk${suffix}', 'chk${suffix:0:16}sentinel');
DELETE FROM rtdb_auth.databases WHERE name = '${stale_db}';
DELETE FROM rtdb_auth.users WHERE id = '${sentinel}';
SQL
}
trap cleanup_sentinels EXIT

echo "dev-db-clean-check: seeded ${stale_db}, ${anon_stale}, ${real_user}, sentinel ${sentinel}"
psql "$RTDB_TEST_DATABASE_URL" -v ON_ERROR_STOP=1 -f "$SQL_FILE" 2>&1 | grep "merge-fixture clean" || {
  echo "dev-db-clean-check: FAIL — clean script produced no merge-fixture notices" >&2
  exit 1
}

fail() { echo "dev-db-clean-check: FAIL — $1" >&2; exit 1; }
count() { psql "$RTDB_TEST_DATABASE_URL" -t -A -c "$1"; }

[ "$(count "SELECT count(*) FROM rtdb_auth.databases WHERE name = '${stale_db}'")" = "0" ] \
  || fail "stale database row survived"
[ "$(count "SELECT count(*) FROM rtdb_auth.users WHERE id IN ('${anon_stale}', '${real_user}')")" = "0" ] \
  || fail "fixture user rows survived"
[ "$(count "SELECT count(*) FROM rtdb_auth.sessions WHERE token_hash = 'chk${suffix}'")" = "0" ] \
  || fail "fixture session row survived"
[ "$(count "SELECT 'y' FROM rtdb_auth.users WHERE id = '${sentinel}' AND EXISTS (SELECT 1 FROM rtdb_auth.sessions WHERE token_hash = 'chk${suffix:0:16}sentinel')")" = "y" ] \
  || fail "sentinel user/session were touched by the clean"

echo "dev-db-clean-check: PASS — fixtures removed, sentinel untouched"

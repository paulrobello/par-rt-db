-- Drop leaked par-rt-db test schemas from the dev DB and clean their registry
-- rows. Integration tests create one database (a Postgres schema `db_t<uuid-v7>`)
-- per test via `common::fresh_db` and self-clean through the RAII cleanup
-- worker, but aborted runs leak a bounded tail; the semantics-corpus runner
-- (ENH-023) names its per-case databases `sc_<stem>_<12hex>` the same way. The
-- dev `rtdb` DB accumulates both, bloating `pg_dump` (and OOMing it past ~50k).
--
-- Idempotent and scoped to the test patterns — it never touches `rtdb`,
-- `rtdb_auth`, or any `pg_*`/system schema, so real (non-test) databases are
-- preserved. Run periodically:  make dev-db-clean   (dev Postgres must be up)
--
-- Each DROP executes in psql autocommit: \gexec runs the generated statements
-- one at a time, committing as it goes, so progress is durable and no single
-- transaction accumulates the catalog locks of dropping every schema at once.
-- One giant DO-block transaction hit "out of shared memory" and rolled back
-- wholesale at ~2.2k leaked schemas (2026-08-17); the sc_ pattern mirrors the
-- runner's db_name_for: sc_ + sanitized stem + _ + 12 hex.
SELECT format('DROP SCHEMA %I CASCADE', schema_name)
  FROM information_schema.schemata
 WHERE schema_name ~ '^db_(t[0-9a-f]{32}|sc_[a-z0-9_]+_[0-9a-f]{12})$'
\gexec
DELETE FROM rtdb_auth.databases      WHERE name    ~ '^(t[0-9a-f]{32}|sc_[a-z0-9_]+_[0-9a-f]{12})$';
DELETE FROM rtdb_auth.machine_tokens WHERE db_name ~ '^(t[0-9a-f]{32}|sc_[a-z0-9_]+_[0-9a-f]{12})$';
DELETE FROM rtdb_auth.allowlist      WHERE db_name ~ '^(t[0-9a-f]{32}|sc_[a-z0-9_]+_[0-9a-f]{12})$';
-- Webhook delivery rows a test enqueued but never drained to a terminal
-- (`delivered` / `failed`) state — left behind by a failed/aborted test run.
-- These linger in the shared `rtdb` schema (not the per-test db schema) and
-- keep retrying against `127.0.0.1:<ephemeral-port>` URLs whose listeners are
-- long gone, polluting parallel test runs whose `drain_once` re-delivers them
-- to whichever test happens to own the recycled port. Scoped to test-pattern
-- URLs (`127.0.0.1` / `localhost`) so production webhooks are never touched.
DELETE FROM rtdb.webhook_deliveries
 WHERE status IN ('pending', 'retrying')
   AND webhook_id IN (
     SELECT id FROM rtdb.webhooks
     WHERE url ~ '^https?://(127\.0\.0\.1|localhost)(:[0-9]+)?(/.*)?$'
   );
DELETE FROM rtdb.webhooks
 WHERE url ~ '^https?://(127\.0\.0\.1|localhost)(:[0-9]+)?(/.*)?$';

-- ---------------------------------------------------------------------------
-- Merge-test fixture rows (server/tests/merge_test.rs). Killed gate runs leak
-- the rows that file seeds directly in rtdb_auth — at ~5.4k of them every
-- merge_users scanned each live sibling registry row and merge_test stalled
-- (2026-10-24) — and the t/sc_ patterns above never match them. The shapes
-- are provably test-only: production ids come from db::new_id() (bare 32-hex
-- UUIDv7, db.rs:1023) and no src/ path mints any of these names.
--
--   databases: "stale<i><10hex>" x50, seeded by the stale-skip test
--   users:     the stale-skip test's anon2?/real2?_stale_<15,16hex> pairs and
--              the anon_<32hex> / real2?_<32hex> pairs the orchestration,
--              storage-swap, and auth-guard tests seed
--   sessions:  token_hash is a sha256 (no name pattern), so rows go by
--              user_id linkage — deleted BEFORE the users (FK references).
--
-- Each family runs in one DO-block transaction: count first, abort that
-- family (the exception rolls its DELETE back) if the match exceeds the
-- 50000-row safety bound, else delete and report the count.
DO $$
DECLARE
    n bigint;
BEGIN
    SELECT count(*) INTO n FROM rtdb_auth.databases
     WHERE name ~ '^stale[0-9]+[0-9a-f]{10}$';
    IF n > 50000 THEN
        RAISE EXCEPTION 'merge-fixture clean: % stale* databases rows exceeds the 50000-row safety bound — refusing; inspect manually', n;
    END IF;
    DELETE FROM rtdb_auth.databases WHERE name ~ '^stale[0-9]+[0-9a-f]{10}$';
    RAISE NOTICE 'merge-fixture clean: removed % stale* rtdb_auth.databases rows', n;
END
$$;

-- sessions before users: sessions.user_id references users(id) with no
-- CASCADE, so the users delete below only succeeds once these are gone.
DO $$
DECLARE
    fixture_users text := '^((anon2?|real2?)_stale_[0-9a-f]{15,16}|(anon|real2?)_[0-9a-f]{32})$';
    n bigint;
BEGIN
    SELECT count(*) INTO n FROM rtdb_auth.sessions
     WHERE user_id ~ fixture_users;
    IF n > 50000 THEN
        RAISE EXCEPTION 'merge-fixture clean: % fixture sessions exceeds the 50000-row safety bound — refusing; inspect manually', n;
    END IF;
    DELETE FROM rtdb_auth.sessions WHERE user_id ~ fixture_users;
    RAISE NOTICE 'merge-fixture clean: removed % fixture rtdb_auth.sessions rows', n;
END
$$;

DO $$
DECLARE
    fixture_users text := '^((anon2?|real2?)_stale_[0-9a-f]{15,16}|(anon|real2?)_[0-9a-f]{32})$';
    n bigint;
BEGIN
    SELECT count(*) INTO n FROM rtdb_auth.users
     WHERE id ~ fixture_users;
    IF n > 50000 THEN
        RAISE EXCEPTION 'merge-fixture clean: % fixture users exceeds the 50000-row safety bound — refusing; inspect manually', n;
    END IF;
    DELETE FROM rtdb_auth.users WHERE id ~ fixture_users;
    RAISE NOTICE 'merge-fixture clean: removed % fixture rtdb_auth.users rows', n;
END
$$;

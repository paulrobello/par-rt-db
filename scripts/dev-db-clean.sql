-- Drop leaked par-rt-db test schemas from the dev DB and clean their registry
-- rows. Integration tests create one database (a Postgres schema `db_t<uuid-v7>`)
-- per test via `common::fresh_db` and do not drop them, so the dev `rtdb` DB
-- accumulates `db_t<32hex>` schemas that bloat `pg_dump` (and OOM it past ~50k).
--
-- Idempotent and scoped to the test pattern — it never touches `rtdb`,
-- `rtdb_auth`, or any `pg_*`/system schema, so real (non-test) databases are
-- preserved. Run periodically:  make dev-db-clean   (dev Postgres must be up)
--
-- NOTE: for a one-time cleanup of an enormous accumulation (tens of thousands),
-- prefer autocommit per-schema DROPs (generate them with the SELECT below) so
-- progress is durable and a giant single transaction can't roll back. This DO
-- block is one transaction, appropriate for normal periodic maintenance volumes.
DO $$
DECLARE r record;
BEGIN
  FOR r IN
    SELECT schema_name FROM information_schema.schemata
    WHERE schema_name ~ '^db_t[0-9a-f]{32}$'
  LOOP
    EXECUTE format('DROP SCHEMA %I CASCADE', r.schema_name);
  END LOOP;
END $$;
DELETE FROM rtdb_auth.databases      WHERE name    ~ '^t[0-9a-f]{32}$';
DELETE FROM rtdb_auth.machine_tokens WHERE db_name ~ '^t[0-9a-f]{32}$';
DELETE FROM rtdb_auth.allowlist      WHERE db_name ~ '^t[0-9a-f]{32}$';
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


//! Managed `pg_dump` backup scheduler. When enabled at boot
//! (`RTDB_BACKUP_ENABLED=true`), a single background task runs `pg_dump`
//! against the configured Postgres on a 5-field UTC cron (`scheduler::next_fire`
//! — the same calculator the scheduled-txn feature uses) and retains the
//! newest `N` dumps in `backup_dir`. The connection string is parsed into
//! `PGUSER`/`PGPASSWORD`/`PGHOST`/`PGPORT`/`PGDATABASE` env vars on the child
//! process; the URL never appears in argv (it would leak credentials in `ps`).

use std::path::PathBuf;
use std::process::Stdio;

use chrono::{Datelike, NaiveDateTime, TimeZone, Timelike, Utc};
use tokio::time::{Duration, timeout};

use crate::db::now_ms;
use crate::error::RtDbError;
use crate::scheduler;

/// Cap on how long the loop sleeps in one `tokio::time::sleep` call. Bounds the
/// latency of a restart catching up to the next fire — a daily cron would
/// otherwise hang the task for ~24h, and a `SIGTERM` during that window would
/// be delayed. Mirrors the `MAX_SLEEP` pattern in `scheduler::run_scheduler`.
const MAX_BACKUP_SLEEP: Duration = Duration::from_secs(60);

/// The backup-task loop. Runs forever (until the process exits); pg_dump
/// failures and prune failures are logged and the loop continues — the backup
/// task must never crash the server. Spawned from `main` only when
/// `config.backup_enabled`.
pub async fn run_backup_task(database_url: String, dir: String, cron: String, retention: u32) {
    tracing::info!(
        cron = %cron,
        dir = %dir,
        retention,
        "backup task started"
    );
    loop {
        let now = now_ms();
        let next = match scheduler::next_fire(&cron, now) {
            Ok(t) => t,
            Err(err) => {
                // An invalid cron expression was accepted at boot because
                // validation is deferred to `next_fire`. Re-fire on a short
                // timer so a bad config surfaces in logs without spinning, and
                // so fixing the env + restarting catches up promptly.
                tracing::warn!(error = %err, cron = %cron, "backup cron invalid; sleeping");
                let _ = timeout(MAX_BACKUP_SLEEP, tokio::time::sleep(MAX_BACKUP_SLEEP)).await;
                continue;
            }
        };

        // Sleep until `next`, but in chunks ≤ MAX_BACKUP_SLEEP so a long cron
        // (e.g. daily) cannot hang a restart/respawn on the order of hours.
        while now_ms() < next {
            let remaining = (next - now_ms()).max(0) as u64;
            let chunk = Duration::from_millis(remaining).min(MAX_BACKUP_SLEEP);
            let _ = timeout(MAX_BACKUP_SLEEP, tokio::time::sleep(chunk)).await;
        }

        if let Err(err) = perform_backup(&database_url, &dir).await {
            tracing::error!(error = %err, "backup failed");
        }
        if let Err(err) = prune_old(&dir, retention).await {
            tracing::warn!(error = %err, "backup prune failed");
        }
    }
}

/// Parsed components of a `postgres://` URL, ready to set as `PG*` env vars on
/// the child. Each field is `Option` because the URL may legitimately omit any
/// of them (e.g. password, port). `pg_dump` falls back to its own defaults for
/// any unset `PG*` var.
struct PgEnv {
    user: Option<String>,
    password: Option<String>,
    host: Option<String>,
    port: Option<u16>,
    database: Option<String>,
}

/// Parses a `postgres://` / `postgresql://` connection string into its `PG*`
/// env components. Used instead of passing the URL on the command line so the
/// password is not visible in `ps` / `/proc/<pid>/cmdline`. URL-encoded
/// credentials (decoded here — `url::Url::username`/`password`/path return the
/// raw percent-encoded form) and missing port/password are handled gracefully.
fn parse_pg_env(database_url: &str) -> Result<PgEnv, RtDbError> {
    use percent_encoding::percent_decode_str;
    let parsed =
        url::Url::parse(database_url).map_err(|_| RtDbError::internal("invalid database_url"))?;
    let scheme = parsed.scheme();
    if scheme != "postgres" && scheme != "postgresql" {
        return Err(RtDbError::internal(
            "database_url must use postgres:// or postgresql://",
        ));
    }
    let decode = |s: &str| percent_decode_str(s).decode_utf8_lossy().into_owned();
    let user = if parsed.username().is_empty() {
        None
    } else {
        Some(decode(parsed.username()))
    };
    let password = parsed.password().map(decode);
    let host = parsed.host_str().map(str::to_string);
    let port = parsed.port();
    // The dbname is the path with the leading '/' stripped; an empty path
    // means "no database specified" (pg_dump then uses PGDATABASE or the
    // Unix-user default).
    let database = {
        let path = parsed.path().trim_start_matches('/');
        if path.is_empty() {
            None
        } else {
            Some(decode(path))
        }
    };
    Ok(PgEnv {
        user,
        password,
        host,
        port,
        database,
    })
}

/// Formats a UTC timestamp (epoch millis) as `YYYYmmddTHHMMSSZ` — an ISO-8601
/// basic-format filename-safe stamp. Lexicographic order equals chronological
/// order, which `prune_old` and the admin listing rely on. Uses `chrono` (the
/// codebase already depends on it for `scheduler::next_fire`) without the
/// `clock` feature.
fn format_timestamp_utc(ms: i64) -> String {
    let dt = Utc.timestamp_millis_opt(ms).single().unwrap_or_else(|| {
        // `now_ms()` can in principle produce a value outside chrono's
        // representable range on exotic platforms; fall back to the epoch
        // rather than panicking. The fallback is not load-bearing in practice.
        Utc.timestamp_millis_opt(0).unwrap()
    });
    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        dt.year(),
        dt.month(),
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second()
    )
}

/// Inverse of `format_timestamp_utc` — parses the filename stem
/// (`YYYYmmddTHHMMSSZ`) back to epoch millis. Used by the admin listing so
/// `createdMs` is the canonical stamp embedded in the name (filesystem `ctime`
/// is best-effort and not portable). Returns `None` on any malformed input.
fn parse_timestamp_utc(stem: &str) -> Option<i64> {
    let ndt = NaiveDateTime::parse_from_str(stem, "%Y%m%dT%H%M%SZ").ok()?;
    Some(Utc.from_utc_datetime(&ndt).timestamp_millis())
}

/// Builds the dump filename `<dir>/rtdb-<UTC stamp>.dump` for the given
/// timestamp, creating `dir` if needed.
async fn backup_path(dir: &str, ms: i64) -> Result<PathBuf, RtDbError> {
    tokio::fs::create_dir_all(dir)
        .await
        .map_err(|e| RtDbError::internal(format!("failed to create backup dir: {e}")))?;
    let mut path = PathBuf::from(dir);
    path.push(format!("rtdb-{}.dump", format_timestamp_utc(ms)));
    Ok(path)
}

/// Runs `pg_dump --format=custom --file <path>` against the database, with the
/// connection parameters supplied as `PG*` env vars (never on argv). On
/// non-zero exit the pg_dump stderr is logged and a generic `internal` error
/// is returned (the stderr text never reaches a client-facing body — this is a
/// background task — but the same discipline applies for defense in depth).
async fn perform_backup(database_url: &str, dir: &str) -> Result<PathBuf, RtDbError> {
    let pg = parse_pg_env(database_url)?;
    let path = backup_path(dir, now_ms()).await?;

    let mut cmd = tokio::process::Command::new("pg_dump");
    cmd.arg("--format=custom");
    cmd.arg("--file");
    cmd.arg(&path);
    if let Some(user) = pg.user.as_deref() {
        cmd.env("PGUSER", user);
    }
    if let Some(pw) = pg.password.as_deref() {
        cmd.env("PGPASSWORD", pw);
    }
    if let Some(host) = pg.host.as_deref() {
        cmd.env("PGHOST", host);
    }
    if let Some(port) = pg.port {
        cmd.env("PGPORT", port.to_string());
    }
    if let Some(db) = pg.database.as_deref() {
        cmd.env("PGDATABASE", db);
    }
    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::null());
    cmd.stderr(Stdio::piped());

    let output = cmd
        .output()
        .await
        .map_err(|e| RtDbError::internal(format!("failed to spawn pg_dump: {e}")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        tracing::error!(
            stderr = %stderr,
            code = ?output.status.code(),
            "pg_dump failed"
        );
        // Best-effort: remove the empty/partial dump so the next run doesn't
        // accumulate a corrupt file the retention sweep would later treat as
        // legitimate.
        let _ = tokio::fs::remove_file(&path).await;
        return Err(RtDbError::internal("pg_dump failed; see server logs"));
    }
    tracing::info!(path = %path.display(), "backup completed");
    Ok(path)
}

/// Deletes all but the newest `retention` `rtdb-*.dump` files in `dir`. Files
/// are sorted by name descending (ISO-8601 stamps make lexicographic order
/// chronological); everything past the first `retention` is removed. Per-file
/// deletion errors are logged and skipped — a single unreadable file does not
/// abort the sweep.
async fn prune_old(dir: &str, retention: u32) -> Result<(), RtDbError> {
    let mut names: Vec<String> = Vec::new();
    let mut reader = match tokio::fs::read_dir(dir).await {
        Ok(r) => r,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => {
            return Err(RtDbError::internal(format!(
                "failed to read backup dir: {e}"
            )));
        }
    };
    while let Some(entry) = reader
        .next_entry()
        .await
        .map_err(|e| RtDbError::internal(format!("failed to read dir entry: {e}")))?
    {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with("rtdb-") && name.ends_with(".dump") {
            names.push(name);
        }
    }
    // Newest first (lexicographic = chronological for ISO-8601 stamps).
    names.sort_by(|a, b| b.cmp(a));
    let keep = retention as usize;
    if names.len() <= keep {
        return Ok(());
    }
    for name in names.into_iter().skip(keep) {
        let path = PathBuf::from(dir).join(&name);
        if let Err(e) = tokio::fs::remove_file(&path).await {
            tracing::warn!(error = %e, file = %name, "prune: failed to delete backup");
        }
    }
    Ok(())
}

/// Lists the `rtdb-*.dump` files in `dir` newest-first, with their size in
/// bytes and the timestamp parsed from the name as `createdMs`. A missing dir
/// yields an empty list — `GET /admin/backups` must never 500 just because no
/// backup has run yet.
pub(crate) async fn list_backups(dir: &str) -> Result<Vec<BackupFile>, RtDbError> {
    let mut out: Vec<BackupFile> = Vec::new();
    let mut reader = match tokio::fs::read_dir(dir).await {
        Ok(r) => r,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(e) => {
            return Err(RtDbError::internal(format!(
                "failed to read backup dir: {e}"
            )));
        }
    };
    while let Some(entry) = reader
        .next_entry()
        .await
        .map_err(|e| RtDbError::internal(format!("failed to read dir entry: {e}")))?
    {
        let name = entry.file_name().to_string_lossy().into_owned();
        if !(name.starts_with("rtdb-") && name.ends_with(".dump")) {
            continue;
        }
        let stem = &name["rtdb-".len()..name.len() - ".dump".len()];
        // Fall back to 0 when the filename is malformed; size still carries
        // useful info and the entry shouldn't be hidden by a parse miss.
        let created_ms = parse_timestamp_utc(stem).unwrap_or(0);
        let size_bytes = entry.metadata().await.map(|m| m.len()).unwrap_or(0);
        out.push(BackupFile {
            name,
            size_bytes,
            created_ms,
        });
    }
    // Newest first (lexicographic = chronological for ISO-8601 stamps).
    out.sort_by(|a, b| b.name.cmp(&a.name));
    Ok(out)
}

/// One managed-backup file as exposed by `GET /admin/backups`.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BackupFile {
    pub name: String,
    pub size_bytes: u64,
    pub created_ms: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_pg_env_basic_postgres_url() {
        let pg = parse_pg_env("postgres://u:p@host:5432/db").unwrap();
        assert_eq!(pg.user.as_deref(), Some("u"));
        assert_eq!(pg.password.as_deref(), Some("p"));
        assert_eq!(pg.host.as_deref(), Some("host"));
        assert_eq!(pg.port, Some(5432));
        assert_eq!(pg.database.as_deref(), Some("db"));
    }

    #[test]
    fn parse_pg_env_accepts_postgresql_scheme() {
        let pg = parse_pg_env("postgresql://u:p@host:5432/db").unwrap();
        assert_eq!(pg.user.as_deref(), Some("u"));
        assert_eq!(pg.database.as_deref(), Some("db"));
    }

    #[test]
    fn parse_pg_env_decodes_url_encoded_credentials() {
        // `p@ss:word` URL-encoded — `url::Url::password` returns it decoded.
        let pg = parse_pg_env("postgres://us%40er:p%40ss%3Aword@h:5433/d").unwrap();
        assert_eq!(pg.user.as_deref(), Some("us@er"));
        assert_eq!(pg.password.as_deref(), Some("p@ss:word"));
        assert_eq!(pg.port, Some(5433));
        assert_eq!(pg.database.as_deref(), Some("d"));
    }

    #[test]
    fn parse_pg_env_handles_missing_port_and_password() {
        let pg = parse_pg_env("postgres://user@host/dbname").unwrap();
        assert_eq!(pg.user.as_deref(), Some("user"));
        assert_eq!(pg.password, None);
        assert_eq!(pg.host.as_deref(), Some("host"));
        assert_eq!(pg.port, None);
        assert_eq!(pg.database.as_deref(), Some("dbname"));
    }

    #[test]
    fn parse_pg_env_handles_missing_database() {
        let pg = parse_pg_env("postgres://u:p@host:5432").unwrap();
        assert_eq!(pg.database, None);
    }

    #[test]
    fn parse_pg_env_rejects_non_postgres_scheme() {
        assert!(parse_pg_env("mysql://u:p@h/db").is_err());
        assert!(parse_pg_env("not a url").is_err());
    }

    #[test]
    fn timestamp_format_round_trips() {
        // 2026-07-28T14:30:45Z. Derive the epoch-ms via the same code path
        // `parse_timestamp_utc` uses (chrono) so the assertion is on the
        // round-trip, not a hand-computed magic number.
        let dt = NaiveDateTime::parse_from_str("2026-07-28T14:30:45", "%Y-%m-%dT%H:%M:%S").unwrap();
        let ms = Utc.from_utc_datetime(&dt).timestamp_millis();
        let stamp = format_timestamp_utc(ms);
        assert_eq!(stamp, "20260728T143045Z");
        assert_eq!(parse_timestamp_utc(&stamp).unwrap(), ms);
    }

    #[test]
    fn parse_timestamp_utc_rejects_garbage() {
        assert!(parse_timestamp_utc("not-a-stamp").is_none());
        assert!(parse_timestamp_utc("20260728143045").is_none()); // missing T
    }

    #[test]
    fn backup_filenames_sort_chronologically() {
        // Newest-first lexicographic ordering is chronological for ISO-8601
        // stamps — `prune_old` and the admin listing rely on this.
        let mut names = vec![
            "rtdb-20260728T030000Z.dump",
            "rtdb-20260727T030000Z.dump",
            "rtdb-20260729T030000Z.dump",
        ];
        names.sort_by(|a, b| b.cmp(a));
        assert_eq!(
            names,
            vec![
                "rtdb-20260729T030000Z.dump",
                "rtdb-20260728T030000Z.dump",
                "rtdb-20260727T030000Z.dump",
            ]
        );
    }

    #[test]
    fn next_fire_reuse_returns_sane_future_for_default_cron() {
        // The default backup cron is `0 3 * * *` (daily 03:00 UTC). The next
        // fire must be strictly after `now` and at most ~1 day away.
        let now = now_ms();
        let next = scheduler::next_fire("0 3 * * *", now).unwrap();
        assert!(next > now);
        assert!(next - now <= 24 * 60 * 60 * 1000 + 60 * 1000);
    }

    #[tokio::test]
    async fn prune_old_keeps_newest_retention_and_deletes_rest() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_str().unwrap().to_string();

        // Seed five backups (one per day). Names are ISO-stamped so the sort
        // is chronological.
        for day in 20..=24u32 {
            let stamp = format!("202607{:02}T030000Z", day); // 2026-07-20..24
            let path = dir.path().join(format!("rtdb-{stamp}.dump"));
            tokio::fs::write(&path, b"x")
                .await
                .expect("write fake dump");
        }
        // Add a non-matching file to confirm the filter skips it.
        tokio::fs::write(dir.path().join("README.txt"), b"nope")
            .await
            .unwrap();
        // And a `.dump` that doesn't match the prefix.
        tokio::fs::write(dir.path().join("other-20260725T030000Z.dump"), b"nope")
            .await
            .unwrap();

        // Retention = 2 → keep 2026-07-23 and 2026-07-24; delete the other 3.
        prune_old(&dir_path, 2).await.expect("prune ok");

        let mut remaining: Vec<String> = Vec::new();
        let mut reader = tokio::fs::read_dir(&dir_path).await.unwrap();
        while let Some(entry) = reader.next_entry().await.unwrap() {
            remaining.push(entry.file_name().to_string_lossy().into_owned());
        }
        remaining.sort();
        assert_eq!(
            remaining,
            vec![
                "README.txt".to_string(),
                "other-20260725T030000Z.dump".to_string(),
                "rtdb-20260723T030000Z.dump".to_string(),
                "rtdb-20260724T030000Z.dump".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn prune_old_missing_dir_is_ok() {
        // A missing backup dir is normal (no run has happened yet); prune is a
        // no-op rather than an error.
        prune_old("/nonexistent/rtdb-backup-test-dir", 7)
            .await
            .expect("missing dir is ok");
    }

    #[tokio::test]
    async fn list_backups_missing_dir_is_empty() {
        let listed = list_backups("/nonexistent/rtdb-backup-test-dir")
            .await
            .expect("missing dir is ok");
        assert!(listed.is_empty());
    }

    #[tokio::test]
    async fn list_backups_returns_newest_first_with_parsed_timestamps() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_str().unwrap().to_string();
        tokio::fs::write(dir.path().join("rtdb-20260727T030000Z.dump"), b"aa")
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("rtdb-20260729T030000Z.dump"), b"c")
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("rtdb-20260728T030000Z.dump"), b"bbb")
            .await
            .unwrap();
        // Non-matching files are filtered out.
        tokio::fs::write(dir.path().join("README.txt"), b"x")
            .await
            .unwrap();

        let listed = list_backups(&dir_path).await.expect("list ok");
        assert_eq!(listed.len(), 3);
        // Expected createdMs derived via chrono so the assertion is on the
        // parse, not a hand-computed magic number.
        let expected_ms = |s: &str| {
            let ndt = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S").unwrap();
            Utc.from_utc_datetime(&ndt).timestamp_millis()
        };
        // Newest first.
        assert_eq!(listed[0].name, "rtdb-20260729T030000Z.dump");
        assert_eq!(listed[0].size_bytes, 1);
        assert_eq!(listed[0].created_ms, expected_ms("2026-07-29T03:00:00"));
        assert_eq!(listed[1].name, "rtdb-20260728T030000Z.dump");
        assert_eq!(listed[1].size_bytes, 3);
        assert_eq!(listed[2].name, "rtdb-20260727T030000Z.dump");
        assert_eq!(listed[2].size_bytes, 2);
    }

    /// End-to-end shell-out to a real `pg_dump`. Self-skips when `pg_dump` is
    /// not on PATH (CI / devs without postgres-client) so `cargo test --lib`
    /// stays green; run with `cargo test --lib backup -- --ignored` against a
    /// live dev Postgres on 127.0.0.1:55434.
    #[tokio::test]
    #[ignore = "requires pg_dump on PATH + a live Postgres; self-skips otherwise"]
    async fn perform_backup_against_dev_postgres() {
        let probe = tokio::process::Command::new("pg_dump")
            .arg("--version")
            .stderr(Stdio::null())
            .stdout(Stdio::null())
            .status()
            .await;
        if !matches!(probe, Ok(s) if s.success()) {
            eprintln!("skipping: pg_dump not found on PATH");
            return;
        }
        let url = std::env::var("RTDB_TEST_DATABASE_URL")
            .unwrap_or_else(|_| "postgres://rtdb:rtdb@127.0.0.1:55434/rtdb".into());
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_str().unwrap().to_string();
        let path = perform_backup(&url, &dir_path).await.expect("backup ok");
        assert!(
            path.exists(),
            "dump file should exist at {}",
            path.display()
        );
        assert!(
            tokio::fs::metadata(&path)
                .await
                .map(|m| m.len() > 0)
                .unwrap_or(false),
            "dump file should be non-empty"
        );
    }
}

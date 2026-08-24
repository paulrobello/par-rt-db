//! Managed pg_dump backup scheduler knobs, nested under `Config::backup`
//! (ARC-012). Off by default — when `enabled` is true, a background task
//! runs `pg_dump` on `cron` (5-field UTC cron, same format
//! `scheduler::next_fire` already handles) into `dir`, keeping the newest
//! `retention` dumps.

use super::{env_bool, env_parsed};

#[derive(Clone, Debug)]
pub struct BackupConfig {
    pub enabled: bool,  // RTDB_BACKUP_ENABLED, default false
    pub cron: String,   // RTDB_BACKUP_CRON, default "0 3 * * *"
    pub dir: String,    // RTDB_BACKUP_DIR, default "./backups"
    pub retention: u32, // RTDB_BACKUP_RETENTION, default 7
}

impl Default for BackupConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cron: "0 3 * * *".to_string(),
            dir: "./backups".to_string(),
            retention: 7,
        }
    }
}

impl BackupConfig {
    pub(super) fn from_env() -> Result<Self, String> {
        // Default off; cron/dir/retention carry their own defaults so an
        // operator can flip just RTDB_BACKUP_ENABLED=true to get daily 03:00
        // UTC dumps with 7-day retention. An empty RTDB_BACKUP_CRON falls
        // back to the default (a blank cron would surface as
        // `invalid cron expression` from `scheduler::next_fire` on every loop
        // iteration, so clamp here).
        let enabled = env_bool("RTDB_BACKUP_ENABLED", false);
        let cron = match std::env::var("RTDB_BACKUP_CRON") {
            Ok(v) if !v.trim().is_empty() => v,
            _ => "0 3 * * *".to_string(),
        };
        let dir = match std::env::var("RTDB_BACKUP_DIR") {
            Ok(v) if !v.trim().is_empty() => v,
            _ => "./backups".to_string(),
        };
        let retention = env_parsed("RTDB_BACKUP_RETENTION", 7u32)?;
        Ok(Self {
            enabled,
            cron,
            dir,
            retention,
        })
    }
}

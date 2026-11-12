//! Database registry and per-database lifecycle. Four concerns that used to
//! share one file: boot-time global DDL (`bootstrap`), tenant database
//! create/drop/read-freeze (`lifecycle`), the quiesce/admission-epoch fence
//! (`quiesce`), and the generic helpers now living in `crate::util`. Every
//! item stays reachable at its historical `crate::db::` path through the
//! re-exports below, so the high fan-in on `now_ms` needed no call-site churn.

mod bootstrap;
mod lifecycle;
mod quiesce;

pub use crate::util::{SchemaCache, new_id, now_ms, random_token, sha256_hex};
pub use bootstrap::{EXTENSION_LOCK_KEY, bootstrap, ensure_audit_table, ensure_webhooks_tables};
pub(crate) use lifecycle::validate_db_name;
pub use lifecycle::{
    create_database, creation_id, database_exists, drop_database, drop_database_if, is_read_only,
    list_databases, load_schema, set_read_only,
};
pub use quiesce::{
    acquire_quiesce_lock, admission_epoch, clear_quiesce, clear_quiesce_on_connection,
    ensure_admission_epoch, ensure_quiesce_generation, ensure_quiesce_open, is_quiesced,
    quiesce_admission, quiesce_database, quiesce_key, quiesce_matches, quiesce_state,
    release_quiesce_lock, try_acquire_quiesce_lock,
};

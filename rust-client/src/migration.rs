//! Declarative schema-migration builder — a port of `ts-client`'s `Migration`
//! (Task 8) and a sibling of [`crate::mutation::Mutation`]. Produces an ordered
//! `Vec<Directive>` (or an owned [`crate::wire::admin::MigrateRequestOwned`])
//! for the admin [`crate::http::RtDbHttpClient::migrate_schema`] HTTP path and
//! the in-memory harness.
//!
//! One `Migration` chains the per-directive methods (`rename_field`, `drop_table`,
//! …), then [`build`](Migration::build) yields the `Vec<Directive>` the server
//! applies transactionally. `dry_run` is carried by
//! [`build_request`](Migration::build_request) for callers (the CLI) that want
//! the full request body.

use crate::schema::FieldType;
use crate::wire::admin::{Cast, Directive, MigrateRequestOwned};
use serde_json::Value;

/// Builder for a schema migration — an ordered list of [`Directive`]s the
/// server applies transactionally to transform a database's schema and
/// documents. Mirrors server `migrate::Directive` (via `wire::admin`) and
/// `ts-client`'s `Migration`.
///
/// Chain the per-directive methods, then call [`build`](Self::build) for a
/// `Vec<Directive>` to pass to
/// [`RtDbHttpClient::migrate_schema`](crate::http::RtDbHttpClient::migrate_schema),
/// or [`build_request`](Self::build_request) for the full owned request body.
pub struct Migration {
    directives: Vec<Directive>,
    dry_run: bool,
}

impl Migration {
    pub fn new() -> Self {
        Self {
            directives: Vec::new(),
            dry_run: false,
        }
    }

    pub fn rename_field(mut self, table: &str, from: &str, to: &str) -> Self {
        self.directives.push(Directive::RenameField {
            table: table.into(),
            from: from.into(),
            to: to.into(),
        });
        self
    }

    pub fn rename_table(mut self, from: &str, to: &str) -> Self {
        self.directives.push(Directive::RenameTable {
            from: from.into(),
            to: to.into(),
        });
        self
    }

    pub fn change_type(
        mut self,
        table: &str,
        field: &str,
        to: FieldType,
        cast: Cast,
        default: Option<Value>,
    ) -> Self {
        self.directives.push(Directive::ChangeType {
            table: table.into(),
            field: field.into(),
            to,
            cast,
            default,
        });
        self
    }

    pub fn drop_field(mut self, table: &str, field: &str) -> Self {
        self.directives.push(Directive::DropField {
            table: table.into(),
            field: field.into(),
        });
        self
    }

    pub fn drop_table(mut self, name: &str) -> Self {
        self.directives
            .push(Directive::DropTable { name: name.into() });
        self
    }

    pub fn drop_index(mut self, table: &str, name: &str) -> Self {
        self.directives.push(Directive::DropIndex {
            table: table.into(),
            name: name.into(),
        });
        self
    }

    pub fn set_default(mut self, table: &str, field: &str, value: Value) -> Self {
        self.directives.push(Directive::SetDefault {
            table: table.into(),
            field: field.into(),
            value,
        });
        self
    }

    pub fn eval_expr(
        mut self,
        table: &str,
        set: &str,
        expr: &str,
        where_clause: Option<&str>,
    ) -> Self {
        self.directives.push(Directive::EvalExpr {
            table: table.into(),
            set: set.into(),
            expr: expr.into(),
            where_clause: where_clause.map(str::to_string),
        });
        self
    }

    /// Stash the `dryRun` flag for [`build_request`](Self::build_request).
    /// [`build`](Self::build) discards it — the HTTP method takes `dry_run` as a
    /// separate argument.
    pub fn dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }

    /// The ordered directives, ready for
    /// [`RtDbHttpClient::migrate_schema`](crate::http::RtDbHttpClient::migrate_schema).
    pub fn build(self) -> Vec<Directive> {
        self.directives
    }

    /// The full owned request body (directives + `dryRun`), for callers that
    /// hold the request past a borrow (the `rtdb` CLI).
    pub fn build_request(self) -> MigrateRequestOwned {
        MigrateRequestOwned {
            directives: self.directives,
            dry_run: self.dry_run,
        }
    }
}

impl Default for Migration {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::FieldType;
    use crate::wire::admin::Cast;
    use serde_json::json;

    #[test]
    fn builder_emits_all_directive_kinds() {
        let req = Migration::new()
            .rename_field("users", "name", "fullName")
            .rename_table("old", "new")
            .change_type(
                "users",
                "age",
                FieldType::String,
                Cast::ToString,
                Some(json!("0")),
            )
            .drop_field("users", "legacy")
            .drop_table("gone")
            .drop_index("users", "by_email")
            .set_default("users", "role", json!("member"))
            .eval_expr(
                "users",
                "upper",
                "upper(doc->>'fullName')",
                Some("doc ? 'fullName'"),
            )
            .build_request();
        let v = serde_json::to_value(&req).unwrap();
        let ops: Vec<&str> = v["directives"]
            .as_array()
            .unwrap()
            .iter()
            .map(|d| d["op"].as_str().unwrap())
            .collect();
        assert_eq!(
            ops,
            [
                "renameField",
                "renameTable",
                "changeType",
                "dropField",
                "dropTable",
                "dropIndex",
                "setDefault",
                "evalExpr"
            ]
        );
        // `where` alias + `default` carried.
        assert_eq!(v["directives"][2]["default"], json!("0"));
        assert_eq!(v["directives"][7]["where"], "doc ? 'fullName'");
        // dryRun defaults false.
        assert_eq!(v["dryRun"], false);
    }

    #[test]
    fn dry_run_flag_surfaces_on_build_request() {
        let req = Migration::new()
            .dry_run(true)
            .rename_field("users", "name", "fullName")
            .build_request();
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(v["dryRun"], true);
    }

    #[test]
    fn build_returns_directives_only() {
        let directives = Migration::new().dry_run(true).drop_table("gone").build();
        assert_eq!(directives.len(), 1);
        // dry_run is discarded by `build` — the HTTP method takes it separately.
        assert!(matches!(
            directives[0],
            Directive::DropTable { ref name } if name == "gone"
        ));
    }
}

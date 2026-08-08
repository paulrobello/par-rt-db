//! Atomic multi-step transaction DSL for writing documents to par-rt-db.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::wire::FilterExpr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub steps: Vec<Step>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "camelCase", deny_unknown_fields)]
pub enum Step {
    Insert {
        table: String,
        doc: Map<String, Value>,
    },
    Patch {
        table: String,
        id: String,
        fields: Map<String, Value>,
    },
    Replace {
        table: String,
        id: String,
        doc: Map<String, Value>,
    },
    Delete {
        table: String,
        id: String,
    },
    ExpectVersion {
        table: String,
        id: String,
        version: i64,
    },
    ExpectAbsent {
        table: String,
        index: String,
        eq: Vec<Value>,
    },
    Upsert {
        table: String,
        index: String,
        eq: Vec<Value>,
        insert: Map<String, Value>,
        patch: Map<String, Value>,
    },
    /// Patch every row in `table` matching `filter`. At most `limit` rows
    /// (default server cap 1000); a larger match set patches `limit` and reports
    /// `truncated: true`. Mirrors `server/src/txn.rs::Step::PatchByQuery`
    /// byte-for-byte.
    PatchByQuery {
        table: String,
        filter: FilterExpr,
        patch: Map<String, Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<u32>,
    },
    /// Delete every row in `table` matching `filter` (same `limit`/`truncated`
    /// semantics as `PatchByQuery`). Mirrors
    /// `server/src/txn.rs::Step::DeleteByQuery` byte-for-byte.
    DeleteByQuery {
        table: String,
        filter: FilterExpr,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<u32>,
    },
}

/// One entry of `mutateOk.results`, positionally aligned with `steps`.
///
/// Variant order matters: `Upsert` must precede `Insert` because serde's
/// `untagged` deserializer tries variants in declaration order and struct
/// variants ignore unknown fields — so `{id, inserted}` would otherwise be
/// greedily captured by `Insert`, silently dropping `inserted`. `PatchByQuery`
/// and `DeleteByQuery` carry distinct fields (`patched`/`deleted` + `truncated`)
/// that never collide with `{id}` / `{id, inserted}`, so their order relative
/// to the others is unconstrained.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StepResult {
    Upsert { id: String, inserted: bool },
    Insert { id: String },
    PatchByQuery { patched: u32, truncated: bool },
    DeleteByQuery { deleted: u32, truncated: bool },
    Null,
}

/// `null` on the wire deserializes to `StepResult::Null`.
impl Default for StepResult {
    fn default() -> Self {
        StepResult::Null
    }
}

pub struct Mutation {
    steps: Vec<Step>,
}

impl Mutation {
    pub fn new() -> Self {
        Self { steps: Vec::new() }
    }

    fn obj(v: Value) -> Map<String, Value> {
        match v {
            Value::Object(m) => m,
            // Non-object input is a caller bug; send an empty object so the server
            // rejects it with SCHEMA_VIOLATION rather than panicking client-side.
            _ => Map::new(),
        }
    }

    pub fn insert(mut self, table: &str, doc: Value) -> Self {
        self.steps.push(Step::Insert {
            table: table.into(),
            doc: Self::obj(doc),
        });
        self
    }
    pub fn patch(mut self, table: &str, id: &str, fields: Value) -> Self {
        self.steps.push(Step::Patch {
            table: table.into(),
            id: id.into(),
            fields: Self::obj(fields),
        });
        self
    }
    pub fn replace(mut self, table: &str, id: &str, doc: Value) -> Self {
        self.steps.push(Step::Replace {
            table: table.into(),
            id: id.into(),
            doc: Self::obj(doc),
        });
        self
    }
    pub fn delete(mut self, table: &str, id: &str) -> Self {
        self.steps.push(Step::Delete {
            table: table.into(),
            id: id.into(),
        });
        self
    }
    pub fn expect_version(mut self, table: &str, id: &str, version: i64) -> Self {
        self.steps.push(Step::ExpectVersion {
            table: table.into(),
            id: id.into(),
            version,
        });
        self
    }
    pub fn expect_absent(mut self, table: &str, index: &str, eq: &[Value]) -> Self {
        self.steps.push(Step::ExpectAbsent {
            table: table.into(),
            index: index.into(),
            eq: eq.to_vec(),
        });
        self
    }
    pub fn upsert(
        mut self,
        table: &str,
        index: &str,
        eq: &[Value],
        insert: Value,
        patch: Value,
    ) -> Self {
        self.steps.push(Step::Upsert {
            table: table.into(),
            index: index.into(),
            eq: eq.to_vec(),
            insert: Self::obj(insert),
            patch: Self::obj(patch),
        });
        self
    }

    /// Patch every row in `table` matching `filter`. `limit` defaults to the
    /// server cap (1000) when `None`; a larger match set patches `limit` rows and
    /// reports `truncated: true` in the result.
    pub fn patch_by_query(
        mut self,
        table: &str,
        filter: FilterExpr,
        patch: Value,
        limit: Option<u32>,
    ) -> Self {
        self.steps.push(Step::PatchByQuery {
            table: table.into(),
            filter,
            patch: Self::obj(patch),
            limit,
        });
        self
    }

    /// Delete every row in `table` matching `filter` (same `limit`/`truncated`
    /// semantics as [`patch_by_query`](Self::patch_by_query)).
    pub fn delete_by_query(mut self, table: &str, filter: FilterExpr, limit: Option<u32>) -> Self {
        self.steps.push(Step::DeleteByQuery {
            table: table.into(),
            filter,
            limit,
        });
        self
    }

    pub fn build(self) -> Transaction {
        Transaction { steps: self.steps }
    }
}

impl Default for Mutation {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn builder_serializes_all_step_kinds() {
        let txn = Mutation::new()
            .insert("items", json!({"projectId":"p1","title":"a"}))
            .patch("items", "i1", json!({"title":"b"}))
            .replace("items", "i4", json!({"projectId":"p1","title":"c"}))
            .delete("items", "i2")
            .expect_version("items", "i3", 7)
            .expect_absent(
                "items",
                "by_project_and_title",
                &[json!("p1"), json!("dup")],
            )
            .upsert(
                "items",
                "by_project",
                &[json!("p1")],
                json!({"projectId":"p1"}),
                json!({"title":"u"}),
            )
            .build();
        assert_eq!(
            serde_json::to_value(&txn).unwrap(),
            json!({
                "steps": [
                    {"op":"insert","table":"items","doc":{"projectId":"p1","title":"a"}},
                    {"op":"patch","table":"items","id":"i1","fields":{"title":"b"}},
                    {"op":"replace","table":"items","id":"i4","doc":{"projectId":"p1","title":"c"}},
                    {"op":"delete","table":"items","id":"i2"},
                    {"op":"expectVersion","table":"items","id":"i3","version":7},
                    {"op":"expectAbsent","table":"items","index":"by_project_and_title","eq":["p1","dup"]},
                    {"op":"upsert","table":"items","index":"by_project","eq":["p1"],"insert":{"projectId":"p1"},"patch":{"title":"u"}}
                ]
            })
        );
    }

    #[test]
    fn step_result_parses_insert_and_null() {
        let ins: StepResult = serde_json::from_value(json!({"id":"x"})).unwrap();
        assert!(matches!(ins, StepResult::Insert { id } if id == "x"));
        let nul: StepResult = serde_json::from_value(json!(null)).unwrap();
        assert!(matches!(nul, StepResult::Null));
    }

    #[test]
    fn step_result_parses_upsert() {
        let ins: StepResult = serde_json::from_value(json!({"id":"x","inserted":true})).unwrap();
        assert!(matches!(ins, StepResult::Upsert { inserted: true, .. }));
        let pat: StepResult = serde_json::from_value(json!({"id":"x","inserted":false})).unwrap();
        assert!(matches!(
            pat,
            StepResult::Upsert {
                inserted: false,
                ..
            }
        ));
    }

    #[test]
    fn patch_by_query_serializes() {
        let txn = Mutation::new()
            .patch_by_query(
                "items",
                FilterExpr::Eq {
                    field: "status".into(),
                    value: json!("backlog"),
                },
                json!({"status":"done"}),
                None,
            )
            .build();
        // limit omitted on the wire when None.
        assert_eq!(
            serde_json::to_value(&txn).unwrap(),
            json!({
                "steps": [
                    {
                        "op":"patchByQuery",
                        "table":"items",
                        "filter":{"op":"eq","field":"status","value":"backlog"},
                        "patch":{"status":"done"}
                    }
                ]
            })
        );
    }

    #[test]
    fn delete_by_query_serializes_with_limit() {
        let txn = Mutation::new()
            .delete_by_query(
                "items",
                FilterExpr::Eq {
                    field: "status".into(),
                    value: json!("archived"),
                },
                Some(50),
            )
            .build();
        assert_eq!(
            serde_json::to_value(&txn).unwrap(),
            json!({
                "steps": [
                    {
                        "op":"deleteByQuery",
                        "table":"items",
                        "filter":{"op":"eq","field":"status","value":"archived"},
                        "limit":50
                    }
                ]
            })
        );
    }

    #[test]
    fn step_result_parses_patch_and_delete_by_query() {
        let patched: StepResult =
            serde_json::from_value(json!({"patched":3,"truncated":false})).unwrap();
        assert!(matches!(
            patched,
            StepResult::PatchByQuery {
                patched: 3,
                truncated: false
            }
        ));
        let deleted: StepResult =
            serde_json::from_value(json!({"deleted":1000,"truncated":true})).unwrap();
        assert!(matches!(
            deleted,
            StepResult::DeleteByQuery {
                deleted: 1000,
                truncated: true
            }
        ));
    }
}

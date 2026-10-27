//! `GET /admin/db/{db}/schema/json-schema`: the export must validate documents
//! read back from the same db. Uses a small in-test validator covering exactly
//! the keywords the exporter emits.

use rtdb_server::auth::PrincipalCtx;
use rtdb_server::db;
use rtdb_server::ddl;
use rtdb_server::query::{Query, QueryResult, execute_query};
use rtdb_server::schema::SchemaDef;
use rtdb_server::txn::{Transaction, execute_txn};
use serde_json::{Value, json};

use crate::common::{admin_get, spawn_app, test_state};

fn schema_json() -> Value {
    json!({"tables": {
        "owners": {"fields": {"name": {"type": "string"}}},
        "things": {
            "description": "A thing",
            "fieldDescriptions": {"title": "Display title"},
            "fields": {
                "title": {"type": "string"},
                "qty": {"type": "number"},
                "big": {"type": "int64"},
                "on": {"type": "boolean"},
                "kind": {"type": "union", "variants": [
                    {"type": "literal", "value": "a"}, {"type": "literal", "value": "b"}]},
                "owner": {"type": "id", "table": "owners"},
                "blob": {"type": "bytes"},
                "note": {"type": "optional", "inner": {"type": "string"}},
                "tags": {"type": "array", "element": {"type": "string"}},
                "meta": {"type": "object", "fields": {
                    "x": {"type": "number"}, "y": {"type": "optional", "inner": {"type": "string"}}}},
                "scores": {"type": "record", "value": {"type": "number"}},
                "anything": {"type": "any"},
                "emb": {"type": "vector", "dimensions": 3}
            },
            "constraints": {"title": {"minLength": 2, "maxLength": 20}}
        }
    }})
}

fn validate(schema: &Value, root: &Value, value: &Value) -> Result<(), String> {
    if let Some(r) = schema.get("$ref").and_then(Value::as_str) {
        let name = r.trim_start_matches("#/$defs/");
        return validate(&root["$defs"][name], root, value);
    }
    if let Some(c) = schema.get("const")
        && c != value
    {
        return Err(format!("const mismatch: {value}"));
    }
    if let Some(ty) = schema.get("type").and_then(Value::as_str) {
        let ok = match ty {
            "string" => value.is_string(),
            "number" => value.is_number(),
            "integer" => value.is_i64() || value.is_u64(),
            "boolean" => value.is_boolean(),
            "null" => value.is_null(),
            "array" => value.is_array(),
            "object" => value.is_object(),
            _ => false,
        };
        if !ok {
            return Err(format!("expected {ty}, got {value}"));
        }
    }
    if let (Some(p), Some(s)) = (
        schema.get("pattern").and_then(Value::as_str),
        value.as_str(),
    ) {
        let re = regex::Regex::new(p).map_err(|e| e.to_string())?;
        if !re.is_match(s) {
            return Err(format!("{s:?} does not match {p}"));
        }
    }
    if let (Some(n), Some(s)) = (
        schema.get("minLength").and_then(Value::as_u64),
        value.as_str(),
    ) && (s.chars().count() as u64) < n
    {
        return Err("too short".into());
    }
    if let Some(variants) = schema.get("anyOf").and_then(Value::as_array)
        && !variants.iter().any(|v| validate(v, root, value).is_ok())
    {
        return Err(format!("no anyOf member accepts {value}"));
    }
    if let (Some(items), Some(arr)) = (schema.get("items"), value.as_array()) {
        for it in arr {
            validate(items, root, it)?;
        }
        for (k, f) in [("minItems", true), ("maxItems", false)] {
            if let Some(n) = schema.get(k).and_then(Value::as_u64) {
                let len = arr.len() as u64;
                if (f && len < n) || (!f && len > n) {
                    return Err(format!("{k} violated"));
                }
            }
        }
    }
    if let Some(obj) = value.as_object() {
        let props = schema.get("properties").and_then(Value::as_object);
        for r in schema
            .get("required")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let k = r.as_str().unwrap_or_default();
            if !obj.contains_key(k) {
                return Err(format!("missing required {k}"));
            }
        }
        for (k, v) in obj {
            match (
                props.and_then(|p| p.get(k)),
                schema.get("additionalProperties"),
            ) {
                (Some(s), _) => validate(s, root, v)?,
                (None, Some(Value::Bool(false))) => return Err(format!("unexpected {k}")),
                (None, Some(s)) if s.is_object() => validate(s, root, v)?,
                _ => {}
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn export_validates_documents_from_the_same_db() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&state.pool, &name).await?;
    let guard = crate::common::wrap_test_db(name.clone());
    let schema: SchemaDef = serde_json::from_value(schema_json())?;
    ddl::push_schema(&state.pool, &name, schema.clone()).await?;
    let ctx = PrincipalCtx::bypass();

    let txn: Transaction = serde_json::from_value(json!({"steps": [
        {"op": "insert", "table": "owners", "doc": {"name": "o"}}
    ]}))?;
    let out = execute_txn(&state.pool, &name, &schema, &txn, &ctx, None).await?;
    let owner = out.results[0]["id"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let doc = |note: bool| {
        let mut d = json!({
            "title": "hello", "qty": 1.5, "big": "9223372036854775807", "on": true,
            "kind": "b", "owner": owner, "blob": "aGk=", "tags": ["a"],
            "meta": {"x": 1}, "scores": {"k": 2}, "anything": [1, "z"], "emb": [0.1, 0.2, 0.3]
        });
        if note {
            d["note"] = json!("n");
        }
        d
    };
    let txn: Transaction = serde_json::from_value(json!({"steps": [
        {"op": "insert", "table": "things", "doc": doc(true)},
        {"op": "insert", "table": "things", "doc": doc(false)}
    ]}))?;
    execute_txn(&state.pool, &name, &schema, &txn, &ctx, None).await?;

    let resp = admin_get(addr, &format!("/admin/db/{name}/schema/json-schema")).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let export: Value = resp.json().await?;
    assert_eq!(
        export["$schema"],
        "https://json-schema.org/draft/2020-12/schema"
    );
    let things = &export["$defs"]["things"];
    assert_eq!(things["description"], "A thing");
    assert_eq!(
        things["properties"]["title"]["description"],
        "Display title"
    );
    assert_eq!(things["properties"]["big"]["type"], "string");
    assert_eq!(things["properties"]["owner"]["x-rtdb-table"], "owners");
    assert!(
        !things["required"]
            .as_array()
            .unwrap_or(&vec![])
            .contains(&json!("note"))
    );

    let q: Query = serde_json::from_value(json!({"table": "things"}))?;
    let QueryResult::Docs(docs) =
        execute_query(&state.pool, &name, &schema, &q, &ctx, false).await?
    else {
        anyhow::bail!("expected docs");
    };
    assert_eq!(docs.len(), 2);
    for d in &docs {
        validate(things, &export, d).map_err(|e| anyhow::anyhow!("{e} in {d}"))?;
    }
    let owners_q: Query = serde_json::from_value(json!({"table": "owners"}))?;
    let QueryResult::Docs(odocs) =
        execute_query(&state.pool, &name, &schema, &owners_q, &ctx, false).await?
    else {
        anyhow::bail!("expected docs");
    };
    validate(&export["$defs"]["owners"], &export, &odocs[0]).map_err(|e| anyhow::anyhow!(e))?;

    // Negative: an int64 JSON number and a literal outside the union are rejected.
    let mut bad = docs[0].clone();
    bad["big"] = json!(5);
    assert!(validate(things, &export, &bad).is_err());
    let mut bad = docs[0].clone();
    bad["kind"] = json!("zzz");
    assert!(validate(things, &export, &bad).is_err());

    let missing = admin_get(addr, "/admin/db/nope_nope/schema/json-schema").await;
    assert_eq!(missing.status(), reqwest::StatusCode::NOT_FOUND);
    drop(guard);
    Ok(())
}

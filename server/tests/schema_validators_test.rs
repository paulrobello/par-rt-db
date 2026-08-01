mod common;

use common::test_state;
use rtdb_server::db;
use rtdb_server::ddl;
use rtdb_server::error::ErrorCode;
use rtdb_server::query::{Query, QueryResult, execute_query};
use rtdb_server::schema::SchemaDef;
use rtdb_server::txn::{Step, Transaction, execute_txn};

fn doc(value: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
    value.as_object().expect("json object").clone()
}

/// A `widgets` table exercising all four new field types, none indexed.
fn widgets_schema_json() -> serde_json::Value {
    serde_json::json!({"tables":{
      "widgets":{
        "fields":{
          "tags": {"type": "record", "value": {"type": "string"}},
          "payload": {"type": "any"},
          "blob": {"type": "bytes"},
          "big": {"type": "int64"}
        },
        "indexes": []
      }
    }})
}

fn valid_widget_doc() -> serde_json::Map<String, serde_json::Value> {
    doc(serde_json::json!({
        "tags": {"a": "x", "b": "y"},
        "payload": {"anything": ["goes", 1, true, null], "n": 7},
        "blob": "aGVsbG8gd29ybGQ=",
        "big": "9223372036854775807"
    }))
}

async fn fresh_widgets_db(state: &std::sync::Arc<rtdb_server::AppState>) -> (String, SchemaDef) {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&state.pool, &name)
        .await
        .expect("create fresh database");
    let schema: SchemaDef =
        serde_json::from_value(widgets_schema_json()).expect("parse widgets schema fixture");
    let applied = ddl::push_schema(&state.pool, &name, schema)
        .await
        .expect("push widgets schema");
    (name, applied)
}

// (a) DDL generation: none of the four new types get an indexed/typed column.
#[tokio::test]
async fn push_schema_creates_table_with_no_extra_typed_columns() -> anyhow::Result<()> {
    let state = test_state().await;
    let (db_name, _schema) = fresh_widgets_db(&state).await;

    let pg_schema_name = ddl::pg_schema(&db_name);
    let table_ident = ddl::pg_table("widgets");
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2",
    )
    .bind(&pg_schema_name)
    .bind(&table_ident)
    .fetch_all(&state.pool)
    .await?;
    let mut columns: Vec<String> = rows.into_iter().map(|(c,)| c).collect();
    columns.sort();
    assert_eq!(columns, vec!["created_at", "doc", "id", "version"]);
    Ok(())
}

// (b) Indexing an int64 field is accepted at push_schema time and produces a
// typed `bigint` column. (Until 2026-07-28 int64 was in the non-indexable
// bucket alongside Bytes/Any/Record; the Int64→bigint path lifted it out.)
#[tokio::test]
async fn push_schema_accepts_index_over_int64_field() -> anyhow::Result<()> {
    let state = test_state().await;
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&state.pool, &name).await?;

    let mut json = widgets_schema_json();
    json["tables"]["widgets"]["indexes"] =
        serde_json::json!([{"name": "by_big", "fields": ["big"]}]);
    let schema: SchemaDef = serde_json::from_value(json)?;

    ddl::push_schema(&state.pool, &name, schema)
        .await
        .expect("indexing an int64 field must succeed");

    // The bigint column for `big` exists alongside the base columns.
    let pg_schema_name = ddl::pg_schema(&name);
    let table_ident = ddl::pg_table("widgets");
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 ORDER BY column_name",
    )
    .bind(&pg_schema_name)
    .bind(&table_ident)
    .fetch_all(&state.pool)
    .await?;
    let columns: Vec<String> = rows.into_iter().map(|(c,)| c).collect();
    assert_eq!(columns, vec!["created_at", "doc", "f_big", "id", "version"]);
    Ok(())
}

// (c) insert rejects an invalid value for each new type.
#[tokio::test]
async fn insert_rejects_invalid_values_for_each_new_type() -> anyhow::Result<()> {
    let state = test_state().await;
    let (db_name, schema) = fresh_widgets_db(&state).await;

    let cases: [(&str, serde_json::Value); 3] = [
        ("tags", serde_json::json!(["not", "an", "object"])),
        ("blob", serde_json::json!("not base64!")),
        ("big", serde_json::json!("not a number")),
    ];
    for (field, bad_value) in cases {
        let mut fields = valid_widget_doc();
        fields.insert(field.to_string(), bad_value);
        let err = execute_txn(
            &state.pool,
            &db_name,
            &schema,
            &Transaction {
                steps: vec![Step::Insert {
                    table: "widgets".to_string(),
                    doc: fields,
                }],
            },
            None,
        )
        .await
        .expect_err("invalid value must be a schema violation");
        assert_eq!(err.code, ErrorCode::SchemaViolation, "field {field}");
    }
    Ok(())
}

// (d) document round-trips through insert, patch, and query unchanged.
#[tokio::test]
async fn document_round_trips_through_insert_patch_and_query() -> anyhow::Result<()> {
    let state = test_state().await;
    let (db_name, schema) = fresh_widgets_db(&state).await;

    let outcome = execute_txn(
        &state.pool,
        &db_name,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "widgets".to_string(),
                doc: valid_widget_doc(),
            }],
        },
        None,
    )
    .await?;
    let id = outcome.results[0]["id"]
        .as_str()
        .expect("id string")
        .to_string();

    execute_txn(
        &state.pool,
        &db_name,
        &schema,
        &Transaction {
            steps: vec![Step::Patch {
                table: "widgets".to_string(),
                id: id.clone(),
                fields: doc(serde_json::json!({
                    "big": "-9223372036854775808",
                    "payload": {"changed": true}
                })),
            }],
        },
        None,
    )
    .await?;

    let result = execute_query(
        &state.pool,
        &db_name,
        &schema,
        &Query {
            table: "widgets".to_string(),
            get: Some(id.clone()),
            index: None,
            eq: vec![],
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
            first: false,
            count: false,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            aggregate: None,
        },
        None,
    )
    .await?;

    let found = match result {
        QueryResult::Doc(Some(d)) => d,
        other => panic!("expected Doc(Some), got {other:?}"),
    };
    assert_eq!(found["tags"], serde_json::json!({"a": "x", "b": "y"}));
    assert_eq!(found["blob"], serde_json::json!("aGVsbG8gd29ybGQ="));
    assert_eq!(found["big"], serde_json::json!("-9223372036854775808"));
    assert_eq!(found["payload"], serde_json::json!({"changed": true}));
    Ok(())
}

// (k) unique + where (partial) index flags: additive declarations that
// round-trip on the wire and are validated as btree-only.
#[tokio::test]
async fn unique_index_round_trips_and_omits_when_absent() {
    // A plain unique index carries `unique` but not `where`; absent `where` is
    // omitted so a non-unique, non-partial index still round-trips as {"name","fields"} only.
    let json = serde_json::json!({
        "tables": {
            "t": {
                "fields": { "email": { "type": "string" } },
                "indexes": [{ "name": "by_email", "fields": ["email"], "unique": true }]
            }
        }
    });
    let schema: SchemaDef = serde_json::from_value(json.clone()).unwrap();
    let idx = schema.tables["t"].indexes[0].clone();
    assert!(idx.unique);
    assert!(idx.r#where.is_none());
    // Round-trip keeps `unique` and omits the absent `where`.
    let re = serde_json::to_value(&schema).unwrap();
    assert_eq!(re["tables"]["t"]["indexes"][0]["unique"], true);
    assert!(re["tables"]["t"]["indexes"][0].get("where").is_none());
}

#[tokio::test]
async fn partial_unique_index_round_trips() {
    let json = serde_json::json!({
        "tables": {
            "t": {
                "fields": { "slug": { "type": "string" }, "deleted": { "type": "boolean" } },
                "indexes": [{
                    "name": "by_slug", "fields": ["slug"], "unique": true,
                    "where": { "op": "eq", "field": "deleted", "value": false }
                }]
            }
        }
    });
    let schema: SchemaDef = serde_json::from_value(json).unwrap();
    let idx = &schema.tables["t"].indexes[0];
    assert!(idx.unique);
    assert!(idx.r#where.is_some());
}

#[tokio::test]
async fn unique_where_rejected_on_search_index() {
    let json = serde_json::json!({
        "tables": {
            "t": {
                "fields": { "body": { "type": "string" } },
                "indexes": [{ "name": "body", "fields": ["body"], "search": true, "unique": true }]
            }
        }
    });
    let schema: SchemaDef = serde_json::from_value(json).unwrap();
    let err = schema.validate().unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);
}

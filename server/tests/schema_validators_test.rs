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

// (b) Attempting to index any of the four new types is rejected at push_schema time.
#[tokio::test]
async fn push_schema_rejects_index_over_int64_field() -> anyhow::Result<()> {
    let state = test_state().await;
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&state.pool, &name).await?;

    let mut json = widgets_schema_json();
    json["tables"]["widgets"]["indexes"] =
        serde_json::json!([{"name": "by_big", "fields": ["big"]}]);
    let schema: SchemaDef = serde_json::from_value(json)?;

    let err = ddl::push_schema(&state.pool, &name, schema)
        .await
        .expect_err("indexing an int64 field must be rejected");
    assert_eq!(err.code, ErrorCode::SchemaViolation);
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

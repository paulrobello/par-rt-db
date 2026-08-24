use std::collections::BTreeMap;

use crate::schema::*;

pub fn kanban_schema_json() -> serde_json::Value {
    serde_json::json!({"tables":{
      "projects":{
        "fields":{
          "name":{"type":"string"},
          "description":{"type":"optional","inner":{"type":"string"}},
          "status":{"type":"union","variants":[
            {"type":"literal","value":"active"},{"type":"literal","value":"paused"},
            {"type":"literal","value":"dormant"},{"type":"literal","value":"archived"}]},
          "tags":{"type":"array","element":{"type":"string"}},
          "updatedAt":{"type":"number"}},
        "indexes":[{"name":"by_name","fields":["name"]},{"name":"by_status","fields":["status"]}]},
      "workItems":{
        "fields":{
          "projectId":{"type":"id","table":"projects"},
          "title":{"type":"string"},
          "status":{"type":"union","variants":[
            {"type":"literal","value":"backlog"},{"type":"literal","value":"in_progress"},
            {"type":"literal","value":"blocked"},{"type":"literal","value":"done"}]},
          "order":{"type":"number"},
          "completedAt":{"type":"optional","inner":{"type":"number"}}},
        "indexes":[{"name":"by_project","fields":["projectId"]},
                   {"name":"by_status","fields":["status"]},
                   {"name":"by_project_and_status","fields":["projectId","status"]}]}
    }})
}

fn kanban_schema() -> SchemaDef {
    serde_json::from_value(kanban_schema_json()).unwrap()
}
fn work_items_table() -> TableDef {
    kanban_schema().tables.get("workItems").unwrap().clone()
}

fn valid_work_item_doc() -> serde_json::Map<String, serde_json::Value> {
    serde_json::json!({
        "projectId": "0123456789abcdef0123456789abcdef",
        "title": "Do the thing",
        "status": "backlog",
        "order": 1.0,
        "completedAt": null
    })
    .as_object()
    .unwrap()
    .clone()
}

#[test]
fn rejects_index_referencing_unknown_field() {
    let table = TableDef {
        defaults: std::collections::BTreeMap::new(),
        computed: std::collections::BTreeMap::new(),
        fields: BTreeMap::from([("name".to_string(), FieldType::String)]),
        indexes: vec![IndexDef {
            name: "by_missing".to_string(),
            fields: vec!["missing".to_string()],
            search: false,
            vector: None,
            unique: false,
            r#where: None,
            language: None,
        }],
        owner_field: None,
        collaborators_field: None,
        ttl: None,
        updated_at_field: None,
        auto_increment_field: None,
        authorize: None,

        soft_delete: false,
    };
    let schema = SchemaDef {
        tables: BTreeMap::from([("items".to_string(), table)]),
    };
    assert!(schema.validate().is_err());
}

#[test]
fn rejects_literal_with_non_scalar_value() {
    let table = TableDef {
        defaults: std::collections::BTreeMap::new(),
        computed: std::collections::BTreeMap::new(),
        fields: BTreeMap::from([(
            "x".to_string(),
            FieldType::Literal {
                value: serde_json::json!(null),
            },
        )]),
        indexes: vec![],
        owner_field: None,
        collaborators_field: None,
        ttl: None,
        updated_at_field: None,
        auto_increment_field: None,
        authorize: None,

        soft_delete: false,
    };
    let schema = SchemaDef {
        tables: BTreeMap::from([("items".to_string(), table)]),
    };
    assert!(schema.validate().is_err());
}

#[test]
fn rejects_empty_union() {
    let table = TableDef {
        defaults: std::collections::BTreeMap::new(),
        computed: std::collections::BTreeMap::new(),
        fields: BTreeMap::from([("x".to_string(), FieldType::Union { variants: vec![] })]),
        indexes: vec![],
        owner_field: None,
        collaborators_field: None,
        ttl: None,
        updated_at_field: None,
        auto_increment_field: None,
        authorize: None,

        soft_delete: false,
    };
    let schema = SchemaDef {
        tables: BTreeMap::from([("items".to_string(), table)]),
    };
    assert!(schema.validate().is_err());
}

#[test]
fn rejects_optional_wrapping_optional() {
    let table = TableDef {
        defaults: std::collections::BTreeMap::new(),
        computed: std::collections::BTreeMap::new(),
        fields: BTreeMap::from([(
            "x".to_string(),
            FieldType::Optional {
                inner: Box::new(FieldType::Optional {
                    inner: Box::new(FieldType::String),
                }),
            },
        )]),
        indexes: vec![],
        owner_field: None,
        collaborators_field: None,
        ttl: None,
        updated_at_field: None,
        auto_increment_field: None,
        authorize: None,

        soft_delete: false,
    };
    let schema = SchemaDef {
        tables: BTreeMap::from([("items".to_string(), table)]),
    };
    assert!(schema.validate().is_err());
}

// (e) indexed_column_type matrix.
#[test]
fn indexed_column_type_matrix() {
    assert_eq!(
        indexed_column_type(&FieldType::String).unwrap(),
        ("text", false)
    );
    assert_eq!(
        indexed_column_type(&FieldType::Number).unwrap(),
        ("double precision", false)
    );
    assert_eq!(
        indexed_column_type(&FieldType::Int64).unwrap(),
        ("bigint", false)
    );
    assert_eq!(
        indexed_column_type(&FieldType::Boolean).unwrap(),
        ("boolean", false)
    );
    assert_eq!(
        indexed_column_type(&FieldType::Id {
            table: "projects".to_string(),
            on_delete: None,
        })
        .unwrap(),
        ("text", false)
    );
    assert_eq!(
        indexed_column_type(&FieldType::Literal {
            value: serde_json::json!("active")
        })
        .unwrap(),
        ("text", false)
    );
    assert_eq!(
        indexed_column_type(&FieldType::Optional {
            inner: Box::new(FieldType::String)
        })
        .unwrap(),
        ("text", true)
    );
    let union = FieldType::Union {
        variants: vec![
            FieldType::Literal {
                value: serde_json::json!("a"),
            },
            FieldType::Literal {
                value: serde_json::json!("b"),
            },
        ],
    };
    assert_eq!(indexed_column_type(&union).unwrap(), ("text", false));
    assert!(
        indexed_column_type(&FieldType::Array {
            element: Box::new(FieldType::String)
        })
        .is_err()
    );
    assert!(indexed_column_type(&FieldType::Null).is_err());
    assert!(
        indexed_column_type(&FieldType::Literal {
            value: serde_json::json!(1)
        })
        .is_err()
    );
}

// (f) validate_doc: accepts valid doc, rejects unknown/missing/wrong-typed/_-prefixed fields.
#[test]
fn validate_doc_accepts_valid_document() {
    let table = work_items_table();
    assert!(validate_doc(&table, &valid_work_item_doc()).is_ok());
}

#[test]
fn validate_doc_rejects_unknown_field() {
    let table = work_items_table();
    let mut doc = valid_work_item_doc();
    doc.insert("bogus".to_string(), serde_json::json!(true));
    assert!(validate_doc(&table, &doc).is_err());
}

#[test]
fn validate_doc_rejects_missing_required_field() {
    let table = work_items_table();
    let mut doc = valid_work_item_doc();
    doc.remove("title");
    assert!(validate_doc(&table, &doc).is_err());
}

#[test]
fn validate_doc_rejects_wrong_typed_field() {
    let table = work_items_table();
    let mut doc = valid_work_item_doc();
    doc.insert("order".to_string(), serde_json::json!("not a number"));
    assert!(validate_doc(&table, &doc).is_err());
}

#[test]
fn validate_doc_rejects_underscore_prefixed_field() {
    let table = work_items_table();
    let mut doc = valid_work_item_doc();
    doc.insert(
        "_id".to_string(),
        serde_json::json!("0123456789abcdef0123456789abcdef"),
    );
    assert!(validate_doc(&table, &doc).is_err());
}

// (g) Optional field explicit null accepted.
#[test]
fn validate_doc_accepts_explicit_null_for_optional_field() {
    let table = work_items_table();
    let mut doc = valid_work_item_doc();
    doc.insert("completedAt".to_string(), serde_json::Value::Null);
    assert!(validate_doc(&table, &doc).is_ok());
}

#[test]
fn validate_value_optional_accepts_null_and_inner_type() {
    let ty = FieldType::Optional {
        inner: Box::new(FieldType::Number),
    };
    assert!(validate_value(&ty, &serde_json::Value::Null));
    assert!(validate_value(&ty, &serde_json::json!(42)));
    assert!(!validate_value(&ty, &serde_json::json!("nope")));
}

#[test]
fn validate_value_id_requires_32_lowercase_hex_chars() {
    let ty = FieldType::Id {
        table: "projects".to_string(),
        on_delete: None,
    };
    assert!(validate_value(
        &ty,
        &serde_json::json!("0123456789abcdef0123456789abcdef")
    ));
    assert!(!validate_value(
        &ty,
        &serde_json::json!("0123456789ABCDEF0123456789abcdef")
    ));
    assert!(!validate_value(&ty, &serde_json::json!("short")));
}

#[test]
fn schema_table_lookup() {
    let schema = kanban_schema();
    assert!(schema.table("projects").is_ok());
    assert!(schema.table("missing").is_err());
}

#[test]
fn table_index_lookup() {
    let schema = kanban_schema();
    let table = schema.table("projects").unwrap();
    assert!(table.index("by_name").is_ok());
    assert!(table.index("missing").is_err());
}

// Extra validators: record/int64/any/bytes wire tags, structural validation, and
// document/value validation (FEATURE_MATRIX rank 13).
#[test]
fn new_variants_serialize_with_expected_wire_tags() {
    assert_eq!(
        serde_json::to_value(FieldType::Int64).unwrap(),
        serde_json::json!({"type": "int64"})
    );
    assert_eq!(
        serde_json::to_value(FieldType::Bytes).unwrap(),
        serde_json::json!({"type": "bytes"})
    );
    assert_eq!(
        serde_json::to_value(FieldType::Any).unwrap(),
        serde_json::json!({"type": "any"})
    );
    assert_eq!(
        serde_json::to_value(FieldType::Record {
            value: Box::new(FieldType::String)
        })
        .unwrap(),
        serde_json::json!({"type": "record", "value": {"type": "string"}})
    );
}

#[test]
fn record_field_validates_structurally_and_recurses() {
    let table = TableDef {
        defaults: std::collections::BTreeMap::new(),
        computed: std::collections::BTreeMap::new(),
        fields: BTreeMap::from([(
            "meta".to_string(),
            FieldType::Record {
                value: Box::new(FieldType::Number),
            },
        )]),
        indexes: vec![],
        owner_field: None,
        collaborators_field: None,
        ttl: None,
        updated_at_field: None,
        auto_increment_field: None,
        authorize: None,

        soft_delete: false,
    };
    let schema = SchemaDef {
        tables: BTreeMap::from([("items".to_string(), table)]),
    };
    assert!(schema.validate().is_ok());
}

#[test]
fn record_value_validates_every_entry() {
    let ty = FieldType::Record {
        value: Box::new(FieldType::String),
    };
    assert!(validate_value(
        &ty,
        &serde_json::json!({"a": "x", "b": "y"})
    ));
    assert!(validate_value(&ty, &serde_json::json!({})));
    assert!(!validate_value(&ty, &serde_json::json!({"a": 1})));
    assert!(!validate_value(&ty, &serde_json::json!(["a", "b"])));
}

#[test]
fn any_field_accepts_every_json_value() {
    let ty = FieldType::Any;
    assert!(validate_value(&ty, &serde_json::json!("x")));
    assert!(validate_value(&ty, &serde_json::json!(42)));
    assert!(validate_value(&ty, &serde_json::json!(true)));
    assert!(validate_value(&ty, &serde_json::Value::Null));
    assert!(validate_value(&ty, &serde_json::json!([1, "a", null])));
    assert!(validate_value(
        &ty,
        &serde_json::json!({"nested": {"x": 1}})
    ));
}

#[test]
fn bytes_field_validates_standard_padded_base64() {
    let ty = FieldType::Bytes;
    assert!(validate_value(&ty, &serde_json::json!("aGVsbG8gd29ybGQ=")));
    assert!(!validate_value(&ty, &serde_json::json!("not base64!!!")));
    // Missing required padding is rejected under STANDARD (not STANDARD_NO_PAD).
    assert!(!validate_value(&ty, &serde_json::json!("aGVsbG8")));
    assert!(!validate_value(&ty, &serde_json::json!(123)));
}

#[test]
fn int64_field_validates_decimal_string_in_i64_range() {
    let ty = FieldType::Int64;
    assert!(validate_value(&ty, &serde_json::json!("0")));
    assert!(validate_value(&ty, &serde_json::json!("-42")));
    assert!(validate_value(
        &ty,
        &serde_json::json!("9223372036854775807")
    )); // i64::MAX
    assert!(validate_value(
        &ty,
        &serde_json::json!("-9223372036854775808")
    )); // i64::MIN
    assert!(!validate_value(
        &ty,
        &serde_json::json!("9223372036854775808")
    )); // overflow
    assert!(!validate_value(&ty, &serde_json::json!("not a number")));
    assert!(!validate_value(&ty, &serde_json::json!("1.5")));
    assert!(!validate_value(&ty, &serde_json::json!(42))); // must be a string, not a JSON number
}

#[test]
fn indexed_column_type_rejects_new_non_indexable_types() {
    assert!(indexed_column_type(&FieldType::Bytes).is_err());
    assert!(indexed_column_type(&FieldType::Any).is_err());
    assert!(
        indexed_column_type(&FieldType::Record {
            value: Box::new(FieldType::String)
        })
        .is_err()
    );
}

#[test]
fn rejects_index_over_record_field() {
    let table = TableDef {
        defaults: std::collections::BTreeMap::new(),
        computed: std::collections::BTreeMap::new(),
        fields: BTreeMap::from([(
            "meta".to_string(),
            FieldType::Record {
                value: Box::new(FieldType::String),
            },
        )]),
        indexes: vec![IndexDef {
            name: "by_meta".to_string(),
            fields: vec!["meta".to_string()],
            search: false,
            vector: None,
            unique: false,
            r#where: None,
            language: None,
        }],
        owner_field: None,
        collaborators_field: None,
        ttl: None,
        updated_at_field: None,
        auto_increment_field: None,
        authorize: None,

        soft_delete: false,
    };
    let schema = SchemaDef {
        tables: BTreeMap::from([("items".to_string(), table)]),
    };
    assert!(schema.validate().is_err());
}

// (h) full-text search index: additive `search: true` flag on IndexDef.
#[test]
fn search_index_round_trips_and_validates() {
    // A search index is declared by carrying `search: true`; a btree index
    // omits it. Both deserialize from existing wire, and the flag round-trips
    // through serde (btree omits `search`, search keeps `search: true`).
    let schema = serde_json::json!({"tables":{"notes":{
        "fields":{"title":{"type":"string"},"body":{"type":"string"}},
        "indexes":[
            {"name":"by_title","fields":["title"]},
            {"name":"search_content","fields":["title","body"],"search":true}
        ]
    }}});
    let parsed: SchemaDef = serde_json::from_value(schema).unwrap();
    assert!(parsed.validate().is_ok());
    let notes = parsed.tables.get("notes").unwrap();
    let by_title = notes.indexes.iter().find(|i| i.name == "by_title").unwrap();
    let search = notes
        .indexes
        .iter()
        .find(|i| i.name == "search_content")
        .unwrap();
    assert!(!by_title.search);
    assert!(search.search);
    // Btree omits `search` on the wire; the search index keeps it.
    assert_eq!(
        serde_json::to_value(by_title).unwrap(),
        serde_json::json!({"name":"by_title","fields":["title"]})
    );
    assert_eq!(
        serde_json::to_value(search).unwrap(),
        serde_json::json!({"name":"search_content","fields":["title","body"],"search":true})
    );
}

#[test]
fn rejects_search_index_over_non_text_field() {
    let table = TableDef {
        defaults: std::collections::BTreeMap::new(),
        computed: std::collections::BTreeMap::new(),
        fields: BTreeMap::from([("count".to_string(), FieldType::Number)]),
        indexes: vec![IndexDef {
            name: "search_count".to_string(),
            fields: vec!["count".to_string()],
            search: true,
            vector: None,
            unique: false,
            r#where: None,
            language: None,
        }],
        owner_field: None,
        collaborators_field: None,
        ttl: None,
        updated_at_field: None,
        auto_increment_field: None,
        authorize: None,

        soft_delete: false,
    };
    let schema = SchemaDef {
        tables: BTreeMap::from([("items".to_string(), table)]),
    };
    assert!(schema.validate().is_err());
}

#[test]
fn accepts_search_index_over_optional_text_field() {
    let table = TableDef {
        defaults: std::collections::BTreeMap::new(),
        computed: std::collections::BTreeMap::new(),
        fields: BTreeMap::from([(
            "body".to_string(),
            FieldType::Optional {
                inner: Box::new(FieldType::String),
            },
        )]),
        indexes: vec![IndexDef {
            name: "search_body".to_string(),
            fields: vec!["body".to_string()],
            search: true,
            vector: None,
            unique: false,
            r#where: None,
            language: None,
        }],
        owner_field: None,
        collaborators_field: None,
        ttl: None,
        updated_at_field: None,
        auto_increment_field: None,
        authorize: None,

        soft_delete: false,
    };
    let schema = SchemaDef {
        tables: BTreeMap::from([("items".to_string(), table)]),
    };
    assert!(schema.validate().is_ok());
}

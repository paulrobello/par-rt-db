use std::collections::BTreeMap;

use crate::error::RtDbError;
use crate::schema::*;

// (i) Vector field type: wire round-trip, not btree-indexable, and value
// validation requires an array of exactly `dimensions` finite numbers.
#[test]
fn vector_field_type_round_trips() {
    let v = FieldType::Vector { dimensions: 1536 };
    let json = serde_json::to_value(&v).unwrap();
    assert_eq!(json, serde_json::json!({"type":"vector","dimensions":1536}));
    let back: FieldType = serde_json::from_value(json).unwrap();
    assert_eq!(back, v);
}

#[test]
fn vector_is_not_btree_indexable() {
    assert!(indexed_column_type(&FieldType::Vector { dimensions: 3 }).is_err());
}

#[test]
fn vector_validate_accepts_exact_length_finite() {
    let ty = FieldType::Vector { dimensions: 3 };
    assert!(validate_value(&ty, &serde_json::json!([1.0, -2.5, 0.0])));
}

#[test]
fn vector_validate_rejects_wrong_length() {
    let ty = FieldType::Vector { dimensions: 3 };
    assert!(!validate_value(&ty, &serde_json::json!([1.0, 2.0])));
}

#[test]
fn vector_validate_rejects_non_finite() {
    let ty = FieldType::Vector { dimensions: 2 };
    assert!(!validate_value(
        &ty,
        &serde_json::json!([1.0, serde_json::Value::from(f64::NAN)])
    ));
}

// (j) Vector index declaration: `IndexDef.vector` carries dimensions and
// optional scalar filterFields; an ordinary btree index omits `vector`
// entirely on the wire.
#[test]
fn vector_index_round_trips_and_btree_omits_it() {
    let json = serde_json::json!({
        "name": "by_embedding",
        "fields": ["embedding"],
        "vector": {"dimensions": 4, "filterFields": ["userId"]}
    });
    let idx: IndexDef = serde_json::from_value(json.clone()).unwrap();
    assert_eq!(idx.vector.as_ref().unwrap().dimensions, 4);
    assert_eq!(
        idx.vector.as_ref().unwrap().filter_fields,
        vec!["userId".to_string()]
    );
    // round-trips byte-identical
    assert_eq!(serde_json::to_value(&idx).unwrap(), json);

    // a btree index omits `vector` entirely
    let btree: IndexDef =
        serde_json::from_value(serde_json::json!({"name":"by_name","fields":["name"]})).unwrap();
    assert!(btree.vector.is_none());
    assert!(
        serde_json::to_value(&btree)
            .unwrap()
            .get("vector")
            .is_none()
    );
}

#[test]
fn vector_index_rejects_dimension_mismatch() {
    let mut fields = BTreeMap::new();
    fields.insert("embedding".to_string(), FieldType::Vector { dimensions: 4 });
    let table = TableDef {
        defaults: std::collections::BTreeMap::new(),
        computed: std::collections::BTreeMap::new(),
        fields,
        indexes: vec![IndexDef {
            name: "by_emb".to_string(),
            fields: vec!["embedding".to_string()],
            search: false,
            vector: Some(VectorIndexSpec {
                dimensions: 8,
                filter_fields: vec![],
                metric: DistanceMetric::Cosine,
            }),
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
    assert!(table.validate_structure("docs").is_err());
}

#[test]
fn vector_index_accepts_matching_dims_and_filter_fields() {
    let mut fields = BTreeMap::new();
    fields.insert("embedding".to_string(), FieldType::Vector { dimensions: 4 });
    fields.insert("userId".to_string(), FieldType::String);
    let table = TableDef {
        defaults: std::collections::BTreeMap::new(),
        computed: std::collections::BTreeMap::new(),
        fields,
        indexes: vec![IndexDef {
            name: "by_emb".to_string(),
            fields: vec!["embedding".to_string()],
            search: false,
            vector: Some(VectorIndexSpec {
                dimensions: 4,
                filter_fields: vec!["userId".to_string()],
                metric: DistanceMetric::Cosine,
            }),
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
    assert!(table.validate_structure("docs").is_ok());
}

#[test]
fn vector_index_rejects_search_and_vector_both_set() {
    let mut fields = BTreeMap::new();
    fields.insert("embedding".to_string(), FieldType::Vector { dimensions: 4 });
    let table = TableDef {
        defaults: std::collections::BTreeMap::new(),
        computed: std::collections::BTreeMap::new(),
        fields,
        indexes: vec![IndexDef {
            name: "by_emb".to_string(),
            fields: vec!["embedding".to_string()],
            search: true,
            vector: Some(VectorIndexSpec {
                dimensions: 4,
                filter_fields: vec![],
                metric: DistanceMetric::Cosine,
            }),
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
    assert!(table.validate_structure("docs").is_err());
}

// A 0-dimensional vector is meaningless and would produce a degenerate
// index; rejecting it here gives a clear schema error.
#[test]
fn vector_index_rejects_zero_dimensions() {
    let mut fields = BTreeMap::new();
    fields.insert("embedding".to_string(), FieldType::Vector { dimensions: 0 });
    let table = TableDef {
        defaults: std::collections::BTreeMap::new(),
        computed: std::collections::BTreeMap::new(),
        fields,
        indexes: vec![IndexDef {
            name: "by_emb".to_string(),
            fields: vec!["embedding".to_string()],
            search: false,
            vector: Some(VectorIndexSpec {
                dimensions: 0,
                filter_fields: vec![],
                metric: DistanceMetric::Cosine,
            }),
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
    assert!(table.validate_structure("docs").is_err());
}

#[test]
fn vector_index_rejects_two_fields() {
    let mut fields = BTreeMap::new();
    fields.insert("a".to_string(), FieldType::Vector { dimensions: 4 });
    fields.insert("b".to_string(), FieldType::Vector { dimensions: 4 });
    let table = TableDef {
        defaults: std::collections::BTreeMap::new(),
        computed: std::collections::BTreeMap::new(),
        fields,
        indexes: vec![IndexDef {
            name: "by_emb".to_string(),
            fields: vec!["a".to_string(), "b".to_string()],
            search: false,
            vector: Some(VectorIndexSpec {
                dimensions: 4,
                filter_fields: vec![],
                metric: DistanceMetric::Cosine,
            }),
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
    assert!(table.validate_structure("docs").is_err());
}

#[test]
fn vector_index_rejects_non_vector_field() {
    let mut fields = BTreeMap::new();
    fields.insert("title".to_string(), FieldType::String);
    let table = TableDef {
        defaults: std::collections::BTreeMap::new(),
        computed: std::collections::BTreeMap::new(),
        fields,
        indexes: vec![IndexDef {
            name: "by_title".to_string(),
            fields: vec!["title".to_string()],
            search: false,
            vector: Some(VectorIndexSpec {
                dimensions: 4,
                filter_fields: vec![],
                metric: DistanceMetric::Cosine,
            }),
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
    assert!(table.validate_structure("docs").is_err());
}

#[test]
fn vector_index_rejects_unknown_filter_field() {
    let mut fields = BTreeMap::new();
    fields.insert("embedding".to_string(), FieldType::Vector { dimensions: 4 });
    let table = TableDef {
        defaults: std::collections::BTreeMap::new(),
        computed: std::collections::BTreeMap::new(),
        fields,
        indexes: vec![IndexDef {
            name: "by_emb".to_string(),
            fields: vec!["embedding".to_string()],
            search: false,
            vector: Some(VectorIndexSpec {
                dimensions: 4,
                filter_fields: vec!["userId".to_string()],
                metric: DistanceMetric::Cosine,
            }),
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
    assert!(table.validate_structure("docs").is_err());
}

#[test]
fn vector_index_rejects_non_scalar_filter_field() {
    let mut fields = BTreeMap::new();
    fields.insert("embedding".to_string(), FieldType::Vector { dimensions: 4 });
    fields.insert(
        "meta".to_string(),
        FieldType::Object {
            fields: BTreeMap::new(),
        },
    );
    let table = TableDef {
        defaults: std::collections::BTreeMap::new(),
        computed: std::collections::BTreeMap::new(),
        fields,
        indexes: vec![IndexDef {
            name: "by_emb".to_string(),
            fields: vec!["embedding".to_string()],
            search: false,
            vector: Some(VectorIndexSpec {
                dimensions: 4,
                filter_fields: vec!["meta".to_string()],
                metric: DistanceMetric::Cosine,
            }),
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
    assert!(table.validate_structure("docs").is_err());
}

#[test]
fn owner_field_round_trips_and_validates() {
    let json = r#"{"fields":{"title":{"type":"string"},"userId":{"type":"string"}},"indexes":[{"name":"by_user","fields":["userId"]}],"ownerField":"userId"}"#;
    let td: TableDef = serde_json::from_str(json).unwrap();
    assert_eq!(td.owner_field.as_deref(), Some("userId"));
    // camelCase wire key survives a round trip
    let re = serde_json::to_value(&td).unwrap();
    assert_eq!(re["ownerField"], "userId");

    // validates as part of a schema
    let mut tables = std::collections::BTreeMap::new();
    tables.insert("notes".to_string(), td);
    let schema = SchemaDef { tables };
    schema.validate().unwrap();

    // absent ownerField is omitted from the wire and deserializes as None
    let none_json = r#"{"fields":{"title":{"type":"string"}}}"#;
    let td2: TableDef = serde_json::from_str(none_json).unwrap();
    assert!(td2.owner_field.is_none());
    assert!(!serde_json::to_string(&td2).unwrap().contains("ownerField"));
}

#[test]
fn owner_field_validation_rejects_bad_declarations() {
    use crate::error::ErrorCode;

    fn validate_owner(fields_json: &str, owner: &str) -> Result<(), RtDbError> {
        let json = format!(r#"{{"fields":{fields_json},"ownerField":"{owner}"}}"#);
        let td: TableDef = serde_json::from_str(&json).unwrap();
        let mut tables = std::collections::BTreeMap::new();
        tables.insert("t".to_string(), td);
        SchemaDef { tables }.validate()
    }

    // names an undeclared field — schema violation (422), not bad_request (400)
    let err = validate_owner(r#"{"title":{"type":"string"}}"#, "missing").unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);

    // names a non-string field (number) — not string-compatible
    let err = validate_owner(r#"{"num":{"type":"number"}}"#, "num").unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);

    // a plain string field is valid
    validate_owner(r#"{"title":{"type":"string"}}"#, "title").unwrap();

    // Accepted-types matrix: every field type that maps to Postgres "text".
    validate_owner(r#"{"o":{"type":"id","table":"users"}}"#, "o").unwrap();
    validate_owner(r#"{"o":{"type":"literal","value":"admin"}}"#, "o").unwrap();
    validate_owner(
            r#"{"o":{"type":"union","variants":[{"type":"literal","value":"a"},{"type":"literal","value":"b"}]}}"#,
            "o",
        )
        .unwrap();
    validate_owner(
        r#"{"o":{"type":"optional","inner":{"type":"string"}}}"#,
        "o",
    )
    .unwrap();

    // Rejected-types matrix: non-text scalars.
    let err = validate_owner(r#"{"o":{"type":"boolean"}}"#, "o").unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);
}

#[test]
fn collaborators_field_round_trips_and_validates() {
    // `collaboratorsField` is an opt-in, array-of-strings authorization hint:
    // present on the wire (camelCase) when set, omitted entirely when absent,
    // mirroring `owner_field` byte-for-byte. Round-trips alongside `owner_field`.
    let json = r#"{"fields":{"title":{"type":"string"},"userId":{"type":"string"},"collaborators":{"type":"array","element":{"type":"string"}}},"indexes":[{"name":"by_user","fields":["userId"]}],"ownerField":"userId","collaboratorsField":"collaborators"}"#;
    let td: TableDef = serde_json::from_str(json).unwrap();
    assert_eq!(td.owner_field.as_deref(), Some("userId"));
    assert_eq!(td.collaborators_field.as_deref(), Some("collaborators"));
    let re = serde_json::to_value(&td).unwrap();
    assert_eq!(re["ownerField"], "userId");
    assert_eq!(re["collaboratorsField"], "collaborators");

    let mut tables = std::collections::BTreeMap::new();
    tables.insert("notes".to_string(), td);
    SchemaDef { tables }.validate().unwrap();

    // Absent collaboratorsField is omitted from the wire and deserializes as None.
    let none_json = r#"{"fields":{"title":{"type":"string"}}}"#;
    let td2: TableDef = serde_json::from_str(none_json).unwrap();
    assert!(td2.collaborators_field.is_none());
    assert!(
        !serde_json::to_string(&td2)
            .unwrap()
            .contains("collaboratorsField")
    );
}

#[test]
fn collaborators_field_validation_rejects_bad_declarations() {
    use crate::error::ErrorCode;

    fn validate_collab(fields_json: &str, collab: &str) -> Result<(), RtDbError> {
        let json = format!(r#"{{"fields":{fields_json},"collaboratorsField":"{collab}"}}"#);
        let td: TableDef = serde_json::from_str(&json).unwrap();
        let mut tables = std::collections::BTreeMap::new();
        tables.insert("t".to_string(), td);
        SchemaDef { tables }.validate()
    }

    // names an undeclared field — schema violation (422).
    let err = validate_collab(r#"{"title":{"type":"string"}}"#, "missing").unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);

    // names a non-array field (string) — not array-of-strings.
    let err = validate_collab(r#"{"c":{"type":"string"}}"#, "c").unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);

    // names an array-of-numbers field — element type not string-compatible.
    let err =
        validate_collab(r#"{"c":{"type":"array","element":{"type":"number"}}}"#, "c").unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);

    // Accepted-types matrix: array of every string-compatible element type.
    validate_collab(r#"{"c":{"type":"array","element":{"type":"string"}}}"#, "c").unwrap();
    validate_collab(
        r#"{"c":{"type":"array","element":{"type":"id","table":"users"}}}"#,
        "c",
    )
    .unwrap();
    validate_collab(
        r#"{"c":{"type":"array","element":{"type":"literal","value":"admin"}}}"#,
        "c",
    )
    .unwrap();
    validate_collab(
            r#"{"c":{"type":"array","element":{"type":"union","variants":[{"type":"literal","value":"a"},{"type":"literal","value":"b"}]}}}"#,
            "c",
        )
        .unwrap();
    // Optional<Array<String>> admits a nullable collaborators column.
    validate_collab(
        r#"{"c":{"type":"optional","inner":{"type":"array","element":{"type":"string"}}}}"#,
        "c",
    )
    .unwrap();
}

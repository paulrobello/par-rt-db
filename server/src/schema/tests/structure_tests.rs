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

fn simple_table() -> TableDef {
    TableDef {
        defaults: std::collections::BTreeMap::new(),
        computed: std::collections::BTreeMap::new(),
        fields: BTreeMap::from([("name".to_string(), FieldType::String)]),
        indexes: vec![],
        owner_field: None,
        collaborators_field: None,
        ttl: None,
        updated_at_field: None,
        auto_increment_field: None,
        authorize: None,

        soft_delete: false,
    }
}

fn kanban_schema() -> SchemaDef {
    serde_json::from_value(kanban_schema_json()).unwrap()
}

// (a) full kanban-shaped schema deserializes and validate() passes.
#[test]
fn kanban_schema_deserializes_and_validates() {
    let schema = kanban_schema();
    assert!(schema.validate().is_ok());
}

// (b) bad identifiers rejected.
#[test]
fn rejects_table_name_starting_with_digit() {
    let schema = SchemaDef {
        tables: BTreeMap::from([("1bad".to_string(), simple_table())]),
    };
    assert!(schema.validate().is_err());
}

#[test]
fn rejects_field_name_with_invalid_chars() {
    let table = TableDef {
        defaults: std::collections::BTreeMap::new(),
        computed: std::collections::BTreeMap::new(),
        fields: BTreeMap::from([("a-b".to_string(), FieldType::String)]),
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
fn accepts_table_name_at_max_length_30() {
    let name = "a".repeat(30);
    let schema = SchemaDef {
        tables: BTreeMap::from([(name, simple_table())]),
    };
    assert!(schema.validate().is_ok());
}

#[test]
fn rejects_table_name_over_max_length_30() {
    let name = "a".repeat(31);
    let schema = SchemaDef {
        tables: BTreeMap::from([(name, simple_table())]),
    };
    assert!(schema.validate().is_err());
}

#[test]
fn accepts_field_name_at_max_length_60() {
    let field_name = "a".repeat(60);
    let table = TableDef {
        defaults: std::collections::BTreeMap::new(),
        computed: std::collections::BTreeMap::new(),
        fields: BTreeMap::from([(field_name, FieldType::String)]),
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
fn rejects_field_name_over_max_length_60() {
    let field_name = "a".repeat(61);
    let table = TableDef {
        defaults: std::collections::BTreeMap::new(),
        computed: std::collections::BTreeMap::new(),
        fields: BTreeMap::from([(field_name, FieldType::String)]),
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
fn accepts_index_name_at_max_length_30() {
    let index_name = "a".repeat(30);
    let table = TableDef {
        defaults: std::collections::BTreeMap::new(),
        computed: std::collections::BTreeMap::new(),
        fields: BTreeMap::from([("name".to_string(), FieldType::String)]),
        indexes: vec![IndexDef {
            name: index_name,
            fields: vec!["name".to_string()],
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
    assert!(schema.validate().is_ok());
}

#[test]
fn rejects_index_name_over_max_length_30() {
    let index_name = "a".repeat(31);
    let table = TableDef {
        defaults: std::collections::BTreeMap::new(),
        computed: std::collections::BTreeMap::new(),
        fields: BTreeMap::from([("name".to_string(), FieldType::String)]),
        indexes: vec![IndexDef {
            name: index_name,
            fields: vec!["name".to_string()],
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
fn rejects_case_insensitive_field_name_collision() {
    let table = TableDef {
        defaults: std::collections::BTreeMap::new(),
        computed: std::collections::BTreeMap::new(),
        fields: BTreeMap::from([
            ("status".to_string(), FieldType::String),
            ("Status".to_string(), FieldType::String),
        ]),
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
fn rejects_case_insensitive_index_name_collision() {
    let table = TableDef {
        defaults: std::collections::BTreeMap::new(),
        computed: std::collections::BTreeMap::new(),
        fields: BTreeMap::from([("name".to_string(), FieldType::String)]),
        indexes: vec![
            IndexDef {
                name: "by_x".to_string(),
                fields: vec!["name".to_string()],
                search: false,
                vector: None,
                unique: false,
                r#where: None,
                language: None,
            },
            IndexDef {
                name: "By_X".to_string(),
                fields: vec!["name".to_string()],
                search: false,
                vector: None,
                unique: false,
                r#where: None,
                language: None,
            },
        ],
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
fn rejects_field_name_starting_with_underscore() {
    let table = TableDef {
        defaults: std::collections::BTreeMap::new(),
        computed: std::collections::BTreeMap::new(),
        fields: BTreeMap::from([("_secret".to_string(), FieldType::String)]),
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

// (c) case-insensitive table collision rejected.
#[test]
fn rejects_case_insensitive_table_name_collision() {
    let schema = SchemaDef {
        tables: BTreeMap::from([
            ("Items".to_string(), simple_table()),
            ("items".to_string(), simple_table()),
        ]),
    };
    assert!(schema.validate().is_err());
}

// (d) index over Array field rejected by validate().
#[test]
fn rejects_index_over_array_field() {
    let table = TableDef {
        defaults: std::collections::BTreeMap::new(),
        computed: std::collections::BTreeMap::new(),
        fields: BTreeMap::from([(
            "tags".to_string(),
            FieldType::Array {
                element: Box::new(FieldType::String),
            },
        )]),
        indexes: vec![IndexDef {
            name: "by_tags".to_string(),
            fields: vec!["tags".to_string()],
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
fn rejects_index_with_empty_fields() {
    let table = TableDef {
        defaults: std::collections::BTreeMap::new(),
        computed: std::collections::BTreeMap::new(),
        fields: BTreeMap::from([("name".to_string(), FieldType::String)]),
        indexes: vec![IndexDef {
            name: "by_nothing".to_string(),
            fields: vec![],
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
fn rejects_index_with_duplicate_fields() {
    let table = TableDef {
        defaults: std::collections::BTreeMap::new(),
        computed: std::collections::BTreeMap::new(),
        fields: BTreeMap::from([("name".to_string(), FieldType::String)]),
        indexes: vec![IndexDef {
            name: "by_name".to_string(),
            fields: vec!["name".to_string(), "name".to_string()],
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
fn rejects_duplicate_index_names() {
    let table = TableDef {
        defaults: std::collections::BTreeMap::new(),
        computed: std::collections::BTreeMap::new(),
        fields: BTreeMap::from([("name".to_string(), FieldType::String)]),
        indexes: vec![
            IndexDef {
                name: "by_name".to_string(),
                fields: vec!["name".to_string()],
                search: false,
                vector: None,
                unique: false,
                r#where: None,
                language: None,
            },
            IndexDef {
                name: "by_name".to_string(),
                fields: vec!["name".to_string()],
                search: false,
                vector: None,
                unique: false,
                r#where: None,
                language: None,
            },
        ],
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
fn rejects_index_name_with_invalid_chars() {
    let table = TableDef {
        defaults: std::collections::BTreeMap::new(),
        computed: std::collections::BTreeMap::new(),
        fields: BTreeMap::from([("name".to_string(), FieldType::String)]),
        indexes: vec![IndexDef {
            name: "by-name".to_string(),
            fields: vec!["name".to_string()],
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

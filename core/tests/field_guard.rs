#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

use par_rt_db_core::field_guard::{
    FieldGuardError, expectations_match, json_eq, validate_expectations,
};
use par_rt_db_core::mutation::{FieldExpectation, Step};
use par_rt_db_core::schema::TableDef;
use serde_json::{Value, json};

fn table() -> TableDef {
    serde_json::from_value(json!({
        "fields": {
            "owner": {"type": "string"},
            "metadata": {"type": "optional", "inner": {"type": "any"}},
            "typed": {"type": "object", "fields": {"base": {"type": "number"}}},
            "records": {"type": "record", "value": {"type": "object", "fields": {"base": {"type": "number"}}}},
            "variants": {"type": "union", "variants": [{"type": "null"}, {"type": "object", "fields": {"base": {"type": "number"}}}]},
            "array": {"type": "array", "element": {"type": "number"}}
        },
        "indexes": []
    }))
    .unwrap()
}

fn expected(path: &[&str], value: Value) -> FieldExpectation {
    FieldExpectation {
        path: path.iter().map(|key| (*key).to_string()).collect(),
        value,
    }
}

#[test]
fn nested_guards_select_stable_fields_and_compare_whole_selected_values() {
    let doc = json!({
        "owner": "one", "metadata": {"base": 1, "tail": 22, "scope": {"a": 1, "b": [true, null]}},
        "typed": {"base": 1}, "records": {"dynamic.key": {"base": 1}}, "variants": {"base": 1}
    });
    let guards = [
        expected(&["owner"], json!("one")),
        expected(&["metadata", "base"], json!(1.0)),
        expected(&["metadata", "scope"], json!({"b": [true, null], "a": 1.0})),
        expected(&["typed", "base"], json!(1)),
        expected(&["records", "dynamic.key", "base"], json!(1)),
        expected(&["variants", "base"], json!(1)),
    ];
    assert_eq!(validate_expectations(&table(), &guards), Ok(()));
    assert!(expectations_match(doc.as_object().unwrap(), &guards));
    assert!(!expectations_match(
        doc.as_object().unwrap(),
        &[expected(&["metadata", "scope"], json!({"a": 1}))]
    ));
    assert!(!json_eq(&json!({"a": [true]}), &json!({"a": [1]})));
    assert!(!json_eq(&json!([1]), &json!([1, 2])));
}

#[test]
fn missing_null_and_array_indexing_remain_distinct() {
    let doc = json!({"metadata": {"present": null, "array": [1], "stopped": null}});
    assert!(expectations_match(
        doc.as_object().unwrap(),
        &[expected(&["metadata", "present"], Value::Null)]
    ));
    for path in [
        vec!["metadata", "missing"],
        vec!["metadata", "stopped", "base"],
        vec!["metadata", "array", "0"],
    ] {
        assert!(!expectations_match(
            doc.as_object().unwrap(),
            &[expected(&path, Value::Null)]
        ));
    }
    for path in [
        vec!["unknown"],
        vec!["_version"],
        vec!["typed", "unknown"],
        vec!["array", "0"],
    ] {
        assert_eq!(
            validate_expectations(&table(), &[expected(&path, Value::Null)]),
            Err(FieldGuardError::UnknownPath)
        );
    }
}

#[test]
fn guards_reject_empty_duplicate_and_over_budget_arguments() {
    let invalid = |guards: &[FieldExpectation]| {
        assert!(matches!(
            validate_expectations(&table(), guards),
            Err(FieldGuardError::InvalidRequest(_))
        ));
    };
    invalid(&[]);
    invalid(&[expected(&[], Value::Null)]);
    invalid(&[expected(&["metadata", ""], Value::Null)]);
    invalid(&[
        expected(&["owner"], json!("one")),
        expected(&["owner"], json!("two")),
    ]);
    let fields: Vec<_> = (0..64)
        .map(|index| expected(&["metadata", &format!("key{index}")], Value::Null))
        .collect();
    assert_eq!(validate_expectations(&table(), &fields), Ok(()));
    let mut too_many = fields;
    too_many.push(expected(&["metadata", "last"], Value::Null));
    invalid(&too_many);
    let path = ["metadata"; 16];
    assert_eq!(
        validate_expectations(&table(), &[expected(&path, Value::Null)]),
        Ok(())
    );
    invalid(&[expected(&["metadata"; 17], Value::Null)]);
    let key = "é".repeat(128);
    assert_eq!(
        validate_expectations(&table(), &[expected(&["metadata", &key], Value::Null)]),
        Ok(())
    );
    invalid(&[expected(&["metadata", &(key + "é")], Value::Null)]);
    let mut deep = Value::Null;
    for _ in 0..32 {
        deep = json!([deep]);
    }
    assert_eq!(
        validate_expectations(&table(), &[expected(&["metadata"], deep.clone())]),
        Ok(())
    );
    invalid(&[expected(&["metadata"], json!([deep]))]);
    assert_eq!(
        validate_expectations(
            &table(),
            &[expected(&["metadata"], json!(vec![Value::Null; 8191]))]
        ),
        Ok(())
    );
    invalid(&[expected(&["metadata"], json!(vec![Value::Null; 8192]))]);
    assert_eq!(
        validate_expectations(&table(), &[expected(&["owner"], json!("x".repeat(65_531)))]),
        Ok(())
    );
    invalid(&[expected(&["owner"], json!("x".repeat(65_532)))]);
}

#[test]
fn guard_wire_requires_strict_assertion_shapes() {
    let valid = json!({"op": "expectFields", "table": "rooms", "id": "row", "expected": [{"path": ["metadata", "base"], "value": null}]});
    let decoded: Step = serde_json::from_value(valid.clone()).unwrap();
    assert_eq!(serde_json::to_value(decoded).unwrap(), valid);
    let mut extra = valid.clone();
    extra["expected"][0]["extra"] = json!(true);
    assert!(serde_json::from_value::<Step>(extra).is_err());
    let mut missing = valid.clone();
    missing["expected"][0]
        .as_object_mut()
        .unwrap()
        .remove("value");
    assert!(serde_json::from_value::<Step>(missing).is_err());
    let mut extra = valid;
    extra["extra"] = json!(true);
    assert!(serde_json::from_value::<Step>(extra).is_err());
}

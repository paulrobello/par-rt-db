use super::*;

// ---- validate_doc --------------------------------------------------

#[test]
fn validate_doc_rejects_unknown_field() {
    let schema = test_schema();
    let bad = json!({"name": "a", "status": "todo", "order": 1, "bogus": 9});
    let err = validate_doc(items_table(&schema), &bad).unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);
    assert!(err.message.contains("bogus"), "got: {}", err.message);
}

#[test]
fn validate_doc_rejects_reserved_field() {
    let schema = test_schema();
    let bad = json!({"name": "a", "status": "todo", "order": 1, "_id": "x"});
    let err = validate_doc(items_table(&schema), &bad).unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);
    assert!(err.message.contains("_id"), "got: {}", err.message);
}

#[test]
fn validate_doc_rejects_wrong_field_type() {
    // The "invalid field type on a doc is rejected" case from the brief.
    let schema = test_schema();
    let bad = json!({"name": 42, "status": "todo", "order": 1});
    let err = validate_doc(items_table(&schema), &bad).unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);
    assert!(err.message.contains("name"), "got: {}", err.message);
}

#[test]
fn validate_doc_rejects_missing_required_field() {
    let schema = test_schema();
    let bad = json!({"name": "a", "order": 1}); // missing required "status"
    let err = validate_doc(items_table(&schema), &bad).unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);
    assert!(err.message.contains("status"), "got: {}", err.message);
}

#[test]
fn validate_doc_accepts_a_valid_doc_with_optional_absent() {
    let schema = test_schema();
    let good = json!({"name": "a", "status": "todo", "order": 1});
    validate_doc(items_table(&schema), &good).expect("valid doc");
}

#[test]
fn validate_doc_accepts_an_optional_field_set_to_null() {
    // `note` is `Optional<String>`; null is accepted at the doc level
    // because Optional accepts null. `strip_unset_optionals` is what
    // converts it to "absent" for storage.
    let schema = test_schema();
    let good = json!({"name": "a", "status": "todo", "order": 1, "note": null});
    validate_doc(items_table(&schema), &good).expect("valid doc");
}

// ---- strip_unset_optionals ----------------------------------------

#[test]
fn strip_unset_optionals_drops_null_optional_string() {
    // `note: Optional<String>` set to null → key is stripped (the inner
    // String doesn't accept null, so this is "unset").
    let schema = test_schema();
    let doc = json!({"name": "a", "status": "todo", "order": 1, "note": null});
    let stripped = strip_unset_optionals(items_table(&schema), &doc);
    assert_eq!(stripped, json!({"name": "a", "status": "todo", "order": 1}));
}

#[test]
fn strip_unset_optionals_keeps_null_for_optional_that_accepts_null() {
    // `Optional<Null>` does accept null as its inner value, so the key is
    // preserved.
    let schema = Schema::builder()
        .table(
            "t",
            Table::new().field("x", FieldType::optional(FieldType::Null)),
        )
        .build();
    let table = schema.tables.get("t").expect("table present");
    let doc = json!({"x": null});
    let stripped = strip_unset_optionals(table, &doc);
    assert_eq!(stripped, json!({"x": null}));
}

// ---- id/format helpers --------------------------------------------

#[test]
fn is_hex_id_checks_32_lowercase_hex_chars() {
    assert!(is_hex_id(&json!("0123456789abcdef0123456789abcdef")));
    assert!(!is_hex_id(&json!("0123456789ABCDEF0123456789ABCDEF"))); // uppercase
    assert!(!is_hex_id(&json!("0123456789abcdef"))); // too short
    assert!(!is_hex_id(&json!(42)));
    assert!(!is_hex_id(&json!(null)));
}

#[test]
fn is_int64_string_accepts_i64_range_only() {
    assert!(is_int64_string(&json!("0")));
    assert!(is_int64_string(&json!("-1")));
    assert!(is_int64_string(&json!("9223372036854775807"))); // i64::MAX
    assert!(is_int64_string(&json!("-9223372036854775808"))); // i64::MIN
    // Out of i64 range:
    assert!(!is_int64_string(&json!("9223372036854775808")));
    assert!(!is_int64_string(&json!("-9223372036854775809")));
    // Bad shape:
    assert!(!is_int64_string(&json!("1.5")));
    assert!(!is_int64_string(&json!("-")));
    assert!(!is_int64_string(&json!("")));
    assert!(!is_int64_string(&json!(42)));
}

#[test]
fn is_base64_string_matches_the_ts_regex() {
    assert!(is_base64_string(&json!("")));
    assert!(is_base64_string(&json!("ABCD")));
    assert!(is_base64_string(&json!("ABC=")));
    assert!(is_base64_string(&json!("AB==")));
    assert!(is_base64_string(&json!("YWJjZA=="))); // "abcd"
    // Length not a multiple of 4:
    assert!(!is_base64_string(&json!("ABC")));
    // Too much padding:
    assert!(!is_base64_string(&json!("A===")));
    // Bad body char:
    assert!(!is_base64_string(&json!("ABC!")));
    assert!(!is_base64_string(&json!(42)));
}

#[test]
fn validate_value_handles_each_field_type_variant() {
    // A sanity sweep over the variants; full per-variant coverage lives in
    // the schema tests. Here we just confirm routing works.
    assert!(validate_value(&FieldType::String, &json!("hi")));
    assert!(!validate_value(&FieldType::String, &json!(2)));
    assert!(validate_value(&FieldType::Number, &json!(2.5)));
    assert!(validate_value(&FieldType::Boolean, &json!(true)));
    assert!(validate_value(&FieldType::Null, &json!(null)));
    assert!(validate_value(&FieldType::Any, &json!(null)));
    assert!(validate_value(
        &FieldType::Id {
            table: "x".into(),
            on_delete: None,
        },
        &json!("0123456789abcdef0123456789abcdef")
    ));
    assert!(validate_value(
        &FieldType::Literal { value: json!("a") },
        &json!("a")
    ));
    assert!(validate_value(
        &FieldType::Optional {
            inner: Box::new(FieldType::String)
        },
        &json!(null)
    ));
    assert!(validate_value(
        &FieldType::Union {
            variants: vec![FieldType::String, FieldType::Number]
        },
        &json!(2)
    ));
    assert!(validate_value(
        &FieldType::Array {
            element: Box::new(FieldType::Number)
        },
        &json!([1, 2, 3])
    ));
    assert!(validate_value(&FieldType::Int64, &json!("42")));
    assert!(validate_value(&FieldType::Bytes, &json!("YWJjZA==")));
    assert!(validate_value(
        &FieldType::Vector { dimensions: 3 },
        &json!([1.0, 2.0, 3.0])
    ));
}

#[test]
fn canonical_is_key_order_independent() {
    // serde_json's default BTreeMap-backed Map serializes with sorted keys,
    // so canonical(a) == canonical(b) even when the source maps had
    // different insertion order.
    let a = json!({"b": 1, "a": 2});
    let b = json!({"a": 2, "b": 1});
    assert_eq!(canonical(&a), canonical(&b));
}

// ---- mutate: step helpers ----------------------------------------

#[test]
fn apply_patch_merges_fields_and_re_validates_whole_doc() {
    let schema = test_schema();
    let table = items_table(&schema);
    let doc = json!({"name": "a", "status": "todo", "order": 1});
    let fields = json!({"order": 9}).as_object().unwrap().clone();
    let merged = apply_patch(table, &doc, &fields).expect("patch ok");
    assert_eq!(merged["order"], 9);
    assert_eq!(merged["name"], "a", "non-patched fields preserved");
}

#[test]
fn apply_patch_null_on_optional_inner_that_rejects_null_deletes_key() {
    // `note: Optional<String>` + null → key is removed (mirrors
    // strip_unset_optionals' single-representation rule).
    let schema = test_schema();
    let table = items_table(&schema);
    let doc = json!({"name": "a", "status": "todo", "order": 1, "note": "hi"});
    let fields = json!({"note": null}).as_object().unwrap().clone();
    let merged = apply_patch(table, &doc, &fields).expect("patch ok");
    assert!(merged.get("note").is_none(), "note key stripped: {merged}");
}

#[test]
fn apply_patch_rejects_unknown_field() {
    let schema = test_schema();
    let table = items_table(&schema);
    let doc = json!({"name": "a", "status": "todo", "order": 1});
    let fields = json!({"bogus": 1}).as_object().unwrap().clone();
    let err = apply_patch(table, &doc, &fields).unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);
    assert!(err.message.contains("bogus"));
}

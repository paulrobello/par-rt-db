use super::*;

// ---- filter: eval_filter_expr + validate_filter ----------------
//
// Direct unit tests for the filter evaluator + validator, ported verbatim
// from `describe("evalFilterExpr + validateFilter")`
// (`ts-client/tests/in_memory.test.ts:539-653`). These are the cases item C
// fixed in the TS source — E must not regress them.

/// The field set used by the unit tests below — mirrors the TS
/// `new Set(["name", "age", "active", "score", "tags"])`.
fn filter_unit_fields() -> BTreeSet<String> {
    ["name", "age", "active", "score", "tags"]
        .iter()
        .map(|s| s.to_string())
        .collect()
}

// Type-less evaluator wrapper: the unit tests below predate the typed
// int64 arm (ENH-027) and exercise kind-based comparison only.
fn eval(expr: &FilterExpr, doc: &Value) -> bool {
    eval_filter_expr(expr, doc, &std::collections::BTreeMap::new())
}

// Declared field map for the typed int64 comparison tests (ENH-027):
// the parity fix against the server's typed bigint column path.
fn int64_typed_fields() -> std::collections::BTreeMap<String, crate::schema::FieldType> {
    [
        ("n", crate::schema::FieldType::Int64),
        (
            "opt_n",
            crate::schema::FieldType::Optional {
                inner: Box::new(crate::schema::FieldType::Int64),
            },
        ),
        ("name", crate::schema::FieldType::String),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect()
}

#[test]
fn eval_filter_eq_neq_on_strings_compare_the_doc_field_text() {
    let fields = filter_unit_fields();
    validate_filter(
        &FilterExpr::Eq {
            field: "name".into(),
            value: json!("ada"),
        },
        &fields,
    )
    .expect("valid");
    assert!(eval(
        &FilterExpr::Eq {
            field: "name".into(),
            value: json!("ada"),
        },
        &json!({"name": "ada"}),
    ));
    assert!(!eval(
        &FilterExpr::Eq {
            field: "name".into(),
            value: json!("ada"),
        },
        &json!({"name": "bob"}),
    ));
    assert!(eval(
        &FilterExpr::Neq {
            field: "name".into(),
            value: json!("ada"),
        },
        &json!({"name": "bob"}),
    ));
}

#[test]
fn eval_filter_number_domain_compares_numerically() {
    // gt/gte/lt/lte over a numeric doc field.
    assert!(eval(
        &FilterExpr::Gt {
            field: "age".into(),
            value: json!(30),
        },
        &json!({"age": 42}),
    ));
    assert!(!eval(
        &FilterExpr::Gt {
            field: "age".into(),
            value: json!(50),
        },
        &json!({"age": 42}),
    ));
    assert!(eval(
        &FilterExpr::Lte {
            field: "age".into(),
            value: json!(42),
        },
        &json!({"age": 42}),
    ));
}

#[test]
fn eval_filter_string_ordering_is_lexicographic() {
    assert!(eval(
        &FilterExpr::Lt {
            field: "name".into(),
            value: json!("b"),
        },
        &json!({"name": "ada"}),
    ));
    assert!(eval(
        &FilterExpr::Gte {
            field: "name".into(),
            value: json!("a"),
        },
        &json!({"name": "ada"}),
    ));
}

#[test]
fn eval_filter_boolean_domain_compares_booleans() {
    assert!(eval(
        &FilterExpr::Eq {
            field: "active".into(),
            value: json!(true),
        },
        &json!({"active": true}),
    ));
    assert!(!eval(
        &FilterExpr::Eq {
            field: "active".into(),
            value: json!(true),
        },
        &json!({"active": false}),
    ));
}

#[test]
fn eval_filter_number_value_matches_a_numeric_string_field() {
    // float8 cast: doc field is the string "5", filter value is the number
    // 5 → match. Mirrors Postgres `(doc->>'field')::float8 = 5`.
    assert!(eval(
        &FilterExpr::Eq {
            field: "score".into(),
            value: json!(5),
        },
        &json!({"score": "5"}),
    ));
}

#[test]
fn eval_filter_null_or_absent_doc_field_never_matches() {
    // SQL NULL exclusion: null/absent never matches any op (even neq).
    assert!(!eval(
        &FilterExpr::Eq {
            field: "name".into(),
            value: json!("ada"),
        },
        &json!({"name": null}),
    ));
    assert!(!eval(
        &FilterExpr::Eq {
            field: "name".into(),
            value: json!("ada"),
        },
        &json!({}),
    ));
    assert!(!eval(
        &FilterExpr::Neq {
            field: "name".into(),
            value: json!("ada"),
        },
        &json!({}),
    ));
}

#[test]
fn eval_filter_and_or_nest_recursively() {
    let expr = FilterExpr::And {
        exprs: vec![
            FilterExpr::Gte {
                field: "age".into(),
                value: json!(30),
            },
            FilterExpr::Or {
                exprs: vec![
                    FilterExpr::Eq {
                        field: "name".into(),
                        value: json!("ada"),
                    },
                    FilterExpr::Eq {
                        field: "name".into(),
                        value: json!("bob"),
                    },
                ],
            },
        ],
    };
    assert!(eval(&expr, &json!({"age": 42, "name": "ada"})));
    assert!(!eval(&expr, &json!({"age": 42, "name": "zed"})));
    assert!(!eval(&expr, &json!({"age": 10, "name": "ada"})));
}

#[test]
fn eval_filter_in_matches_membership() {
    assert!(eval(
        &FilterExpr::In {
            field: "name".into(),
            values: vec![json!("ada"), json!("bob")],
        },
        &json!({"name": "bob"}),
    ));
    assert!(!eval(
        &FilterExpr::In {
            field: "name".into(),
            values: vec![json!("ada"), json!("bob")],
        },
        &json!({"name": "zed"}),
    ));
}

#[test]
fn validate_filter_rejects_an_unknown_field() {
    let fields = filter_unit_fields();
    let err = validate_filter(
        &FilterExpr::Eq {
            field: "missing".into(),
            value: json!("x"),
        },
        &fields,
    )
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("unknown field"), "got: {err}");
}

#[test]
fn validate_filter_rejects_empty_and_or_and_empty_in() {
    let fields = filter_unit_fields();
    let err = validate_filter(&FilterExpr::And { exprs: vec![] }, &fields).unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("at least one expr"), "got: {err}");

    let err = validate_filter(&FilterExpr::Or { exprs: vec![] }, &fields).unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("at least one expr"), "got: {err}");

    let err = validate_filter(
        &FilterExpr::In {
            field: "name".into(),
            values: vec![],
        },
        &fields,
    )
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("at least one value"), "got: {err}");
}

#[test]
fn validate_filter_rejects_a_non_string_number_boolean_value() {
    let fields = filter_unit_fields();
    let err = validate_filter(
        &FilterExpr::Eq {
            field: "name".into(),
            value: Value::Null,
        },
        &fields,
    )
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message.contains("string, number, or boolean"),
        "got: {err}"
    );

    let err = validate_filter(
        &FilterExpr::Eq {
            field: "tags".into(),
            value: json!(["a"]),
        },
        &fields,
    )
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message.contains("string, number, or boolean"),
        "got: {err}"
    );
}

#[test]
fn validate_filter_accepts_a_well_formed_nested_filter() {
    let fields = filter_unit_fields();
    validate_filter(
        &FilterExpr::And {
            exprs: vec![
                FilterExpr::Eq {
                    field: "name".into(),
                    value: json!("ada"),
                },
                FilterExpr::In {
                    field: "age".into(),
                    values: vec![json!(1), json!(2)],
                },
            ],
        },
        &fields,
    )
    .expect("well-formed nested filter");
}

#[test]
fn validate_filter_rejects_mixed_type_in_values() {
    let fields = filter_unit_fields();
    let err = validate_filter(
        &FilterExpr::In {
            field: "age".into(),
            values: vec![json!(5), json!("ada")],
        },
        &fields,
    )
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("same type"), "got: {err}");
}

#[test]
fn validate_filter_accepts_same_type_in_values() {
    let fields = filter_unit_fields();
    validate_filter(
        &FilterExpr::In {
            field: "age".into(),
            values: vec![json!(5), json!(6), json!(7)],
        },
        &fields,
    )
    .expect("same-type in values");
}

// ---- query: filter end-to-end ----------------------------------
//
// Ports `describe("InMemoryRtDbClient filter")`
// (`ts-client/tests/in_memory.test.ts:655-756`) — exercises the typed
// `TableQuery.filter(...)` builder end-to-end through `run_query`, the
// same surface live app code uses.

/// Self-contained `users` schema so this block doesn't perturb the shared
/// `items` harness above. Mirrors the TS `usersSchema`.
fn users_schema() -> SchemaDef {
    Schema::builder()
        .table(
            "users",
            Table::new()
                .field("name", FieldType::String)
                .field("age", FieldType::Number)
                .field("active", FieldType::Boolean)
                .index("by_name", &["name"]),
        )
        .build()
}

fn new_users_client() -> InMemoryRtDbClient {
    let counter = Arc::new(Mutex::new(1_700_000_000_000_i64));
    let mut client = InMemoryRtDbClient::new(
        InMemoryRtDbClientOptions::default()
            .now(move || {
                let mut g = counter.lock().expect("counter not poisoned");
                let v = *g;
                *g += 1;
                v
            })
            .random(|| 0.0),
    );
    client.push_schema(&users_schema()).unwrap();
    client
}

async fn seed_users(c: &mut InMemoryRtDbClient) {
    for (name, age, active) in [("ada", 42_i64, true), ("bob", 17, false), ("cy", 65, true)] {
        c.mutate(
            &Mutation::new()
                .insert("users", json!({"name": name, "age": age, "active": active}))
                .build(),
            None,
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn query_filter_reduces_the_result_set_to_matching_docs() {
    let mut c = new_users_client();
    seed_users(&mut c).await;
    let docs = c
        .run::<Vec<Value>>(
            &TableQuery::new("users")
                .filter(FilterExpr::Gt {
                    field: "age".into(),
                    value: json!(20),
                })
                .collect(),
        )
        .expect("filter query ok");
    let mut names: Vec<String> = docs
        .iter()
        .map(|d| d["name"].as_str().unwrap().to_string())
        .collect();
    names.sort();
    assert_eq!(names, vec!["ada".to_string(), "cy".to_string()]);
}

#[tokio::test]
async fn query_filter_composes_with_an_index_eq_prefix_and_take() {
    let mut c = new_users_client();
    seed_users(&mut c).await;
    let docs = c
        .run::<Vec<Value>>(
            &TableQuery::new("users")
                .with_index("by_name", &[json!("ada")])
                .filter(FilterExpr::Eq {
                    field: "active".into(),
                    value: json!(true),
                })
                .take(10),
        )
        .expect("filter+index ok");
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0]["name"], json!("ada"));
}

#[tokio::test]
async fn query_and_or_in_filter_evaluates_correctly_end_to_end() {
    let mut c = new_users_client();
    seed_users(&mut c).await;

    let docs = c
        .run::<Vec<Value>>(
            &TableQuery::new("users")
                .filter(FilterExpr::Or {
                    exprs: vec![
                        FilterExpr::Lt {
                            field: "age".into(),
                            value: json!(18),
                        },
                        FilterExpr::Gte {
                            field: "age".into(),
                            value: json!(65),
                        },
                    ],
                })
                .collect(),
        )
        .expect("or filter ok");
    let mut names: Vec<String> = docs
        .iter()
        .map(|d| d["name"].as_str().unwrap().to_string())
        .collect();
    names.sort();
    assert_eq!(names, vec!["bob".to_string(), "cy".to_string()]);

    let in_docs = c
        .run::<Vec<Value>>(
            &TableQuery::new("users")
                .filter(FilterExpr::In {
                    field: "name".into(),
                    values: vec![json!("ada"), json!("cy")],
                })
                .collect(),
        )
        .expect("in filter ok");
    let mut names: Vec<String> = in_docs
        .iter()
        .map(|d| d["name"].as_str().unwrap().to_string())
        .collect();
    names.sort();
    assert_eq!(names, vec!["ada".to_string(), "cy".to_string()]);
}

#[tokio::test]
async fn query_filter_unknown_field_throws_bad_request() {
    let mut c = new_users_client();
    seed_users(&mut c).await;
    let err = c
        .run_query(
            &TableQuery::new("users")
                .filter(FilterExpr::Eq {
                    field: "nope".into(),
                    value: json!("x"),
                })
                .collect(),
        )
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
}

#[tokio::test]
async fn query_filter_combined_with_get_is_rejected() {
    // Mirrors the server: `get` is exclusive of `filter` (and everything
    // else); the get-exclusivity guard fires before filter validation.
    let mut c = new_users_client();
    let r = c
        .mutate(
            &Mutation::new()
                .insert("users", json!({"name": "ada", "age": 42, "active": true}))
                .build(),
            None,
        )
        .await
        .unwrap();
    let id = match &r[0] {
        StepResult::Insert { id } => id.clone(),
        _ => unreachable!(),
    };
    let err = c
        .run_query(&Query {
            table: "users".into(),
            get: Some(id),
            filter: Some(FilterExpr::Eq {
                field: "age".into(),
                value: json!(42),
            }),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
}

// ---- filter: typed int64 comparison (ENH-027 parity fix) ----------------
//
// Found by `server/tests/proptest_parity.rs`: a string filter value on a
// declared `int64` field is the server's typed-`bigint` wire form and must
// order NUMERICALLY. The pre-fix evaluator compared the decimal strings
// lexicographically ("15" < "9"), diverging from Postgres on every
// digit-count-variant pair (the proptest counterexample was `not(gt "-1")`
// dropping a row with `-605`, which the server keeps).

#[test]
fn int64_string_filter_values_order_numerically() {
    let fields = int64_typed_fields();
    // The minimized proptest counterexample: "-605" > "-1" lexicographically
    // ('6' sorts after '1') but -605 > -1 is false numerically — the
    // server keeps this row, the pre-fix engine dropped it.
    assert!(!eval_filter_expr(
        &FilterExpr::Gt {
            field: "n".into(),
            value: json!("-1"),
        },
        &json!({"n": "-605"}),
        &fields,
    ));
    // 15 > 9 numerically; lexicographically "15" < "9".
    assert!(eval_filter_expr(
        &FilterExpr::Gt {
            field: "n".into(),
            value: json!("9"),
        },
        &json!({"n": "15"}),
        &fields,
    ));
    assert!(eval_filter_expr(
        &FilterExpr::Lt {
            field: "n".into(),
            value: json!("100"),
        },
        &json!({"n": "99"}),
        &fields,
    ));
    // Boundaries compare exactly (i64::MAX is not f64-exact).
    assert!(eval_filter_expr(
        &FilterExpr::Lte {
            field: "n".into(),
            value: json!("9223372036854775807"),
        },
        &json!({"n": "9223372036854775806"}),
        &fields,
    ));
    assert!(eval_filter_expr(
        &FilterExpr::Gte {
            field: "n".into(),
            value: json!("-9223372036854775808"),
        },
        &json!({"n": "-9223372036854775807"}),
        &fields,
    ));
    assert!(!eval_filter_expr(
        &FilterExpr::Gte {
            field: "n".into(),
            value: json!("9223372036854775807"),
        },
        &json!({"n": "9223372036854775806"}),
        &fields,
    ));
    // eq on canonical decimal strings still matches exactly.
    assert!(eval_filter_expr(
        &FilterExpr::Eq {
            field: "n".into(),
            value: json!("42"),
        },
        &json!({"n": "42"}),
        &fields,
    ));
}

#[test]
fn int64_typed_comparison_unwraps_optional_and_keeps_null_exclusion() {
    let fields = int64_typed_fields();
    // optional<int64> unwraps to the same typed comparison.
    assert!(eval_filter_expr(
        &FilterExpr::Gt {
            field: "opt_n".into(),
            value: json!("9"),
        },
        &json!({"opt_n": "15"}),
        &fields,
    ));
    // null/absent doc fields never match (SQL NULL exclusion) — unchanged.
    assert!(!eval_filter_expr(
        &FilterExpr::Gt {
            field: "opt_n".into(),
            value: json!("9"),
        },
        &json!({"other": "15"}),
        &fields,
    ));
    assert!(!eval_filter_expr(
        &FilterExpr::Gt {
            field: "opt_n".into(),
            value: json!("9"),
        },
        &json!({"opt_n": null}),
        &fields,
    ));
    // `in` goes through the same leaf path, so it inherits the fix.
    assert!(eval_filter_expr(
        &FilterExpr::In {
            field: "n".into(),
            values: vec![json!("9"), json!("15")],
        },
        &json!({"n": "15"}),
        &fields,
    ));
}

#[test]
fn int64_number_filter_values_still_compare_as_float8() {
    // The jsonb-path wire form (non-indexed int64 field): a NUMBER value
    // compares via `(doc->>'f')::float8` on the server — unchanged behavior.
    let fields = int64_typed_fields();
    assert!(eval_filter_expr(
        &FilterExpr::Gt {
            field: "n".into(),
            value: json!(0),
        },
        &json!({"n": "15"}),
        &fields,
    ));
    assert!(!eval_filter_expr(
        &FilterExpr::Lt {
            field: "n".into(),
            value: json!(9),
        },
        &json!({"n": "15"}),
        &fields,
    ));
}

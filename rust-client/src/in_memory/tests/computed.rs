//! ENH-028 computed fields: the `ValueExpr` interpreter (semantics-table
//! edges), write-path stamping through the engine's public surface, push
//! validation (the six `validate_computed` rules, `BAD_REQUEST`), and the
//! migrate interplay (rename rewrite, dropField rejection, changeType
//! re-validation — admin builds only, where the migrate surface lives).

use super::*;
use crate::value_expr::{Cast, ValueExpr};

/// The corpus fixture shape: `fullName = concat(first, " ", last)`.
fn users_schema() -> SchemaDef {
    Schema::builder()
        .table(
            "users",
            Table::new()
                .field("first", FieldType::String)
                .field("last", FieldType::String)
                .field("fullName", FieldType::String)
                .index("by_fullName", &["fullName"])
                .computed(
                    "fullName",
                    ValueExpr::concat([
                        ValueExpr::field("first"),
                        ValueExpr::literal(" "),
                        ValueExpr::field("last"),
                    ]),
                ),
        )
        .build()
}

/// A bare deterministic client (unlike [`new_client`], no `test_schema` is
/// pre-pushed — pushing a schema without `items` over it would read as a
/// removed table).
fn bare_client() -> InMemoryRtDbClient {
    let counter = Arc::new(Mutex::new(1_700_000_000_000_i64));
    InMemoryRtDbClient::new(
        InMemoryRtDbClientOptions::default()
            .now(move || {
                let mut g = counter.lock().expect("counter not poisoned");
                let v = *g;
                *g += 1;
                v
            })
            .random(|| 0.0),
    )
}

/// A bare client over `schema` (pushed; a rejection panics).
fn computed_client(schema: &SchemaDef) -> InMemoryRtDbClient {
    let mut client = bare_client();
    client.push_schema(schema).unwrap();
    client
}

// ---- interpreter: the semantics table's edges ---------------------------

fn eval(ve: &ValueExpr, doc: &Map<String, Value>) -> Result<Value, RtDbError> {
    eval_value_expr(ve, doc, 0, &BTreeMap::new())
}

fn doc(pairs: &[(&str, Value)]) -> Map<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

#[test]
fn field_reads_are_text_and_absent_is_null() {
    let d = doc(&[
        ("s", json!("x")),
        ("n", json!(42)),
        ("f", json!(42.5)),
        ("b", json!(true)),
        ("o", json!({"a": 1})),
        ("nil", json!(null)),
    ]);
    assert_eq!(eval(&ValueExpr::field("s"), &d).unwrap(), json!("x"));
    assert_eq!(eval(&ValueExpr::field("n"), &d).unwrap(), json!("42"));
    assert_eq!(eval(&ValueExpr::field("f"), &d).unwrap(), json!("42.5"));
    assert_eq!(eval(&ValueExpr::field("b"), &d).unwrap(), json!("true"));
    // Objects use COMPACT JSON text — the convention pinned for all five
    // implementations (not Postgres's spaced jsonb text).
    assert_eq!(
        eval(&ValueExpr::field("o"), &d).unwrap(),
        json!("{\"a\":1}")
    );
    assert_eq!(eval(&ValueExpr::field("nil"), &d).unwrap(), json!(null));
    assert_eq!(eval(&ValueExpr::field("missing"), &d).unwrap(), json!(null));
}

#[test]
fn concat_skips_nulls_and_casts_numbers_to_text() {
    let d = doc(&[("first", json!("Ada")), ("n", json!(42))]);
    let expr = ValueExpr::concat([
        ValueExpr::field("first"),
        ValueExpr::field("missing"),
        ValueExpr::field("n"),
    ]);
    assert_eq!(eval(&expr, &d).unwrap(), json!("Ada42"));
}

#[test]
fn concat_all_null_parts_is_empty_string() {
    let expr = ValueExpr::concat([ValueExpr::field("missing"), ValueExpr::literal(json!(null))]);
    assert_eq!(eval(&expr, &doc(&[])).unwrap(), json!(""));
}

#[test]
fn add_coerces_string_fields_to_numeric() {
    // Arithmetic runs on f64, so the result is a float JSON number (43.0,
    // not 43) — serde_json numbers are representation-sensitive.
    let d = doc(&[("a", json!("42")), ("b", json!("1"))]);
    let expr = ValueExpr::add(ValueExpr::field("a"), ValueExpr::field("b"));
    assert_eq!(eval(&expr, &d).unwrap(), json!(43.0));
}

#[test]
fn arithmetic_propagates_null_over_operands() {
    let missing = || ValueExpr::field("missing");
    let one = || ValueExpr::literal(1);
    let exprs = [
        ValueExpr::add(missing(), one()),
        ValueExpr::sub(one(), missing()),
        ValueExpr::mul(missing(), one()),
        ValueExpr::div(one(), missing()),
    ];
    for e in &exprs {
        assert_eq!(eval(e, &doc(&[])).unwrap(), json!(null));
    }
    // Null precedes the zero check: null / 0 is null, not an error.
    let null_div_zero = ValueExpr::div(missing(), ValueExpr::literal(0));
    assert_eq!(eval(&null_div_zero, &doc(&[])).unwrap(), json!(null));
}

#[test]
fn div_by_zero_errors() {
    let expr = ValueExpr::div(ValueExpr::literal(1), ValueExpr::literal(0));
    assert_eq!(
        eval(&expr, &doc(&[])).unwrap_err().message,
        "division by zero"
    );
    // -0.0 is the same IEEE zero.
    let neg_zero = ValueExpr::div(ValueExpr::literal(1), ValueExpr::literal(-0.0));
    assert_eq!(
        eval(&neg_zero, &doc(&[])).unwrap_err().message,
        "division by zero"
    );
}

#[test]
fn div_non_finite_result_errors() {
    let expr = ValueExpr::div(ValueExpr::literal(1e308), ValueExpr::literal(1e-10));
    assert_eq!(
        eval(&expr, &doc(&[])).unwrap_err().message,
        "numeric result is not finite"
    );
}

#[test]
fn coalesce_returns_first_non_null_else_null() {
    let first_missing = ValueExpr::coalesce([ValueExpr::field("missing"), ValueExpr::literal(7)]);
    assert_eq!(eval(&first_missing, &doc(&[])).unwrap(), json!(7));
    let all_missing = ValueExpr::coalesce([ValueExpr::field("a"), ValueExpr::field("b")]);
    assert_eq!(eval(&all_missing, &doc(&[])).unwrap(), json!(null));
}

#[test]
fn lower_upper_trim() {
    let d = doc(&[
        ("mixed", json!("MiXeD")),
        ("padded", json!("  x  ")),
        ("tabbed", json!("  \tx  ")),
    ]);
    let lower = ValueExpr::lower(ValueExpr::field("mixed"));
    assert_eq!(eval(&lower, &d).unwrap(), json!("mixed"));
    let upper = ValueExpr::upper(ValueExpr::field("mixed"));
    assert_eq!(eval(&upper, &d).unwrap(), json!("MIXED"));
    let trim = ValueExpr::trim(ValueExpr::field("padded"));
    assert_eq!(eval(&trim, &d).unwrap(), json!("x"));
    // Spaces only — the tab survives btrim's default.
    let tabbed = ValueExpr::trim(ValueExpr::field("tabbed"));
    assert_eq!(eval(&tabbed, &d).unwrap(), json!("\tx"));
    let lower_null = ValueExpr::lower(ValueExpr::field("missing"));
    assert_eq!(eval(&lower_null, &d).unwrap(), json!(null));
}

#[test]
fn cast_error_paths_and_nulls() {
    let d = doc(&[
        ("bad", json!("abc")),
        ("b", json!(true)),
        ("float", json!(3.5)),
        ("two", json!(2)),
        ("maybe", json!("maybe")),
    ]);
    // ToNumber: unparseable string errors; a bool LITERAL is a type error.
    let to_num = |v: ValueExpr| ValueExpr::cast(v, Cast::ToNumber);
    assert!(eval(&to_num(ValueExpr::field("bad")), &d).is_err());
    assert_eq!(
        eval(&to_num(ValueExpr::literal(true)), &d)
            .unwrap_err()
            .message,
        "cannot cast to number"
    );
    // ToInt64: a non-integral number errors; a trimmed string parses.
    let to_i64 = |v: ValueExpr| ValueExpr::cast(v, Cast::ToInt64);
    assert!(eval(&to_i64(ValueExpr::field("float")), &d).is_err());
    assert_eq!(
        eval(&to_i64(ValueExpr::literal(json!("  7 "))), &d).unwrap(),
        json!(7)
    );
    assert_eq!(
        eval(&to_i64(ValueExpr::literal(true)), &d)
            .unwrap_err()
            .message,
        "cannot cast to int64"
    );
    // ToBoolean: the Postgres literal word set, case-insensitive; a number
    // other than 1/0 errors; a bad word errors.
    let to_bool = |v: ValueExpr| ValueExpr::cast(v, Cast::ToBoolean);
    for (word, want) in [
        (json!("TRUE"), true),
        (json!("t"), true),
        (json!("Yes"), true),
        (json!("on"), true),
        (json!("1"), true),
        (json!("False"), false),
        (json!("f"), false),
        (json!("No"), false),
        (json!("OFF"), false),
        (json!("0"), false),
    ] {
        assert_eq!(
            eval(&to_bool(ValueExpr::literal(word.clone())), &d).unwrap(),
            json!(want),
            "word {word}"
        );
    }
    assert!(eval(&to_bool(ValueExpr::field("maybe")), &d).is_err());
    assert!(eval(&to_bool(ValueExpr::field("two")), &d).is_err());
    // Every cast propagates null.
    for to in [
        Cast::ToString,
        Cast::ToNumber,
        Cast::ToInt64,
        Cast::ToBoolean,
    ] {
        assert_eq!(
            eval(&ValueExpr::cast(ValueExpr::field("missing"), to), &d).unwrap(),
            json!(null)
        );
    }
}

#[test]
fn now_yields_epoch_ms_as_number() {
    let d = doc(&[]);
    assert_eq!(
        eval_value_expr(&ValueExpr::now(), &d, 1234567890, &BTreeMap::new()).unwrap(),
        json!(1234567890)
    );
}

#[test]
fn case_takes_first_match_then_otherwise() {
    // The `when` predicates run through the engine's FilterExpr matcher.
    let d = doc(&[("status", json!("admin")), ("n", json!(5))]);
    let whens = vec![
        crate::value_expr::CaseWhen {
            when: FilterExpr::Eq {
                field: "status".into(),
                value: json!("user"),
            },
            then: ValueExpr::literal(1),
        },
        crate::value_expr::CaseWhen {
            when: FilterExpr::Eq {
                field: "status".into(),
                value: json!("admin"),
            },
            then: ValueExpr::literal(2),
        },
    ];
    let matched = ValueExpr::case(whens, ValueExpr::literal(4));
    assert_eq!(eval(&matched, &d).unwrap(), json!(2));

    let unmatched = ValueExpr::case(
        vec![crate::value_expr::CaseWhen {
            when: FilterExpr::Gt {
                field: "n".into(),
                value: json!(10),
            },
            then: ValueExpr::literal(3),
        }],
        ValueExpr::field("status"),
    );
    assert_eq!(eval(&unmatched, &d).unwrap(), json!("admin"));
}

// ---- write path: stamping through the public surface --------------------

#[tokio::test]
async fn insert_overwrites_client_supplied_computed_value() {
    let mut c = computed_client(&users_schema());
    c.mutate(
        &Mutation::new()
            .insert(
                "users",
                json!({"first": "Ada", "last": "Lovelace", "fullName": "WRONG"}),
            )
            .build(),
        None,
    )
    .await
    .unwrap();
    let rows = c.collect_all("users");
    assert_eq!(rows[0]["fullName"], json!("Ada Lovelace"));
}

#[tokio::test]
async fn patch_recomputes_from_merged_doc() {
    let mut c = computed_client(&users_schema());
    let txn = Mutation::new()
        .insert("users", json!({"first": "Gracie", "last": "Hopper"}))
        .build();
    let results = c.mutate(&txn, None).await.unwrap();
    let id = match &results[0] {
        StepResult::Insert { id } => id.clone(),
        other => panic!("expected Insert, got {other:?}"),
    };
    c.mutate(
        &Mutation::new()
            .patch("users", &id, json!({"first": "Grace"}))
            .build(),
        None,
    )
    .await
    .unwrap();
    let rows = c.collect_all("users");
    assert_eq!(rows[0]["fullName"], json!("Grace Hopper"));
}

#[tokio::test]
async fn patch_computed_key_is_dropped_not_validated() {
    // A wrong-typed client value on a computed key never reaches
    // validate_value — the stamp re-derives it from the merged doc.
    let mut c = computed_client(&users_schema());
    let txn = Mutation::new()
        .insert("users", json!({"first": "Ada", "last": "Lovelace"}))
        .build();
    let results = c.mutate(&txn, None).await.unwrap();
    let id = match &results[0] {
        StepResult::Insert { id } => id.clone(),
        other => panic!("expected Insert, got {other:?}"),
    };
    c.mutate(
        &Mutation::new()
            .patch("users", &id, json!({"fullName": 123, "first": "Grace"}))
            .build(),
        None,
    )
    .await
    .unwrap();
    let rows = c.collect_all("users");
    assert_eq!(rows[0]["fullName"], json!("Grace Lovelace"));
}

#[tokio::test]
async fn replace_drops_client_computed_values_and_restamps() {
    let mut c = computed_client(&users_schema());
    let txn = Mutation::new()
        .insert("users", json!({"first": "Ada", "last": "Lovelace"}))
        .build();
    let results = c.mutate(&txn, None).await.unwrap();
    let id = match &results[0] {
        StepResult::Insert { id } => id.clone(),
        other => panic!("expected Insert, got {other:?}"),
    };
    c.mutate(
        &Mutation::new()
            .replace(
                "users",
                &id,
                json!({"first": "Alan", "last": "Turing", "fullName": 42}),
            )
            .build(),
        None,
    )
    .await
    .unwrap();
    let rows = c.collect_all("users");
    assert_eq!(rows[0]["fullName"], json!("Alan Turing"));
}

#[tokio::test]
async fn upsert_update_branch_restamps() {
    let mut c = computed_client(&users_schema());
    let txn = Mutation::new()
        .insert("users", json!({"first": "Ada", "last": "Lovelace"}))
        .build();
    let results = c.mutate(&txn, None).await.unwrap();
    let id = match &results[0] {
        StepResult::Insert { id } => id.clone(),
        other => panic!("expected Insert, got {other:?}"),
    };
    let upsert = Mutation::new()
        .upsert(
            "users",
            "by_fullName",
            &[json!("Ada Lovelace")],
            json!({"first": "Grace", "last": "Hopper"}),
            json!({"first": "Grace"}),
        )
        .build();
    c.mutate(&upsert, None).await.unwrap();
    let row = c.get("users", &id).expect("row present");
    // The patch only changes `first` — `last` (and the index key it fed)
    // survives the merge, so the re-stamp is "Grace Lovelace".
    assert_eq!(row["fullName"], json!("Grace Lovelace"));
}

#[tokio::test]
async fn null_result_removes_the_key() {
    // `nick = coalesce(nickname)` on an OPTIONAL field: a null evaluation
    // stores NO key — the unset-optional shape convention.
    let schema = Schema::builder()
        .table(
            "users",
            Table::new()
                .field("name", FieldType::String)
                .field("nickname", FieldType::optional(FieldType::String))
                .field("nick", FieldType::optional(FieldType::String))
                .index("by_name", &["name"])
                .computed("nick", ValueExpr::coalesce([ValueExpr::field("nickname")])),
        )
        .build();
    let mut c = computed_client(&schema);
    let txn = Mutation::new()
        .insert("users", json!({"name": "Ada", "nickname": "Ace"}))
        .insert("users", json!({"name": "Bo"}))
        .build();
    let results = c.mutate(&txn, None).await.unwrap();
    let ada = match &results[0] {
        StepResult::Insert { id } => id.clone(),
        other => panic!("expected Insert, got {other:?}"),
    };
    let rows = c.collect_all("users");
    let bo = rows
        .iter()
        .find(|r| r["name"] == json!("Bo"))
        .expect("Bo present");
    assert_eq!(bo["nick"], json!(null), "absent key reads as null");
    assert!(
        !bo.as_object().expect("row is object").contains_key("nick"),
        "a null evaluation stores NO key"
    );

    // Patch the input to null: the optional input key is stripped AND the
    // recomputed null REMOVES the computed key.
    c.mutate(
        &Mutation::new()
            .patch("users", &ada, json!({"nickname": null}))
            .build(),
        None,
    )
    .await
    .unwrap();
    let ada_row = c.get("users", &ada).expect("ada present");
    assert!(
        !ada_row
            .as_object()
            .expect("row is object")
            .contains_key("nick"),
        "recomputed null removes the computed key: {ada_row}"
    );
}

#[tokio::test]
async fn evaluation_error_fails_the_write_naming_the_field() {
    // Arithmetic over a non-numeric string errors; the message names the
    // computed field and the whole txn rolls back.
    let schema = Schema::builder()
        .table(
            "metrics",
            Table::new()
                .field("label", FieldType::String)
                .field("score", FieldType::Number)
                .index("by_label", &["label"])
                .computed(
                    "score",
                    ValueExpr::add(ValueExpr::field("label"), ValueExpr::literal(1)),
                ),
        )
        .build();
    let mut c = computed_client(&schema);
    let err = c
        .mutate(
            &Mutation::new()
                .insert("metrics", json!({"label": "abc"}))
                .build(),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message.contains("computed field 'score'"),
        "error names the computed field: {}",
        err.message
    );
    assert!(c.collect_all("metrics").is_empty(), "txn rolled back");
}

// ---- push validation: the six rules --------------------------------------

fn push_err(schema: &SchemaDef) -> RtDbError {
    let mut client = bare_client();
    client
        .push_schema(schema)
        .expect_err("push must be rejected")
}

#[test]
fn push_rejects_undeclared_computed_key() {
    let schema = Schema::builder()
        .table(
            "users",
            Table::new()
                .field("first", FieldType::String)
                .computed("fullName", ValueExpr::field("first")),
        )
        .build();
    let err = push_err(&schema);
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message.contains("not a declared field"),
        "{}",
        err.message
    );
}

#[test]
fn push_rejects_stamped_field_targets() {
    let base = |table: Table| Schema::builder().table("users", table).build();
    // ownerField target
    let owner = Table::new()
        .field("owner", FieldType::String)
        .field("first", FieldType::String)
        .owner_field("owner")
        .computed("owner", ValueExpr::field("first"));
    let err = push_err(&base(owner));
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("ownerField"), "{}", err.message);
    // collaboratorsField target
    let collabs = Table::new()
        .field("collabs", FieldType::array(FieldType::String))
        .field("first", FieldType::String)
        .collaborators_field("collabs")
        .computed("collabs", ValueExpr::field("first"));
    let err = push_err(&base(collabs));
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message.contains("collaboratorsField"),
        "{}",
        err.message
    );
    // autoIncrementField target
    let auto = Table::new()
        .field("num", FieldType::Int64)
        .field("first", FieldType::String)
        .auto_increment_field("num")
        .computed(
            "num",
            ValueExpr::cast(ValueExpr::field("first"), Cast::ToString),
        );
    let err = push_err(&base(auto));
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message.contains("autoIncrementField"),
        "{}",
        err.message
    );
}

#[test]
fn push_rejects_undeclared_and_computed_references() {
    // Rule 3a: the expression reads an undeclared field.
    let undeclared = Schema::builder()
        .table(
            "users",
            Table::new()
                .field("first", FieldType::String)
                .field("fullName", FieldType::String)
                .computed(
                    "fullName",
                    ValueExpr::concat([ValueExpr::field("first"), ValueExpr::field("middle")]),
                ),
        )
        .build();
    let err = push_err(&undeclared);
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message.contains("undeclared field 'middle'"),
        "{}",
        err.message
    );
    // Rule 3b: computed fields may not reference each other.
    let chained = Schema::builder()
        .table(
            "users",
            Table::new()
                .field("first", FieldType::String)
                .field("fullName", FieldType::String)
                .field("shout", FieldType::String)
                .computed("fullName", full_name_expr_field_only())
                .computed("shout", ValueExpr::upper(ValueExpr::field("fullName"))),
        )
        .build();
    let err = push_err(&chained);
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message.contains("references computed field 'fullName'"),
        "{}",
        err.message
    );
}

/// `fullName = concat(field(first))` — one input only, for the chained test.
fn full_name_expr_field_only() -> ValueExpr {
    ValueExpr::concat([ValueExpr::field("first")])
}

#[test]
fn push_rejects_case_when_principal_markers() {
    let schema = Schema::builder()
        .table(
            "docs",
            Table::new()
                .field("owner", FieldType::String)
                .field("vis", FieldType::String)
                .computed(
                    "vis",
                    ValueExpr::case(
                        vec![crate::value_expr::CaseWhen {
                            when: FilterExpr::Eq {
                                field: "owner".into(),
                                value: json!({"$user": true}),
                            },
                            then: ValueExpr::literal("private"),
                        }],
                        ValueExpr::literal("public"),
                    ),
                ),
        )
        .build();
    let err = push_err(&schema);
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("principal markers"), "{}", err.message);
}

#[test]
fn push_rejects_static_kind_the_field_type_does_not_accept() {
    // Rule 5: a string-producing expression (concat) on a NUMBER field.
    let schema = Schema::builder()
        .table(
            "metrics",
            Table::new()
                .field("denom", FieldType::optional(FieldType::Number))
                .field("ratio", FieldType::optional(FieldType::Number))
                .index("by_denom", &["denom"])
                .computed(
                    "ratio",
                    ValueExpr::concat([ValueExpr::field("denom"), ValueExpr::literal("x")]),
                ),
        )
        .build();
    let err = push_err(&schema);
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message.contains("which the field type does not accept"),
        "{}",
        err.message
    );
    // Int64 accepts a String-kind expression (the decimal-string wire form)
    // but not a Number-kind one (arithmetic).
    let string_kind = Schema::builder()
        .table(
            "t",
            Table::new()
                .field("n", FieldType::Number)
                .field("big", FieldType::Int64)
                .computed(
                    "big",
                    ValueExpr::cast(ValueExpr::field("n"), Cast::ToString),
                ),
        )
        .build();
    let mut ok = bare_client();
    assert!(ok.push_schema(&string_kind).is_ok());
    let number_kind = Schema::builder()
        .table(
            "t",
            Table::new()
                .field("n", FieldType::Number)
                .field("big", FieldType::Int64)
                .computed(
                    "big",
                    ValueExpr::add(ValueExpr::field("n"), ValueExpr::literal(1)),
                ),
        )
        .build();
    let err = push_err(&number_kind);
    assert_eq!(err.code, ErrorCode::BadRequest);
}

#[test]
fn push_rejects_authorize_referencing_a_computed_field() {
    let schema = Schema::builder()
        .table(
            "docs",
            Table::new()
                .field("vis", FieldType::String)
                .field("owner", FieldType::String)
                .authorize(FilterExpr::Eq {
                    field: "vis".into(),
                    value: json!("public"),
                })
                .computed("vis", ValueExpr::literal("public")),
        )
        .build();
    let err = push_err(&schema);
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message.contains("authorize predicate"),
        "{}",
        err.message
    );
}

#[test]
fn push_accepts_the_canonical_fixture() {
    // `computed_client` pushes the schema and unwraps — a rejection panics.
    let _ = computed_client(&users_schema());
}

// ---- migrate interplay (admin builds only) -------------------------------

#[cfg(feature = "admin")]
mod migrate_interplay {
    use super::*;
    use crate::wire::admin::Directive;

    #[tokio::test]
    async fn rename_rewrites_computed_refs_and_moves_the_key() {
        let mut c = computed_client(&users_schema());
        c.mutate(
            &Mutation::new()
                .insert("users", json!({"first": "Ada", "last": "Lovelace"}))
                .build(),
            None,
        )
        .await
        .unwrap();
        let result = c
            .migrate_schema(
                &[Directive::RenameField {
                    table: "users".into(),
                    from: "first".into(),
                    to: "givenName".into(),
                }],
                false,
            )
            .expect("migrate ok");
        assert!(result.applied);
        // The derived schema carries the keyed entry AND the rewritten Field
        // references; the stored doc's key is renamed with the value intact.
        let users = result.schema.tables.get("users").expect("users present");
        assert_eq!(
            users.computed.get("fullName"),
            Some(&ValueExpr::concat([
                ValueExpr::field("givenName"),
                ValueExpr::literal(" "),
                ValueExpr::field("last"),
            ]))
        );
        let rows = c.collect_all("users");
        assert_eq!(rows[0]["givenName"], json!("Ada"));
        assert_eq!(rows[0]["fullName"], json!("Ada Lovelace"));
        // A post-migrate write re-derives through the renamed field.
        let id = rows[0]["_id"].as_str().unwrap().to_string();
        c.mutate(
            &Mutation::new()
                .patch("users", &id, json!({"givenName": "Grace"}))
                .build(),
            None,
        )
        .await
        .unwrap();
        let rows = c.collect_all("users");
        assert_eq!(rows[0]["fullName"], json!("Grace Lovelace"));
    }

    #[tokio::test]
    async fn rename_rewrites_case_when_filter_refs() {
        let schema = Schema::builder()
            .table(
                "docs",
                Table::new()
                    .field("vis", FieldType::String)
                    .field("label", FieldType::String)
                    .field("kind", FieldType::String)
                    .computed(
                        "kind",
                        ValueExpr::case(
                            vec![crate::value_expr::CaseWhen {
                                when: FilterExpr::Eq {
                                    field: "vis".into(),
                                    value: json!("public"),
                                },
                                then: ValueExpr::literal("open"),
                            }],
                            ValueExpr::literal("shut"),
                        ),
                    ),
            )
            .build();
        let mut c = computed_client(&schema);
        let result = c
            .migrate_schema(
                &[Directive::RenameField {
                    table: "docs".into(),
                    from: "vis".into(),
                    to: "visibility".into(),
                }],
                false,
            )
            .expect("migrate ok");
        let docs = result.schema.tables.get("docs").expect("docs present");
        let ValueExpr::Case { whens, .. } = docs.computed.get("kind").expect("entry present")
        else {
            panic!("expected Case");
        };
        assert!(
            matches!(&whens[0].when, FilterExpr::Eq { field, .. } if field == "visibility"),
            "Case.when filter refs follow the rename"
        );
    }

    #[tokio::test]
    async fn drop_field_referenced_by_computed_is_rejected() {
        let mut c = computed_client(&users_schema());
        let err = c
            .migrate_schema(
                &[Directive::DropField {
                    table: "users".into(),
                    field: "first".into(),
                }],
                false,
            )
            .expect_err("drop must be rejected");
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(
            err.message
                .contains("referenced by computed field 'users.fullName'"),
            "{}",
            err.message
        );
    }

    #[tokio::test]
    async fn dropping_the_computed_field_removes_its_entry() {
        // The index on the dropped field goes first — a btree with no fields
        // fails the derived schema's validation (same as the server).
        let mut c = computed_client(&users_schema());
        let result = c
            .migrate_schema(
                &[
                    Directive::DropIndex {
                        table: "users".into(),
                        name: "by_fullName".into(),
                    },
                    Directive::DropField {
                        table: "users".into(),
                        field: "fullName".into(),
                    },
                ],
                false,
            )
            .expect("migrate ok");
        let users = result.schema.tables.get("users").expect("users present");
        assert!(users.computed.is_empty(), "entry removed with the field");
        assert!(!users.fields.contains_key("fullName"));
    }

    #[tokio::test]
    async fn change_type_revalidates_the_derived_schema() {
        // Retyping a computed field so its expression's static kind no longer
        // fits fails the migrate at plan time — before any commit.
        let mut c = computed_client(&users_schema());
        let err = c
            .migrate_schema(
                &[Directive::ChangeType {
                    table: "users".into(),
                    field: "fullName".into(),
                    to: FieldType::Number,
                    cast: Cast::ToNumber,
                    default: None,
                }],
                false,
            )
            .expect_err("changeType must be rejected");
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(
            err.message.contains("which the field type does not accept"),
            "{}",
            err.message
        );
        // dryRun of the same invalid plan fails identically (server
        // plan_migration validates before the dry-run branch).
        let err = c
            .migrate_schema(
                &[Directive::ChangeType {
                    table: "users".into(),
                    field: "fullName".into(),
                    to: FieldType::Number,
                    cast: Cast::ToNumber,
                    default: None,
                }],
                true,
            )
            .expect_err("dryRun of an invalid plan must fail");
        assert_eq!(err.code, ErrorCode::BadRequest);
    }
}

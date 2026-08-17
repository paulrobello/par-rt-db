use super::*;

#[test]
fn index_column_type_maps_each_indexable_field_and_rejects_others() {
    // Indexable shapes:
    assert_eq!(
        index_column_type(&FieldType::String).unwrap().pg,
        PgType::Text
    );
    assert_eq!(
        index_column_type(&FieldType::Number).unwrap().pg,
        PgType::Number
    );
    assert_eq!(
        index_column_type(&FieldType::Boolean).unwrap().pg,
        PgType::Boolean
    );
    assert_eq!(
        index_column_type(&FieldType::Int64).unwrap().pg,
        PgType::Int64
    );
    assert_eq!(
        index_column_type(&FieldType::id("t")).unwrap().pg,
        PgType::Text
    );
    assert_eq!(
        index_column_type(&FieldType::literal("a")).unwrap().pg,
        PgType::Text
    );
    assert_eq!(
        index_column_type(&FieldType::optional(FieldType::Number))
            .unwrap()
            .pg,
        PgType::Number
    );
    // Optional wraps and reports nullable=true.
    let it = index_column_type(&FieldType::optional(FieldType::Number)).unwrap();
    assert!(it.nullable);
    // Non-indexable shapes:
    let err = index_column_type(&FieldType::Array {
        element: Box::new(FieldType::Number),
    })
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);
    let err = index_column_type(&FieldType::literal(7)).unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);
}

#[test]
fn coerce_index_value_type_checks_against_index_column() {
    let schema = test_schema();
    let table = items_table(&schema);
    // `name` is String → text column. Number is rejected.
    coerce_index_value(table, "name", &json!("a")).expect("string ok");
    let err = coerce_index_value(table, "name", &json!(7)).unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    // `order` is Number → number column. String is rejected.
    coerce_index_value(table, "order", &json!(7)).expect("number ok");
    let err = coerce_index_value(table, "order", &json!("7")).unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    // Unknown field is INTERNAL (schema-declared index references a missing
    // field — a server-side programming error, not a client one).
    let err = coerce_index_value(table, "bogus", &json!(7)).unwrap_err();
    assert_eq!(err.code, ErrorCode::Internal);
}

#[test]
fn compare_index_values_orders_nulls_last_and_compares_each_domain() {
    use std::cmp::Ordering;
    // Numbers:
    assert_eq!(
        compare_index_values(&json!(1), &json!(2), PgType::Number),
        Ordering::Less
    );
    assert_eq!(
        compare_index_values(&json!(2), &json!(2), PgType::Number),
        Ordering::Equal
    );
    // Strings (lexicographic):
    assert_eq!(
        compare_index_values(&json!("a"), &json!("b"), PgType::Text),
        Ordering::Less
    );
    // Booleans (false < true):
    assert_eq!(
        compare_index_values(&json!(false), &json!(true), PgType::Boolean),
        Ordering::Less
    );
    // Int64 decimal strings compare numerically, not lexicographically:
    assert_eq!(
        compare_index_values(&json!("3"), &json!("20"), PgType::Int64),
        Ordering::Less
    );
    assert_eq!(
        compare_index_values(&json!("100"), &json!("20"), PgType::Int64),
        Ordering::Greater
    );
    assert_eq!(
        compare_index_values(&json!("-1"), &json!("0"), PgType::Int64),
        Ordering::Less
    );
    // Nulls sort last under asc — `null > anything`. The `pg` domain is
    // irrelevant once either side is null.
    assert_eq!(
        compare_index_values(&json!(null), &json!(1), PgType::Number),
        Ordering::Greater
    );
    assert_eq!(
        compare_index_values(&json!(1), &json!(null), PgType::Number),
        Ordering::Less
    );
    assert_eq!(
        compare_index_values(&json!(null), &json!(null), PgType::Number),
        Ordering::Equal
    );
}

#[test]
fn merge_doc_layers_system_fields_over_user_doc() {
    let row = StoredRow {
        id: "0018beacc10070000000000000000000".to_string(),
        doc: json!({"name": "a", "status": "todo", "order": 1}),
        version: 7,
        created_at: 1_700_000_000_000,
        deleted_at: None,
    };
    let merged = merge_doc(&row);
    assert_eq!(merged["_id"], json!("0018beacc10070000000000000000000"));
    assert_eq!(merged["_version"], 7);
    assert_eq!(merged["_creationTime"], 1_700_000_000_000_i64);
    // User fields preserved.
    assert_eq!(merged["name"], "a");
    assert_eq!(merged["order"], 1);
}

#[tokio::test]
async fn query_collect_returns_empty_for_empty_table() {
    // Mirrors TS "collects [] from an empty table after pushSchema".
    let c = new_client();
    let docs = c
        .run::<Vec<Value>>(&TableQuery::new("items").collect())
        .expect("collect ok");
    assert!(docs.is_empty());
}

#[tokio::test]
async fn query_get_returns_merged_doc() {
    // Mirrors TS "inserts a doc and merges system fields at read time"
    // (the read is now via the DSL `get` terminal, not the bare helper).
    let mut c = new_client();
    let r = c
        .mutate(
            &Mutation::new()
                .insert("items", json!({"name": "a", "status": "todo", "order": 1}))
                .build(),
            None,
        )
        .await
        .expect("insert ok");
    let id = match &r[0] {
        StepResult::Insert { id } => id.clone(),
        other => panic!("expected Insert, got {other:?}"),
    };

    let doc = c
        .run::<Value>(&TableQuery::get("items", &id))
        .expect("get ok");
    assert_eq!(doc["_id"], json!(id));
    assert_eq!(doc["name"], "a");
    assert_eq!(doc["status"], "todo");
    assert_eq!(doc["order"], 1);
    assert_eq!(doc["_version"], 1);
    assert!(doc["_creationTime"].is_number());
}

#[tokio::test]
async fn query_get_returns_null_for_missing_id() {
    // Mirrors TS "point-reads a missing id as null". The server returns
    // JSON null for a missing point read (TS :916), not an error.
    let c = new_client();
    let v = c
        .run::<Value>(&TableQuery::get(
            "items",
            "0123456789abcdef0123456789abcdef",
        ))
        .expect("get resolves");
    assert!(v.is_null(), "missing get returns Value::Null, got: {v}");
}

#[tokio::test]
async fn query_get_rejects_combinations() {
    // Ports the `get`-exclusivity guard at TS :895-914. `get` plus any
    // narrowing clause is BAD_REQUEST.
    let c = new_client();
    let q = Query {
        table: "items".into(),
        get: Some("x".into()),
        index: Some("by_name".into()),
        ..Default::default()
    };
    let err = c.run_query(&q).unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message.contains("get cannot be combined"),
        "got: {}",
        err.message
    );
}

// ---- query: index eq + order + take ------------------------------

#[tokio::test]
async fn query_eq_prefix_with_order_asc_sorts_by_remaining_field() {
    // Mirrors TS "filters by an eq index prefix and orders by the remaining
    // index field" — the asc branch.
    let mut c = new_client();
    seed_query_rows(&mut c).await;

    let asc = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[json!("todo")])
                .order(Order::Asc)
                .collect(),
        )
        .expect("asc ok");
    let orders: Vec<i64> = asc
        .iter()
        .map(|d| d["order"].as_i64().unwrap_or_default())
        .collect();
    assert_eq!(orders, vec![1, 2, 3], "asc order");
}

#[tokio::test]
async fn query_eq_prefix_with_order_desc_and_take_n() {
    // Mirrors TS "filters by an eq index prefix and orders by the remaining
    // index field" — the desc+take(2) branch.
    let mut c = new_client();
    seed_query_rows(&mut c).await;

    let desc = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[json!("todo")])
                .order(Order::Desc)
                .take(2),
        )
        .expect("desc+take ok");
    let orders: Vec<i64> = desc
        .iter()
        .map(|d| d["order"].as_i64().unwrap_or_default())
        .collect();
    assert_eq!(orders, vec![3, 2], "desc order, take 2");
}

#[tokio::test]
async fn query_eq_on_single_field_index_returns_matching_rows() {
    // The brief calls out single-field eq match explicitly; `by_name` is
    // single-field. Two rows share `name="dup"`, the third doesn't.
    let mut c = new_client();
    for order in [1_i64, 2, 3] {
        let name = if order <= 2 { "dup" } else { "uniq" };
        c.mutate(
            &Mutation::new()
                .insert(
                    "items",
                    json!({"name": name, "status": "todo", "order": order}),
                )
                .build(),
            None,
        )
        .await
        .unwrap();
    }
    let docs = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .with_index("by_name", &[json!("dup")])
                .collect(),
        )
        .expect("eq ok");
    assert_eq!(docs.len(), 2, "both dup rows match");
    for d in &docs {
        assert_eq!(d["name"], "dup");
    }
}

// ---- query: range bounds ----------------------------------------

#[tokio::test]
async fn query_range_filters_by_index_field() {
    // gt / lt / gte / lte over the remaining index field. `by_status_and_order`
    // has `status` then `order`; the eq prefix pins status, the range
    // narrows order. Seed order values [3,1,2] and assert each bound.
    let mut c = new_client();
    seed_query_rows(&mut c).await;

    let collect_range = |gt: Option<i64>, gte: Option<i64>, lt: Option<i64>, lte: Option<i64>| {
        let mut q = TableQuery::new("items").with_index("by_status_and_order", &[json!("todo")]);
        if let Some(v) = gt {
            q = q.gt(v);
        }
        if let Some(v) = gte {
            q = q.gte(v);
        }
        if let Some(v) = lt {
            q = q.lt(v);
        }
        if let Some(v) = lte {
            q = q.lte(v);
        }
        c.run::<Vec<Value>>(&q.order(Order::Asc).collect())
            .expect("range ok")
    };

    let orders = |docs: Vec<Value>| -> Vec<i64> {
        docs.iter()
            .map(|d| d["order"].as_i64().unwrap_or_default())
            .collect()
    };

    // gt=1 → {2,3}; gte=2 → {2,3}; lt=3 → {1,2}; lte=2 → {1,2}.
    assert_eq!(orders(collect_range(Some(1), None, None, None)), vec![2, 3]);
    assert_eq!(orders(collect_range(None, Some(2), None, None)), vec![2, 3]);
    assert_eq!(orders(collect_range(None, None, Some(3), None)), vec![1, 2]);
    assert_eq!(orders(collect_range(None, None, None, Some(2))), vec![1, 2]);
}

// ---- query: int64 index (numeric ordering + range) ----------------

/// Schema for int64-indexable coverage: a single `by_ts` index over an
/// `Int64` field, plus a string payload to identify rows in assertions.
fn int64_test_schema() -> SchemaDef {
    Schema::builder()
        .table(
            "events",
            Table::new()
                .field("ts", FieldType::Int64)
                .field("kind", FieldType::String)
                .index("by_ts", &["ts"]),
        )
        .build()
}

/// Client seeded with [`int64_test_schema`] and a deterministic incrementing
/// clock so each insert gets a distinct `_id` (the default constant-RNG id
/// collides within a single millisecond, which would make successive inserts
/// overwrite each other).
fn int64_client() -> InMemoryRtDbClient {
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
    client.push_schema(&int64_test_schema()).unwrap();
    client
}

#[tokio::test]
async fn int64_index_orders_and_ranges_numerically() {
    // Int64 indexes store decimal strings, but the index order has to be
    // numeric (3 < 20 < 100), not lexicographic (100 < 20 < 3). Seeds the
    // rows out of numeric order to catch a lexicographic regression on
    // both the sort path and the range-bound path.
    let mut c = int64_client();
    for (ts, kind) in [("100", "a"), ("20", "b"), ("3", "c")] {
        c.mutate(
            &Mutation::new()
                .insert("events", json!({ "ts": ts, "kind": kind }))
                .build(),
            None,
        )
        .await
        .unwrap();
    }

    let kinds = |docs: Vec<Value>| -> Vec<String> {
        docs.iter()
            .map(|d| d["kind"].as_str().unwrap_or_default().to_string())
            .collect()
    };

    // Ascending numeric sort over the by_ts index → 3, 20, 100.
    let asc = c
        .run::<Vec<Value>>(
            &TableQuery::new("events")
                .with_index("by_ts", &[])
                .order(Order::Asc)
                .collect(),
        )
        .expect("asc ok");
    assert_eq!(
        kinds(asc),
        vec!["c".to_string(), "b".to_string(), "a".to_string()],
        "int64 index should sort numerically (3, 20, 100)"
    );

    // Range on the int64 field: gte=20 keeps {20, 100}, asc → [b, a].
    let ranged = c
        .run::<Vec<Value>>(
            &TableQuery::new("events")
                .with_index("by_ts", &[])
                .gte(json!("20"))
                .order(Order::Asc)
                .collect(),
        )
        .expect("range ok");
    assert_eq!(
        kinds(ranged),
        vec!["b".to_string(), "a".to_string()],
        "int64 range bound should compare numerically (gte=20 keeps 20, 100)"
    );
}

// ---- query: terminals -------------------------------------------

#[tokio::test]
async fn query_count_returns_number_of_matching_rows() {
    // Mirrors TS "counts matching rows over an eq prefix".
    let mut c = new_client();
    seed_query_rows(&mut c).await;
    let n = c
        .run::<i64>(
            &TableQuery::new("items")
                .with_index("by_status", &[json!("todo")])
                .count(),
        )
        .expect("count ok");
    assert_eq!(n, 3);
}

#[tokio::test]
async fn query_unique_returns_doc_when_exactly_one_match() {
    let mut c = new_client();
    c.mutate(
        &Mutation::new()
            .insert(
                "items",
                json!({"name": "only", "status": "todo", "order": 1}),
            )
            .build(),
        None,
    )
    .await
    .unwrap();
    let doc = c
        .run::<Value>(
            &TableQuery::new("items")
                .with_index("by_name", &[json!("only")])
                .unique(),
        )
        .expect("unique ok");
    assert_eq!(doc["name"], "only");
}

#[tokio::test]
async fn query_unique_throws_precondition_failed_when_multiple_match() {
    // Mirrors TS "unique throws PRECONDITION_FAILED when more than one doc
    // matches".
    let mut c = new_client();
    for order in [1_i64, 2] {
        c.mutate(
            &Mutation::new()
                .insert(
                    "items",
                    json!({"name": "dup", "status": "todo", "order": order}),
                )
                .build(),
            None,
        )
        .await
        .unwrap();
    }
    let err = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_name", &[json!("dup")])
                .unique(),
        )
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::PreconditionFailed);
}

#[tokio::test]
async fn query_unique_returns_null_when_zero_match() {
    // TS :1143 — `unique` with zero matches returns null (no precondition
    // to fail; only a multi-match is an error).
    let c = new_client();
    let v = c
        .run::<Value>(
            &TableQuery::new("items")
                .with_index("by_name", &[json!("ghost")])
                .unique(),
        )
        .expect("unique resolves");
    assert!(v.is_null(), "zero-match unique returns null, got: {v}");
}

#[tokio::test]
async fn query_first_returns_first_or_null() {
    // Mirrors TS `first` terminal: the first row of the filtered+sorted
    // set, or null when empty.
    let mut c = new_client();
    // Empty table: first = null.
    let v = c
        .run::<Value>(
            &TableQuery::new("items")
                .with_index("by_status", &[json!("todo")])
                .first(),
        )
        .expect("first on empty");
    assert!(v.is_null(), "first on empty table is null");

    seed_query_rows(&mut c).await;
    // With rows sorted ascending, first is order=1.
    let first = c
        .run::<Value>(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[json!("todo")])
                .order(Order::Asc)
                .first(),
        )
        .expect("first ok");
    assert_eq!(first["order"], 1, "first asc is order=1");
}

#[tokio::test]
async fn query_take_caps_results_at_n() {
    let mut c = new_client();
    seed_query_rows(&mut c).await;
    let docs = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .with_index("by_status", &[json!("todo")])
                .order(Order::Asc)
                .take(2),
        )
        .expect("take ok");
    assert_eq!(docs.len(), 2, "take(2) on 3 rows caps at 2");
}

// ---- query: validation rejections -------------------------------

#[tokio::test]
async fn query_rejects_eq_without_index() {
    let c = new_client();
    let err = c
        .run_query(&Query {
            table: "items".into(),
            eq: vec![json!("x")],
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("eq requires an index"), "got: {err}");
}

#[tokio::test]
async fn query_rejects_range_without_index() {
    let c = new_client();
    let err = c
        .run_query(&Query {
            table: "items".into(),
            gt: Some(json!(1)),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message.contains("range bound requires an index"),
        "got: {err}"
    );
}

#[tokio::test]
async fn query_rejects_range_without_remaining_field_after_eq() {
    // `by_name` has one field — a full-arity eq leaves no field for a
    // range bound.
    let c = new_client();
    let err = c
        .run_query(&Query {
            table: "items".into(),
            index: Some("by_name".into()),
            eq: vec![json!("a")],
            gt: Some(json!("z")),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message.contains("remaining index field after eq"),
        "got: {err}"
    );
}

#[tokio::test]
async fn query_rejects_eq_arity_above_index_field_count() {
    // `by_name` is single-field; two eq values is over-arity.
    let c = new_client();
    let err = c
        .run_query(&Query {
            table: "items".into(),
            index: Some("by_name".into()),
            eq: vec![json!("a"), json!("b")],
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("expects at most"), "got: {err}");
}

#[tokio::test]
async fn query_rejects_gt_and_gte_together() {
    let c = new_client();
    let err = c
        .run_query(&Query {
            table: "items".into(),
            index: Some("by_status_and_order".into()),
            eq: vec![json!("todo")],
            gt: Some(json!(1)),
            gte: Some(json!(1)),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("gt and gte"), "got: {err}");
}

#[tokio::test]
async fn query_rejects_lt_and_lte_together() {
    let c = new_client();
    let err = c
        .run_query(&Query {
            table: "items".into(),
            index: Some("by_status_and_order".into()),
            eq: vec![json!("todo")],
            lt: Some(json!(1)),
            lte: Some(json!(1)),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("lt and lte"), "got: {err}");
}

#[tokio::test]
async fn query_rejects_take_over_max_take() {
    // MAX_TAKE guard (TS :963-965).
    let c = new_client();
    let err = c
        .run_query(&Query {
            table: "items".into(),
            take: Some((MAX_TAKE as u32) + 1),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("maximum"), "got: {err}");
}

#[tokio::test]
async fn query_accepts_take_at_max_take() {
    // `take == MAX_TAKE` is the boundary — accepted.
    let c = new_client();
    let docs = c
        .run::<Vec<Value>>(&Query {
            table: "items".into(),
            take: Some(MAX_TAKE as u32),
            ..Default::default()
        })
        .expect("take=MAX_TAKE ok");
    assert!(docs.is_empty(), "empty table → empty page");
}

/// One assertion per conflicting-terminal guard at TS :919-939. Each case
/// is BAD_REQUEST; the needle distinguishes which guard fired.
#[tokio::test]
async fn query_rejects_conflicting_terminals() {
    let c = new_client();
    let base_index_query =
        |unique: bool, first: bool, count: bool, order: bool, take: Option<u32>| Query {
            table: "items".into(),
            index: Some("by_status".into()),
            eq: vec![json!("todo")],
            unique,
            first,
            count,
            order: order.then_some(Order::Asc),
            take,
            ..Default::default()
        };

    let cases: &[(Query, &str)] = &[
        // unique + take
        (
            base_index_query(true, false, false, false, Some(1)),
            "unique cannot be combined with take",
        ),
        // unique + order
        (
            base_index_query(true, false, false, true, None),
            "unique cannot be combined with take, order",
        ),
        // first + unique
        (
            base_index_query(true, true, false, false, None),
            "first cannot be combined with unique",
        ),
        // first + take
        (
            base_index_query(false, true, false, false, Some(1)),
            "first cannot be combined with take",
        ),
        // count + unique
        (
            base_index_query(true, false, true, false, None),
            "count cannot be combined with unique",
        ),
        // count + take
        (
            base_index_query(false, false, true, false, Some(1)),
            "count cannot be combined with take",
        ),
        // count + first
        (
            base_index_query(false, true, true, false, None),
            "count cannot be combined with first",
        ),
        // count + order
        (
            base_index_query(false, false, true, true, None),
            "count cannot be combined with order",
        ),
    ];
    for (q, needle) in cases {
        let err = c.run_query(q).unwrap_err();
        assert_eq!(
            err.code,
            ErrorCode::BadRequest,
            "case '{needle}': got {err:?}"
        );
        assert!(
            err.message.contains(needle),
            "case '{needle}' missing needle: got {}",
            err.message
        );
    }
}

use super::*;

// ---- query: distinct + aggregate terminals ---------------------
//
// Ports distinct/aggregate coverage from `ts-client/src/in_memory.ts`
// (`executeQuery` :1355-1462) and the server's `execute_query` arms. Both
// are standalone terminals over the index field immediately after the eq
// prefix; they compose only with index/eq/range/filter.

/// Seeds `items` with duplicated `order` values {3,1,2,1,2} (all "todo") so
/// distinct dedupe and asc sort are both observable.
async fn seed_dup_orders(c: &mut InMemoryRtDbClient) {
    for order in [3_i64, 1, 2, 1, 2] {
        c.mutate(
            &Mutation::new()
                .insert(
                    "items",
                    json!({"name": format!("n{order}"), "status": "todo", "order": order}),
                )
                .build(),
            None,
        )
        .await
        .unwrap();
    }
}

/// Seeds `items` with two statuses so a `groupBy` over
/// `by_status_and_order` has multiple groups: todo {1,2}, done {3,4}.
async fn seed_group_rows(c: &mut InMemoryRtDbClient) {
    for (status, order) in [("todo", 1_i64), ("todo", 2), ("done", 3), ("done", 4)] {
        c.mutate(
            &Mutation::new()
                .insert(
                    "items",
                    json!({"name": "n", "status": status, "order": order}),
                )
                .build(),
            None,
        )
        .await
        .unwrap();
    }
}

/// Seeds `items` with `status` values {charlie, alpha, bravo} so non-numeric
/// MIN/MAX pick lexicographic extremes.
async fn seed_status_rows(c: &mut InMemoryRtDbClient) {
    for (i, status) in ["charlie", "alpha", "bravo"].iter().enumerate() {
        c.mutate(
            &Mutation::new()
                .insert(
                    "items",
                    json!({"name": "n", "status": status, "order": i as i64}),
                )
                .build(),
            None,
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn distinct_returns_unique_index_field_values_sorted_asc() {
    let mut c = new_client();
    seed_query_rows(&mut c).await; // orders 3, 1, 2 — all "todo"
    let v = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[json!("todo")])
                .distinct(),
        )
        .expect("distinct ok");
    assert_eq!(v, json!([1, 2, 3]));
}

#[tokio::test]
async fn distinct_dedupes_repeated_values() {
    let mut c = new_client();
    seed_dup_orders(&mut c).await; // orders 3,1,2,1,2
    let v = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[json!("todo")])
                .distinct(),
        )
        .expect("distinct ok");
    assert_eq!(v, json!([1, 2, 3]));
}

#[tokio::test]
async fn distinct_composes_with_range_bound() {
    let mut c = new_client();
    seed_query_rows(&mut c).await; // orders 3, 1, 2
    let v = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[json!("todo")])
                .gte(2)
                .distinct(),
        )
        .expect("distinct+range ok");
    assert_eq!(v, json!([2, 3]));
}

#[tokio::test]
async fn distinct_empty_matching_set_returns_empty_array() {
    let mut c = new_client();
    seed_query_rows(&mut c).await;
    let v = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[json!("missing")])
                .distinct(),
        )
        .expect("distinct ok");
    assert_eq!(v, json!([]));
}

#[tokio::test]
async fn distinct_requires_an_index_field_beyond_eq_prefix() {
    let c = new_client();
    // eq prefix [todo, 1] consumes both index fields of by_status_and_order.
    let err = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[json!("todo"), json!(1)])
                .distinct(),
        )
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message
            .contains("distinct requires an index field beyond the eq prefix"),
        "got: {}",
        err.message
    );
}

#[tokio::test]
async fn distinct_rejects_conflicting_terminals() {
    // Ownership mirrors the server's check order (query.rs :676-706): get,
    // unique, first, count are validated before distinct, so distinct+
    // {get,unique,first,count} surfaces *that* terminal's message; distinct
    // owns only take/order/aggregate.
    let c = new_client();
    let base = || Query {
        table: "items".into(),
        index: Some("by_status_and_order".into()),
        eq: vec![json!("todo")],
        ..Default::default()
    };
    let cases: &[(Query, &str)] = &[
        (
            Query {
                distinct: true,
                take: Some(1),
                ..base()
            },
            "distinct cannot be combined with take",
        ),
        (
            Query {
                distinct: true,
                order: Some(Order::Asc),
                ..base()
            },
            "distinct cannot be combined with order",
        ),
        (
            Query {
                distinct: true,
                aggregate: Some(AggregateSpec {
                    op: AggregateOp::Sum,
                    group_by: false,
                }),
                ..base()
            },
            "distinct cannot be combined with aggregate",
        ),
        (
            Query {
                distinct: true,
                unique: true,
                ..base()
            },
            "unique cannot be combined with take, order, distinct, or aggregate",
        ),
        (
            Query {
                distinct: true,
                first: true,
                ..base()
            },
            "first cannot be combined with distinct",
        ),
        (
            Query {
                distinct: true,
                count: true,
                ..base()
            },
            "count cannot be combined with distinct",
        ),
        (
            Query {
                distinct: true,
                get: Some("x".into()),
                ..base()
            },
            "get cannot be combined with",
        ),
    ];
    for (q, needle) in cases {
        let err = c.run_query(q).unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest, "case '{needle}': {err:?}");
        assert!(
            err.message.contains(needle),
            "case '{needle}': got {}",
            err.message
        );
    }
}

#[tokio::test]
async fn aggregate_sum_avg_min_max_over_numeric_field() {
    let mut c = new_client();
    seed_query_rows(&mut c).await; // orders 3, 1, 2

    let sum = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[json!("todo")])
                .aggregate(AggregateOp::Sum, false),
        )
        .expect("sum");
    assert_eq!(sum.as_f64(), Some(6.0));
    let avg = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[json!("todo")])
                .aggregate(AggregateOp::Avg, false),
        )
        .expect("avg");
    assert_eq!(avg.as_f64(), Some(2.0));
    let min = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[json!("todo")])
                .aggregate(AggregateOp::Min, false),
        )
        .expect("min");
    assert_eq!(min.as_f64(), Some(1.0));
    let max = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[json!("todo")])
                .aggregate(AggregateOp::Max, false),
        )
        .expect("max");
    assert_eq!(max.as_f64(), Some(3.0));
}

#[tokio::test]
async fn aggregate_empty_matching_set_returns_null() {
    let mut c = new_client();
    seed_query_rows(&mut c).await;
    let v = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[json!("missing")])
                .aggregate(AggregateOp::Sum, false),
        )
        .expect("aggregate ok");
    assert!(v.is_null(), "empty aggregate is null, got: {v}");
}

#[tokio::test]
async fn aggregate_sum_requires_a_numeric_field() {
    let mut c = new_client();
    seed_status_rows(&mut c).await;
    // by_status [status], empty eq → agg field is `status` (a string).
    let err = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status", &[])
                .aggregate(AggregateOp::Sum, false),
        )
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message
            .contains("aggregate op sum requires a numeric index field"),
        "got: {}",
        err.message
    );
}

#[tokio::test]
async fn aggregate_min_max_over_string_field_are_lexicographic() {
    let mut c = new_client();
    seed_status_rows(&mut c).await; // statuses charlie, alpha, bravo
    let min = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status", &[])
                .aggregate(AggregateOp::Min, false),
        )
        .expect("min");
    assert_eq!(min.as_str(), Some("alpha"));
    let max = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status", &[])
                .aggregate(AggregateOp::Max, false),
        )
        .expect("max");
    assert_eq!(max.as_str(), Some("charlie"));
}

#[tokio::test]
async fn aggregate_group_by_groups_and_aggregates() {
    let mut c = new_client();
    seed_group_rows(&mut c).await; // todo{1,2}, done{3,4}
    let v = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[])
                .aggregate(AggregateOp::Sum, true),
        )
        .expect("groupBy ok");
    let arr = v.as_array().expect("array of {key,value}");
    assert_eq!(arr.len(), 2);
    // Groups are ordered by key ascending: "done" < "todo".
    assert_eq!(arr[0]["key"].as_str(), Some("done"));
    assert_eq!(arr[0]["value"].as_f64(), Some(7.0));
    assert_eq!(arr[1]["key"].as_str(), Some("todo"));
    assert_eq!(arr[1]["value"].as_f64(), Some(3.0));
}

#[tokio::test]
async fn aggregate_count_scalar_returns_matching_row_count() {
    let mut c = new_client();
    seed_query_rows(&mut c).await; // three "todo" rows
    let v = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[json!("todo")])
                .aggregate(AggregateOp::Count, false),
        )
        .expect("count ok");
    assert_eq!(v.as_i64(), Some(3));
}

#[tokio::test]
async fn aggregate_count_scalar_empty_matching_set_is_zero() {
    let mut c = new_client();
    seed_query_rows(&mut c).await;
    // count over zero rows is 0 (never null, unlike sum/avg/min/max).
    let v = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[json!("missing")])
                .aggregate(AggregateOp::Count, false),
        )
        .expect("count ok");
    assert_eq!(v.as_i64(), Some(0));
}

#[tokio::test]
async fn aggregate_count_grouped_returns_group_sizes() {
    let mut c = new_client();
    seed_group_rows(&mut c).await; // todo{1,2}, done{3,4}
    let v = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[])
                .aggregate(AggregateOp::Count, true),
        )
        .expect("groupBy count ok");
    let arr = v.as_array().expect("array of {key,value}");
    assert_eq!(arr.len(), 2);
    // Ordered by key ascending: "done" < "todo".
    assert_eq!(arr[0]["key"].as_str(), Some("done"));
    assert_eq!(arr[0]["value"].as_i64(), Some(2));
    assert_eq!(arr[1]["key"].as_str(), Some("todo"));
    assert_eq!(arr[1]["value"].as_i64(), Some(2));
}

#[tokio::test]
async fn aggregate_count_consumes_no_aggregate_field() {
    // count needs no field beyond the eq prefix: by_status [status] with an
    // empty eq prefix would error for sum/avg ("requires an index field
    // beyond the eq prefix") but succeeds for count.
    let mut c = new_client();
    seed_status_rows(&mut c).await; // three rows
    let v = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status", &[])
                .aggregate(AggregateOp::Count, false),
        )
        .expect("count needs no agg field");
    assert_eq!(v.as_i64(), Some(3));
}

#[tokio::test]
async fn aggregate_group_by_requires_two_index_fields_beyond_prefix() {
    let c = new_client();
    // by_status [status], empty eq → only one field beyond the prefix.
    let err = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status", &[])
                .aggregate(AggregateOp::Sum, true),
        )
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message
            .contains("aggregate groupBy requires two index fields beyond the eq prefix"),
        "got: {}",
        err.message
    );
}

#[tokio::test]
async fn aggregate_requires_an_index_field_beyond_eq_prefix() {
    let c = new_client();
    // eq prefix [todo, 1] consumes both fields of by_status_and_order.
    let err = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[json!("todo"), json!(1)])
                .aggregate(AggregateOp::Min, false),
        )
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message
            .contains("aggregate requires an index field beyond the eq prefix"),
        "got: {}",
        err.message
    );
}

#[tokio::test]
async fn aggregate_rejects_conflicting_terminals() {
    let c = new_client();
    let base = || Query {
        table: "items".into(),
        index: Some("by_status_and_order".into()),
        eq: vec![json!("todo")],
        ..Default::default()
    };
    let sum = || AggregateSpec {
        op: AggregateOp::Sum,
        group_by: false,
    };
    let cases: &[(Query, &str)] = &[
        (
            Query {
                aggregate: Some(sum()),
                take: Some(1),
                ..base()
            },
            "aggregate cannot be combined with take",
        ),
        (
            Query {
                aggregate: Some(sum()),
                order: Some(Order::Asc),
                ..base()
            },
            "aggregate cannot be combined with order",
        ),
        (
            Query {
                aggregate: Some(sum()),
                unique: true,
                ..base()
            },
            "unique cannot be combined with take, order, distinct, or aggregate",
        ),
        (
            Query {
                aggregate: Some(sum()),
                first: true,
                ..base()
            },
            "first cannot be combined with aggregate",
        ),
        (
            Query {
                aggregate: Some(sum()),
                count: true,
                ..base()
            },
            "count cannot be combined with aggregate",
        ),
        (
            Query {
                aggregate: Some(sum()),
                distinct: true,
                ..base()
            },
            "distinct cannot be combined with aggregate",
        ),
        (
            Query {
                aggregate: Some(sum()),
                get: Some("x".into()),
                ..base()
            },
            "get cannot be combined with",
        ),
    ];
    for (q, needle) in cases {
        let err = c.run_query(q).unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest, "case '{needle}': {err:?}");
        assert!(
            err.message.contains(needle),
            "case '{needle}': got {}",
            err.message
        );
    }
}

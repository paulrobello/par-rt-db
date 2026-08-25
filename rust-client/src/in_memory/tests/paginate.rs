use super::*;

// ---- query: paginate (cursor keyset) -----------------------------
//
// Direct port of `describe("InMemoryRtDbClient — paginate (cursor keyset)")`
// (`ts-client/tests/in_memory.test.ts:250-431`). The deterministic clock +
// RNG make `_creationTime` and `_id` rise with insertion order, so an
// ascending sort yields insertion order and a descending sort reverses it.

/// Mirrors TS `seedItems` (`ts-client/tests/in_memory.test.ts:254-269`):
/// insert `count` items with `order` = 1..count and `status` cycling
/// through `statuses`. Returns the inserted ids in insertion order.
async fn seed_items(c: &mut InMemoryRtDbClient, count: i64, statuses: &[&str]) -> Vec<String> {
    let mut ids = Vec::new();
    for i in 1..=count {
        let txn = Mutation::new()
            .insert(
                "items",
                json!({
                    "name": format!("n{i}"),
                    "status": statuses[((i - 1) as usize) % statuses.len()],
                    "order": i,
                }),
            )
            .build();
        let results = c.mutate(&txn, None).await.expect("insert ok");
        match &results[0] {
            StepResult::Insert { id } => ids.push(id.clone()),
            other => panic!("expected Insert, got {other:?}"),
        }
    }
    ids
}

/// Walks the full cursor chain until `next_cursor` is absent — ports TS
/// `walkPages` (`ts-client/tests/in_memory.test.ts:272-295`). Returns the
/// observed page sizes, the per-page cursors (final one `None`), and all
/// docs concatenated in page order.
async fn walk_pages<F>(
    c: &InMemoryRtDbClient,
    build: F,
) -> (Vec<usize>, Vec<Option<String>>, Vec<Value>)
where
    F: Fn(Option<&str>) -> Query,
{
    let mut page_sizes = Vec::new();
    let mut cursors = Vec::new();
    let mut docs = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..1000 {
        let page: Paginated<Value> = c.run(&build(cursor.as_deref())).expect("paginate ok");
        page_sizes.push(page.docs.len());
        cursors.push(page.next_cursor.clone());
        docs.extend(page.docs);
        if page.next_cursor.is_none() {
            return (page_sizes, cursors, docs);
        }
        cursor = page.next_cursor;
    }
    panic!("pagination did not terminate");
}

#[tokio::test]
async fn paginate_returns_empty_page_with_no_cursor_on_empty_table() {
    // Ports TS "returns an empty page with no nextCursor on an empty table".
    let c = new_client();
    let page: Paginated<Value> = c
        .run(&TableQuery::new("items").paginate(None, 3))
        .expect("paginate ok");
    assert!(page.docs.is_empty());
    assert!(page.next_cursor.is_none());
}

#[tokio::test]
async fn paginate_walks_all_pages_terminating_on_short_last_page() {
    // Ports TS "walks all pages in order, terminating on a short last page".
    let mut c = new_client();
    seed_items(&mut c, 7, &["todo"]).await;
    let (page_sizes, cursors, docs) =
        walk_pages(&c, |cursor| TableQuery::new("items").paginate(cursor, 3)).await;
    // Page sizes 3, 3, 1; the walk must equal a plain collect() with no
    // skips or duplicates.
    assert_eq!(page_sizes, vec![3, 3, 1]);
    assert!(cursors[..cursors.len() - 1].iter().all(|x| x.is_some()));
    assert!(cursors.last().is_some_and(|x| x.is_none()));

    let collected: Vec<Value> = c
        .run::<Vec<Value>>(&TableQuery::new("items").collect())
        .expect("collect ok");
    let walked_ids: Vec<&str> = docs
        .iter()
        .map(|d| d["_id"].as_str().expect("id string"))
        .collect();
    let collected_ids: Vec<&str> = collected
        .iter()
        .map(|d| d["_id"].as_str().expect("id string"))
        .collect();
    assert_eq!(walked_ids, collected_ids);
    let mut unique = walked_ids.to_vec();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), walked_ids.len(), "no duplicates across pages");
}

#[tokio::test]
async fn paginate_terminates_on_full_last_page_when_count_is_exact_multiple() {
    // Ports TS "terminates on a full last page when the count is an exact
    // multiple": the final page is full but `nextCursor` is None.
    let mut c = new_client();
    seed_items(&mut c, 6, &["todo"]).await;
    let (page_sizes, cursors, _docs) =
        walk_pages(&c, |cursor| TableQuery::new("items").paginate(cursor, 3)).await;
    assert_eq!(page_sizes, vec![3, 3]);
    assert!(cursors[0].is_some());
    assert!(cursors[1].is_none());
}

#[tokio::test]
async fn paginate_within_eq_prefixed_index_in_index_order() {
    // Ports TS "paginates within an eq-prefixed multi-field index in index
    // order": status cycles todo/done/todo ⇒ todos are orders 1,3,4,6,7,9.
    let mut c = new_client();
    seed_items(&mut c, 9, &["todo", "done", "todo"]).await;
    let (page_sizes, _cursors, docs) = walk_pages(&c, |cursor| {
        TableQuery::new("items")
            .with_index("by_status_and_order", &[json!("todo")])
            .paginate(cursor, 4)
    })
    .await;
    assert_eq!(page_sizes, vec![4, 2]);
    let orders: Vec<i64> = docs
        .iter()
        .map(|d| d["order"].as_i64().expect("order number"))
        .collect();
    assert_eq!(orders, vec![1, 3, 4, 6, 7, 9]);
    assert!(docs.iter().all(|d| d["status"] == json!("todo")));
}

#[tokio::test]
async fn paginate_descending_pages_in_reverse_index_order() {
    // Ports TS "walks descending pages in reverse index order": same seed
    // as the asc case, but order=desc ⇒ 9,7,6,4,3,1.
    let mut c = new_client();
    seed_items(&mut c, 9, &["todo", "done", "todo"]).await;
    let (page_sizes, _cursors, docs) = walk_pages(&c, |cursor| {
        TableQuery::new("items")
            .with_index("by_status_and_order", &[json!("todo")])
            .order(Order::Desc)
            .paginate(cursor, 4)
    })
    .await;
    assert_eq!(page_sizes, vec![4, 2]);
    let orders: Vec<i64> = docs
        .iter()
        .map(|d| d["order"].as_i64().expect("order number"))
        .collect();
    assert_eq!(orders, vec![9, 7, 6, 4, 3, 1]);
}

#[tokio::test]
async fn paginate_cursor_round_trips_and_resumes_chain() {
    // Ports TS "emits cursors decodable by the live client; resume
    // continues the chain": the cursor decodes to the last row's
    // [order, _creationTime, _id] tuple — cursors are interchangeable.
    let mut c = new_client();
    seed_items(&mut c, 5, &["todo"]).await; // todo orders 1..5
    let first: Paginated<Value> = c
        .run(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[json!("todo")])
                .paginate(None, 2),
        )
        .expect("first page");
    let orders: Vec<i64> = first
        .docs
        .iter()
        .map(|d| d["order"].as_i64().expect("order number"))
        .collect();
    assert_eq!(orders, vec![1, 2]);
    let next_cursor = first.next_cursor.expect("expected a nextCursor");

    // Cursor decodes to [order, _creationTime, _id] of the page's last row.
    let decoded = crate::cursor::decode_cursor(&next_cursor).expect("cursor decodes");
    let last = &first.docs[1];
    assert_eq!(decoded.len(), 3);
    assert_eq!(decoded[0], last["order"]);
    assert_eq!(decoded[1], last["_creationTime"]);
    assert_eq!(decoded[2], last["_id"]);

    let second: Paginated<Value> = c
        .run(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[json!("todo")])
                .paginate(Some(&next_cursor), 2),
        )
        .expect("second page");
    let orders: Vec<i64> = second
        .docs
        .iter()
        .map(|d| d["order"].as_i64().expect("order number"))
        .collect();
    assert_eq!(orders, vec![3, 4]);
}

#[tokio::test]
async fn paginate_rejects_malformed_cursor_as_bad_request() {
    // Ports TS "rejects a malformed (non-base64) cursor with BAD_REQUEST,
    // not INTERNAL" — the codec returns INTERNAL; the harness rewraps it.
    let mut c = new_client();
    seed_items(&mut c, 3, &["todo"]).await;
    let err = c
        .run_query(&Query {
            table: "items".into(),
            paginate: Some(Paginate {
                cursor: Some("not-valid-base64!!!".into()),
                num_items: 3,
            }),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
}

#[tokio::test]
async fn paginate_rejects_cursor_with_mismatched_arity() {
    // Ports TS "rejects a cursor whose arity mismatches the sort columns":
    // no-index query sorts over 2 columns (createdAt, id); 3 values
    // mismatch.
    let mut c = new_client();
    seed_items(&mut c, 3, &["todo"]).await;
    let bad = crate::cursor::encode_cursor(&[json!(1), json!(2), json!(3)]).expect("encode");
    let err = c
        .run_query(&Query {
            table: "items".into(),
            paginate: Some(Paginate {
                cursor: Some(bad),
                num_items: 3,
            }),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("sorts over 2 column(s)"), "got: {err}");
}

#[tokio::test]
async fn paginate_rejects_cursor_whose_created_at_is_not_a_number() {
    // Ports TS "rejects a cursor whose created_at value is not a number":
    // no-index cursor = [createdAt, id]; a non-numeric createdAt fails
    // type-check.
    let mut c = new_client();
    seed_items(&mut c, 3, &["todo"]).await;
    let bad = crate::cursor::encode_cursor(&[
        json!("not-a-number"),
        json!("0123456789abcdef0123456789abcdef"),
    ])
    .expect("encode");
    let err = c
        .run_query(&Query {
            table: "items".into(),
            paginate: Some(Paginate {
                cursor: Some(bad),
                num_items: 3,
            }),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message.contains("created_at must be a number"),
        "got: {err}"
    );
}

#[tokio::test]
async fn paginate_rejects_combination_with_take_count_unique_or_first() {
    // Ports TS "rejects paginate combined with take or count" and extends
    // to unique/first. ENH-028: paginate/take/count/unique/first are all
    // members of the table-driven evaluator's `terminal-exclusive` rule, so
    // every pairing here now surfaces that rule's one generic message.
    let mut c = new_client();
    seed_items(&mut c, 3, &["todo"]).await;
    for (needle, q) in [
        (
            "only one terminal may be set",
            Query {
                table: "items".into(),
                paginate: Some(Paginate {
                    cursor: None,
                    num_items: 3,
                }),
                take: Some(3),
                ..Default::default()
            },
        ),
        (
            "only one terminal may be set",
            Query {
                table: "items".into(),
                paginate: Some(Paginate {
                    cursor: None,
                    num_items: 3,
                }),
                count: true,
                ..Default::default()
            },
        ),
        (
            "only one terminal may be set",
            Query {
                table: "items".into(),
                paginate: Some(Paginate {
                    cursor: None,
                    num_items: 3,
                }),
                unique: true,
                ..Default::default()
            },
        ),
        (
            "only one terminal may be set",
            Query {
                table: "items".into(),
                paginate: Some(Paginate {
                    cursor: None,
                    num_items: 3,
                }),
                first: true,
                ..Default::default()
            },
        ),
    ] {
        let err = c.run_query(&q).unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest, "case '{needle}'");
        assert!(
            err.message.contains(needle),
            "case '{needle}' missing needle: got {}",
            err.message
        );
    }
}

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

// ---- ENH-030: paginate over the ranked terminals ------------------
//
// `paginate` composes with `search`/`vectorSearch`/`hybridSearch` as a peer
// clause: the terminal still runs, only the row cap, the resume predicate, and
// the `{docs, nextCursor?}` envelope come from `paginate`. The engine's
// cursors are its own — minted and consumed here, never compared to the
// server's — so these tests pin the ORDER and the page split, not the bytes.

/// Seed `items` whose `name` (the `by_content` search index's only field)
/// repeats the word `task` the given number of times. The tsquery relevance
/// stand-in counts query lexemes, so the repeat count IS the score. The
/// insertion order deliberately differs from the score order, so a page that
/// fell back to insertion order fails loudly.
async fn seed_task_repeats(c: &mut InMemoryRtDbClient, repeats: &[usize]) {
    for (i, n) in repeats.iter().enumerate() {
        let name = vec!["task"; *n].join(" ");
        let txn = Mutation::new()
            .insert(
                "items",
                json!({ "name": name, "status": "todo", "order": i as i64 }),
            )
            .build();
        c.mutate(&txn, None).await.expect("insert ok");
    }
}

fn names_of(docs: &[Value]) -> Vec<String> {
    docs.iter()
        .map(|d| d["name"].as_str().expect("name string").to_string())
        .collect()
}

#[tokio::test]
async fn paginate_search_walks_ranked_pages_without_overlap() {
    // Pages are non-overlapping, correctly ordered, and their concatenation
    // is the full ranked result: score (query-lexeme count) desc.
    let mut c = new_client();
    seed_task_repeats(&mut c, &[2, 5, 1, 4, 3]).await;
    let (page_sizes, cursors, docs) = walk_pages(&c, |cursor| {
        TableQuery::new("items")
            .search("by_content", "task", ())
            .paginate(cursor, 2)
    })
    .await;
    assert_eq!(page_sizes, vec![2, 2, 1]);
    // Every page but the last carries a cursor; the short last page does not.
    assert!(cursors[..cursors.len() - 1].iter().all(Option::is_some));
    assert!(cursors.last().is_some_and(Option::is_none));

    assert_eq!(
        names_of(&docs),
        vec![
            "task task task task task",
            "task task task task",
            "task task task",
            "task task",
            "task",
        ]
    );
    // No duplicates across pages, and the walk covers exactly the set the
    // unpaginated (unranked, so set-compared) search returns.
    let mut walked = names_of(&docs);
    walked.sort();
    let unpaginated: Vec<Value> = c
        .run(
            &TableQuery::new("items")
                .search("by_content", "task", ())
                .build(),
        )
        .expect("unpaginated search");
    let mut all = names_of(&unpaginated);
    all.sort();
    assert_eq!(walked, all);
}

#[tokio::test]
async fn paginate_search_short_last_page_carries_no_cursor() {
    // Mirrors the `search-paginate-*` corpus pair: 3 hits, numItems 2 ⇒ a
    // full first page with a cursor, then a short page with none.
    let mut c = new_client();
    seed_task_repeats(&mut c, &[2, 1, 3]).await;
    let first: Paginated<Value> = c
        .run(
            &TableQuery::new("items")
                .search("by_content", "task", ())
                .paginate(None, 2),
        )
        .expect("first page");
    assert_eq!(names_of(&first.docs), vec!["task task task", "task task"]);
    let cursor = first.next_cursor.expect("first page has a nextCursor");

    let second: Paginated<Value> = c
        .run(
            &TableQuery::new("items")
                .search("by_content", "task", ())
                .paginate(Some(&cursor), 2),
        )
        .expect("second page");
    assert_eq!(names_of(&second.docs), vec!["task"]);
    assert!(
        second.next_cursor.is_none(),
        "short last page has no cursor"
    );
}

#[tokio::test]
async fn paginate_search_cursor_encodes_score_created_at_and_id() {
    // The ranked keyset is `[score, _creationTime, _id]` of the page's last
    // row — three columns, unlike the btree path's index-field columns.
    let mut c = new_client();
    seed_task_repeats(&mut c, &[1, 3, 2]).await;
    let first: Paginated<Value> = c
        .run(
            &TableQuery::new("items")
                .search("by_content", "task", ())
                .paginate(None, 2),
        )
        .expect("first page");
    let cursor = first.next_cursor.clone().expect("nextCursor");
    let decoded = crate::cursor::decode_cursor(&cursor).expect("cursor decodes");
    let last = &first.docs[1];
    assert_eq!(decoded.len(), 3);
    assert_eq!(decoded[0], json!(2)); // "task task" ⇒ two query lexemes
    assert_eq!(decoded[1], last["_creationTime"]);
    assert_eq!(decoded[2], last["_id"]);
}

#[tokio::test]
async fn paginate_search_trgm_pages_by_similarity() {
    // The trgm arm ranks by query.len()/field.len(), so a SHORTER containing
    // field is more similar — the exact reverse of the tsquery ordering over
    // this seed. Paging must follow the arm's own ranking.
    let mut c = new_client();
    seed_task_repeats(&mut c, &[2, 5, 1, 4, 3]).await;
    let (page_sizes, _cursors, docs) = walk_pages(&c, |cursor| {
        TableQuery::new("items")
            .search(
                "by_content",
                "task",
                SearchOpts {
                    filter: None,
                    mode: Some(SearchMode::Trgm),
                    snippet: None,
                },
            )
            .paginate(cursor, 2)
    })
    .await;
    assert_eq!(page_sizes, vec![2, 2, 1]);
    assert_eq!(
        names_of(&docs),
        vec![
            "task",
            "task task",
            "task task task",
            "task task task task",
            "task task task task task",
        ]
    );
}

#[tokio::test]
async fn paginate_vector_search_pages_the_candidate_pool() {
    // `limit` sizes the candidate POOL, `numItems` slices it into pages. The
    // harness models no vector distance, so the pool's order is the
    // `_creationTime` desc / `_id` desc tie-breaker order — the three newest
    // rows, newest first.
    let mut c = new_client();
    seed_items(&mut c, 5, &["todo"]).await;
    let (page_sizes, cursors, docs) = walk_pages(&c, |cursor| {
        TableQuery::new("items")
            .vector_search("by_embedding", vec![1.0, 0.0, 0.0], 3, ())
            .paginate(cursor, 2)
    })
    .await;
    assert_eq!(page_sizes, vec![2, 1]);
    assert!(cursors[0].is_some());
    assert!(cursors[1].is_none());
    assert_eq!(names_of(&docs), vec!["n5", "n4", "n3"]);
    // Two-column keyset here (no distance to page by), unlike `search`.
    let decoded =
        crate::cursor::decode_cursor(cursors[0].as_deref().expect("cursor")).expect("decodes");
    assert_eq!(decoded.len(), 2);
    assert_eq!(decoded[1], docs[1]["_id"]);
}

#[tokio::test]
async fn paginate_hybrid_search_returns_an_empty_page() {
    // No in-memory ts_rank + distance fusion, so there is nothing to rank and
    // nothing to resume: an empty page with no cursor — as an ENVELOPE, not a
    // bare array (the wire `Paginated` shape every paginate terminal returns).
    let mut c = new_client();
    seed_items(&mut c, 3, &["todo"]).await;
    let page: Paginated<Value> = c
        .run(
            &TableQuery::new("items")
                .hybrid_search("task", vec![1.0, 0.0, 0.0], 5, ())
                .paginate(None, 2),
        )
        .expect("hybrid paginate ok");
    assert!(page.docs.is_empty());
    assert!(page.next_cursor.is_none());
    let raw = c
        .run_query(
            &TableQuery::new("items")
                .hybrid_search("task", vec![1.0, 0.0, 0.0], 5, ())
                .paginate(None, 2),
        )
        .expect("hybrid paginate ok");
    assert_eq!(raw, json!({"docs": []}));
}

#[tokio::test]
async fn paginate_ranked_rejects_a_cursor_of_the_wrong_arity() {
    // A `search` page sorts over three columns; a two-value cursor (the
    // `vectorSearch` shape) is rejected rather than silently mis-resumed.
    let mut c = new_client();
    seed_task_repeats(&mut c, &[1, 2]).await;
    let bad = crate::cursor::encode_cursor(&[json!(1), json!("id")]).expect("encode");
    let err = c
        .run_query(
            &TableQuery::new("items")
                .search("by_content", "task", ())
                .paginate(Some(&bad), 2),
        )
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("sorts over 3 column(s)"), "got: {err}");
}

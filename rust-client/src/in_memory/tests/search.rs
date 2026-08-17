use super::*;

#[tokio::test]
async fn query_search_returns_empty_array_stub() {
    // No in-memory ts_rank — the cascade agrees with the server by
    // returning [] for a valid `search`, while still rejecting conflicting
    // combinations.
    let c = new_client();
    let v = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search("by_content", "hello", ())
                .take(5),
        )
        .expect("search stub");
    assert!(v.is_empty(), "search stub returns []");
}

#[tokio::test]
async fn query_search_rejects_conflicting_terminals() {
    let c = new_client();
    let err = c
        .run_query(&Query {
            table: "items".into(),
            search: Some(crate::wire::SearchQuery {
                index: "by_content".into(),
                query: "hello".into(),
                filter: None,
                mode: None,
                snippet: None,
            }),
            index: Some("by_name".into()),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message.contains("search cannot be combined"),
        "got: {err}"
    );
}

#[tokio::test]
async fn query_search_with_filter_returns_empty_after_narrowing() {
    // ts_rank is unavailable in-memory, so the search stub stays empty; the
    // carried `filter` is still validated and run through `matches_filter`
    // on the (empty) result set, exercising the narrowing path.
    let c = new_client();
    let v = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search(
                    "by_content",
                    "hello",
                    SearchOpts {
                        filter: Some(FilterExpr::Eq {
                            field: "status".into(),
                            value: "done".into(),
                        }),
                        mode: None,
                        snippet: None,
                    },
                )
                .take(5),
        )
        .expect("search with filter narrows cleanly");
    assert!(v.is_empty(), "search stub still returns [] after narrowing");
}

#[tokio::test]
async fn query_search_with_unknown_filter_field_is_bad_request() {
    // The search filter runs through `validate_filter` against the table's
    // declared fields, so an unknown field surfaces as BadRequest before
    // the (stub) result is returned.
    let c = new_client();
    let err = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search(
                    "by_content",
                    "hello",
                    SearchOpts {
                        filter: Some(FilterExpr::Eq {
                            field: "nonexistent".into(),
                            value: "x".into(),
                        }),
                        mode: None,
                        snippet: None,
                    },
                )
                .take(5),
        )
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("nonexistent"), "got: {err}");
}

/// Inserts `(name, status)` rows into `items` in order — the deterministic
/// `new_client()` clock makes later inserts newer (`_creationTime` asc, ids
/// lexicographically asc), which the trgm tie-break tests rely on.
async fn seed_search_items(c: &mut InMemoryRtDbClient, rows: &[(&str, &str)]) {
    for (i, (name, status)) in rows.iter().enumerate() {
        c.mutate(
            &Mutation::new()
                .insert("items", json!({"name": name, "status": status, "order": i}))
                .build(),
            None,
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn query_search_trgm_substring_match_ranks_and_takes() {
    // trgm matches the whole query as a case-insensitive SUBSTRING of an
    // indexed field — "conv" hits "convex"/"Convex"/"convexity appendix",
    // infixes server-side plainto_tsquery stemming cannot match. Ranking is
    // the pinned cross-harness approximation: query.len()/field.len() per
    // containing field, max per doc (shorter field = more similar), then
    // created_at desc, then id desc; `take` truncates after ranking.
    let mut c = new_client();
    seed_search_items(
        &mut c,
        &[
            ("unrelated", "todo"),
            ("convexity appendix", "todo"),
            ("convex", "todo"),
            ("Convex", "todo"),
        ],
    )
    .await;
    let names = |v: &[Value]| -> Vec<String> {
        v.iter()
            .filter_map(|d| d["name"].as_str().map(String::from))
            .collect()
    };

    // Untruncated (take above the match count): all three containing docs,
    // ranked — "Convex"/"convex" tie at 4/6 and the LATER insert wins the
    // created_at tie-break; "convexity appendix" (4/18) ranks last.
    // "unrelated" never matched.
    let all = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search(
                    "by_content",
                    "conv",
                    SearchOpts {
                        filter: None,
                        mode: Some(SearchMode::Trgm),
                        snippet: None,
                    },
                )
                .take(10),
        )
        .expect("trgm search without take");
    assert_eq!(
        names(&all),
        ["Convex", "convex", "convexity appendix"].map(String::from)
    );

    // take(2) truncates the ranked list (drops the lowest-similarity doc).
    let capped = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search(
                    "by_content",
                    "conv",
                    SearchOpts {
                        filter: None,
                        mode: Some(SearchMode::Trgm),
                        snippet: None,
                    },
                )
                .take(2),
        )
        .expect("trgm search with take");
    assert_eq!(names(&capped), ["Convex", "convex"].map(String::from));
}

#[tokio::test]
async fn query_search_trgm_is_case_insensitive_and_index_scoped() {
    // Containment is lowercased on both sides; only the search index's
    // declared fields (here just `name`) are matched — `status` containing
    // the query never hits.
    let mut c = new_client();
    seed_search_items(&mut c, &[("Shiny Widget", "todo")]).await;
    for query in ["widget", "SHINY", "sHiNy"] {
        let v = c
            .run::<Vec<Value>>(
                &TableQuery::new("items")
                    .search(
                        "by_content",
                        query,
                        SearchOpts {
                            filter: None,
                            mode: Some(SearchMode::Trgm),
                            snippet: None,
                        },
                    )
                    .take(5),
            )
            .expect("trgm case-insensitive search");
        assert_eq!(v.len(), 1, "query '{query}' should match");
        assert_eq!(v[0]["name"], json!("Shiny Widget"));
    }
    let none = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search(
                    "by_content",
                    "tod", // substring of status="todo", not of name
                    SearchOpts {
                        filter: None,
                        mode: Some(SearchMode::Trgm),
                        snippet: None,
                    },
                )
                .take(5),
        )
        .expect("trgm index-scoped search");
    assert!(none.is_empty(), "non-indexed fields must not match");
}

#[tokio::test]
async fn query_search_trgm_requires_the_whole_query_as_substring() {
    // trgm matches the query as ONE contiguous substring; the tsquery
    // approximation matches per-token. "con vex" over "convex" therefore
    // diverges: both tokens are substrings (tsquery-mode hit) but the phrase
    // is not (trgm miss).
    let mut c = new_client();
    seed_search_items(&mut c, &[("convex", "todo")]).await;
    let tsquery = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search("by_content", "con vex", ())
                .take(5),
        )
        .expect("default-mode search");
    assert_eq!(
        tsquery.len(),
        1,
        "token-AND approximation matches per-token"
    );
    let trgm = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search(
                    "by_content",
                    "con vex",
                    SearchOpts {
                        filter: None,
                        mode: Some(SearchMode::Trgm),
                        snippet: None,
                    },
                )
                .take(5),
        )
        .expect("trgm search");
    assert!(trgm.is_empty(), "trgm requires the contiguous phrase");
}

#[tokio::test]
async fn query_search_trgm_composes_with_filter() {
    // The carried FilterExpr narrows BEFORE ranking, so filter + take compose
    // exactly as in tsquery mode.
    let mut c = new_client();
    seed_search_items(
        &mut c,
        &[
            ("convex", "done"),
            ("convexity appendix", "done"),
            ("convex ruler", "todo"),
        ],
    )
    .await;
    let v = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search(
                    "by_content",
                    "conv",
                    SearchOpts {
                        filter: Some(FilterExpr::Eq {
                            field: "status".into(),
                            value: "done".into(),
                        }),
                        mode: Some(SearchMode::Trgm),
                        snippet: None,
                    },
                )
                .take(1),
        )
        .expect("trgm search with filter");
    // "convex ruler" matches the substring but is filtered out; among the
    // done docs "convex" (4/6) outranks "convexity appendix" (4/18) and
    // take(1) keeps only it.
    assert_eq!(v.len(), 1);
    assert_eq!(v[0]["name"], json!("convex"));
}

#[tokio::test]
async fn query_search_explicit_tsquery_mode_equals_omitted() {
    // Explicit SearchMode::Tsquery routes through the same default path —
    // results identical to mode omitted (both run against the same unchanged
    // store, so array order is comparable).
    let mut c = new_client();
    seed_search_items(&mut c, &[("alpha beta", "todo"), ("gamma", "todo")]).await;
    let omitted = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search("by_content", "alpha beta", ())
                .take(5),
        )
        .expect("search with mode omitted");
    let explicit = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search(
                    "by_content",
                    "alpha beta",
                    SearchOpts {
                        filter: None,
                        mode: Some(SearchMode::Tsquery),
                        snippet: None,
                    },
                )
                .take(5),
        )
        .expect("search with explicit tsquery");
    assert_eq!(omitted.len(), 1, "token-AND matches the containing doc");
    assert_eq!(omitted, explicit, "explicit tsquery == default");
}

#[tokio::test]
async fn query_search_rejects_empty_query_in_both_modes() {
    // Empty (or whitespace-only) query text is BadRequest before the mode
    // branch — mirrors server `compile_search` and the ts/python harnesses.
    let mut c = new_client();
    seed_search_items(&mut c, &[("convex", "todo")]).await;
    for mode in [None, Some(SearchMode::Tsquery), Some(SearchMode::Trgm)] {
        for query in ["", "   "] {
            let err = c
                .run::<Vec<Value>>(
                    &TableQuery::new("items")
                        .search(
                            "by_content",
                            query,
                            SearchOpts {
                                filter: None,
                                mode,
                                snippet: None,
                            },
                        )
                        .take(5),
                )
                .unwrap_err();
            assert_eq!(err.code, ErrorCode::BadRequest, "mode {mode:?}");
            assert_eq!(err.message, "search query text must not be empty");
        }
    }
}

#[tokio::test]
async fn query_search_requires_a_search_index_in_both_modes() {
    // The index check lives in the shared prologue (server `compile_search`
    // runs it before the mode branch), so a btree index name is rejected for
    // tsquery (the default) too — not just trgm.
    let mut c = new_client();
    seed_search_items(&mut c, &[("convex", "todo")]).await;
    for mode in [None, Some(SearchMode::Tsquery), Some(SearchMode::Trgm)] {
        let err = c
            .run::<Vec<Value>>(
                &TableQuery::new("items")
                    .search(
                        "by_name",
                        "convex",
                        SearchOpts {
                            filter: None,
                            mode,
                            snippet: None,
                        },
                    )
                    .take(5),
            )
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest, "mode {mode:?}");
        assert_eq!(err.message, "search index 'by_name' not found");
    }
}

#[tokio::test]
async fn query_search_phrase_requires_adjacent_words() {
    // A quoted phrase requires the words ADJACENT (FM-31): only the doc where
    // "database notes" appears contiguously matches; the doc carrying the
    // same words apart does not. Unquoted, the same words stay ANDed — so
    // both docs match — pinning plain-query equivalence with the pre-FM-31
    // token-AND behavior through the websearch upgrade (mirrors the server's
    // `phrase_query_requires_adjacent_words`).
    let mut c = new_client();
    seed_search_items(
        &mut c,
        &[
            ("the database notes are great", "todo"),
            ("notes about the database", "todo"),
        ],
    )
    .await;
    let names = |v: &[Value]| -> Vec<String> {
        v.iter()
            .filter_map(|d| d["name"].as_str().map(String::from))
            .collect()
    };

    let phrase = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search("by_content", "\"database notes\"", ())
                .take(5),
        )
        .expect("phrase search");
    assert_eq!(
        names(&phrase),
        ["the database notes are great".to_string()],
        "only the adjacent doc matches a quoted phrase"
    );

    let plain = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search("by_content", "database notes", ())
                .take(5),
        )
        .expect("plain AND search");
    let plain_names = names(&plain);
    assert_eq!(plain_names.len(), 2, "unquoted terms stay ANDed");
    assert!(plain_names.contains(&"the database notes are great".to_string()));
    assert!(plain_names.contains(&"notes about the database".to_string()));
}

#[tokio::test]
async fn query_search_or_operator_unions_alternatives() {
    // The bare word `or` unions alternatives (FM-31): a doc with either term
    // matches; an unrelated doc does not (mirrors the server's
    // `or_operator_unions_alternatives`).
    let mut c = new_client();
    seed_search_items(
        &mut c,
        &[
            ("alpha only", "todo"),
            ("beta only", "todo"),
            ("gamma", "todo"),
        ],
    )
    .await;
    let v = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search("by_content", "alpha or beta", ())
                .take(5),
        )
        .expect("or search");
    let names: Vec<&str> = v.iter().filter_map(|d| d["name"].as_str()).collect();
    assert_eq!(names.len(), 2);
    assert!(names.contains(&"alpha only"));
    assert!(names.contains(&"beta only"));
    assert!(!names.contains(&"gamma"));
}

#[tokio::test]
async fn query_search_minus_operator_excludes_term() {
    // `-term` excludes docs carrying the negated word while keeping the
    // positive one (FM-31; mirrors the server's `minus_operator_excludes_term`).
    let mut c = new_client();
    seed_search_items(
        &mut c,
        &[("database intro", "todo"), ("database cooking", "todo")],
    )
    .await;
    let v = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search("by_content", "database -cooking", ())
                .take(5),
        )
        .expect("minus search");
    let names: Vec<&str> = v.iter().filter_map(|d| d["name"].as_str()).collect();
    assert_eq!(names, ["database intro"]);
}

#[tokio::test]
async fn query_search_snippet_marks_matched_terms() {
    // snippet: true attaches a `_searchSnippet` to every hit — a ≤35-word
    // excerpt with the matched word wrapped in <mark> (FM-31). Omitted or
    // explicitly false, no snippet field appears (mirrors the server's
    // `snippet_returns_highlighted_fragment` /
    // `snippet_false_behaves_like_omitted`).
    let mut c = new_client();
    seed_search_items(&mut c, &[("the database notes are great", "todo")]).await;

    let v = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search(
                    "by_content",
                    "database",
                    SearchOpts {
                        filter: None,
                        mode: None,
                        snippet: Some(true),
                    },
                )
                .take(5),
        )
        .expect("snippet search");
    assert_eq!(v.len(), 1);
    let snippet = v[0]["_searchSnippet"].as_str().expect("snippet string");
    assert!(
        snippet.contains("<mark>database</mark>"),
        "no highlighted term in {snippet}"
    );
    assert!(
        snippet.split_whitespace().count() <= 35,
        "snippet exceeds the word bound: {snippet}"
    );

    // Omitted snippet: no field.
    let plain = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search("by_content", "database", ())
                .take(5),
        )
        .expect("plain search");
    assert_eq!(plain.len(), 1);
    assert!(
        plain[0].get("_searchSnippet").is_none(),
        "snippet field present without snippet: true"
    );

    // Explicit `Some(false)` behaves exactly like omission.
    let off = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search(
                    "by_content",
                    "database",
                    SearchOpts {
                        filter: None,
                        mode: None,
                        snippet: Some(false),
                    },
                )
                .take(5),
        )
        .expect("snippet-false search");
    assert_eq!(off.len(), 1);
    assert!(off[0].get("_searchSnippet").is_none());
}

#[tokio::test]
async fn query_search_snippet_highlights_phrase_queries() {
    // The snippet render highlights the PHRASE words too — like the server's
    // ts_headline, each matched word carries its own <mark>, adjacent for a
    // phrase hit (mirrors `snippet_highlights_phrase_queries`).
    let mut c = new_client();
    seed_search_items(&mut c, &[("the database notes are great", "todo")]).await;
    let v = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search(
                    "by_content",
                    "\"database notes\"",
                    SearchOpts {
                        filter: None,
                        mode: None,
                        snippet: Some(true),
                    },
                )
                .take(5),
        )
        .expect("phrase snippet search");
    assert_eq!(v.len(), 1);
    let snippet = v[0]["_searchSnippet"].as_str().expect("snippet string");
    assert!(
        snippet.contains("<mark>database</mark> <mark>notes</mark>"),
        "phrase words not contiguously highlighted in {snippet}"
    );
}

#[tokio::test]
async fn query_search_snippet_rejected_with_trgm_mode() {
    // snippet + trgm is rejected up front — trgm matches substrings, so
    // there is no tsquery tree to highlight (mirrors the server's
    // `snippet_rejected_with_trgm_mode`).
    let c = new_client();
    let err = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search(
                    "by_content",
                    "conv",
                    SearchOpts {
                        filter: None,
                        mode: Some(SearchMode::Trgm),
                        snippet: Some(true),
                    },
                )
                .take(5),
        )
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("tsquery mode"), "got: {err}");
}

#[tokio::test]
async fn query_vector_search_returns_empty_array_stub() {
    // The TS harness rejects `vectorSearch` combined with any other
    // terminal (including `take`) — unlike `search`, vectorSearch carries
    // its own `limit`. So the bare-stub path is exercised without a
    // trailing terminal.
    let c = new_client();
    let v = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .vector_search("by_embedding", vec![1.0, 0.0, 0.0], 5, ())
                .build(),
        )
        .expect("vector stub");
    assert!(v.is_empty(), "vector stub returns []");
}

#[tokio::test]
async fn query_vector_search_rejects_conflicting_terminals() {
    let c = new_client();
    let err = c
        .run_query(&Query {
            table: "items".into(),
            vector_search: Some(crate::wire::VectorSearchQuery {
                index: "by_embedding".into(),
                vector: vec![1.0],
                limit: 5,
                filter: None,
            }),
            index: Some("by_name".into()),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message.contains("vectorSearch cannot be combined"),
        "got: {err}"
    );
}

#[tokio::test]
async fn query_vector_search_with_filter_returns_empty_after_narrowing() {
    // No in-memory vector ranking, so the vector stub stays empty; the
    // carried `filter` (a `FilterExpr`) is still validated and run through
    // `matches_filter` on the (empty) candidate set, exercising the same
    // narrowing path as the `search` terminal.
    let c = new_client();
    let v = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .vector_search(
                    "by_embedding",
                    vec![1.0, 0.0, 0.0],
                    5,
                    VectorSearchOpts {
                        filter: Some(FilterExpr::Eq {
                            field: "status".into(),
                            value: "done".into(),
                        }),
                    },
                )
                .build(),
        )
        .expect("vector search with filter narrows cleanly");
    assert!(v.is_empty(), "vector stub still returns [] after narrowing");
}

#[tokio::test]
async fn query_vector_search_with_unknown_filter_field_is_bad_request() {
    // The vector-search filter runs through `validate_filter` against the
    // table's declared fields, so an unknown field surfaces as BadRequest
    // before the (stub) result is returned.
    let c = new_client();
    let err = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .vector_search(
                    "by_embedding",
                    vec![1.0, 0.0, 0.0],
                    5,
                    VectorSearchOpts {
                        filter: Some(FilterExpr::Eq {
                            field: "nonexistent".into(),
                            value: "x".into(),
                        }),
                    },
                )
                .build(),
        )
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("nonexistent"), "got: {err}");
}

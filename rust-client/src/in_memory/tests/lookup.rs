//! Lookup expansion — the read-DSL id join, in-memory engine view.
//!
//! Mirrors `server/tests/lookup_test.rs` case-for-case where the harness has
//! an equivalent: single-`Id` and `Array<Id>` lookups (child system fields,
//! declared-order fan-out, `cap` truncation), dangling ids, soft-deleted
//! children, compile-time validation (`unknown table` NOT_FOUND, non-id
//! field, declared-table mismatch, `cap` > 1000, `as` collisions, >4
//! lookups, doc-less terminals), projection naming an alias, and composition
//! with `paginate`/`search`. Gaps with no harness equivalent (per-row
//! ownerField/authorize auth on joined rows) stay on the server suite — the
//! in-memory engine models no principal.

use super::*;

/// users.tagIds is an Array<Id tags>; posts.authorId is an
/// Optional<Id users>. Mirrors `lookup_schema` in `server/tests/lookup_test.rs`.
fn lookup_schema() -> SchemaDef {
    Schema::builder()
        .table(
            "users",
            Table::new()
                .field("name", FieldType::String)
                .field("tagIds", FieldType::array(FieldType::id("tags")))
                .index("by_name", &["name"]),
        )
        .table("tags", Table::new().field("label", FieldType::String))
        .table(
            "posts",
            Table::new()
                .field("title", FieldType::String)
                .field("authorId", FieldType::optional(FieldType::id("users")))
                .index("by_title", &["title"])
                .search_index("s_title", &["title"], None),
        )
        .build()
}

fn lookup_client() -> InMemoryRtDbClient {
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
    client.push_schema(&lookup_schema()).unwrap();
    client
}

/// Insert one doc and return its id. Array fields are required
/// (non-Optional), so default the users table tagIds to empty when the caller
/// does not care about them (mirrors the server test insert helper).
async fn put(c: &mut InMemoryRtDbClient, table: &str, mut doc: Value) -> String {
    if table == "users" && doc.get("tagIds").is_none() {
        doc["tagIds"] = json!([]);
    }
    let results = c
        .mutate(&Mutation::new().insert(table, doc).build(), None)
        .await
        .expect("insert ok");
    match &results[0] {
        StepResult::Insert { id } => id.clone(),
        other => panic!("expected Insert, got {other:?}"),
    }
}

fn author_lookup() -> LookupSpec {
    LookupSpec {
        field: "authorId".into(),
        table: "users".into(),
        r#as: "author".into(),
        cap: None,
    }
}

fn tags_lookup(cap: Option<u32>) -> LookupSpec {
    LookupSpec {
        field: "tagIds".into(),
        table: "tags".into(),
        r#as: "tags".into(),
        cap,
    }
}

async fn seed_ann_and_post(c: &mut InMemoryRtDbClient) -> String {
    let ann = put(c, "users", json!({"name": "Ann"})).await;
    put(c, "posts", json!({"title": "p1", "authorId": ann})).await;
    ann
}

// ---- happy paths ----------------------------------------------------------

#[tokio::test]
async fn single_id_lookup_joins_author_with_child_system_fields() {
    let mut c = lookup_client();
    seed_ann_and_post(&mut c).await;
    put(&mut c, "posts", json!({"title": "p2"})).await;

    let docs = c
        .run_query(&Query {
            table: "posts".into(),
            lookups: vec![author_lookup()],
            ..Default::default()
        })
        .expect("query ok");
    let docs = docs.as_array().expect("collect result is an array");
    assert_eq!(docs.len(), 2);
    for doc in docs {
        let author = &doc["author"];
        if doc["title"] == "p1" {
            assert_eq!(author["name"], "Ann");
            // Child system fields ride the joined doc, merged like the
            // parents (server `merge_doc`).
            assert!(author["_id"].is_string());
            assert!(author["_creationTime"].is_number());
            assert!(author["_version"].is_number());
        } else {
            assert!(author.is_null(), "absent authorId reads as null");
        }
    }
}

#[tokio::test]
async fn array_id_lookup_preserves_order_and_caps() {
    let mut c = lookup_client();
    let mut tag_ids = Vec::new();
    for label in ["t1", "t2", "t3"] {
        tag_ids.push(put(&mut c, "tags", json!({"label": label})).await);
    }
    put(&mut c, "users", json!({"name": "U", "tagIds": tag_ids})).await;

    let docs = c
        .run_query(&Query {
            table: "users".into(),
            lookups: vec![tags_lookup(Some(2))],
            ..Default::default()
        })
        .expect("query ok");
    let docs = docs.as_array().expect("collect result is an array");
    assert_eq!(docs.len(), 1);
    let tags = docs[0]["tags"].as_array().expect("tags array");
    // Declared id-array order, truncated to the first cap entries.
    assert_eq!(tags.len(), 2);
    assert_eq!(tags[0]["label"], "t1");
    assert_eq!(tags[1]["label"], "t2");
}

#[tokio::test]
async fn dangling_ids_omit_not_error() {
    let mut c = lookup_client();
    // Well-formed 32-hex ids that reference no row.
    put(
        &mut c,
        "posts",
        json!({"title": "p1", "authorId": "e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0"}),
    )
    .await;
    put(
        &mut c,
        "users",
        json!({"name": "U", "tagIds": ["d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1"]}),
    )
    .await;

    let docs = c
        .run_query(&Query {
            table: "posts".into(),
            lookups: vec![author_lookup()],
            ..Default::default()
        })
        .expect("query ok");
    let docs = docs.as_array().unwrap();
    assert!(
        docs[0]["author"].is_null(),
        "dangling single id reads as null"
    );

    let docs = c
        .run_query(&Query {
            table: "users".into(),
            lookups: vec![tags_lookup(None)],
            ..Default::default()
        })
        .expect("query ok");
    let docs = docs.as_array().unwrap();
    assert!(
        docs[0]["tags"].as_array().expect("tags").is_empty(),
        "dangling array ids are dropped"
    );
}

#[tokio::test]
async fn soft_deleted_child_reads_as_absent() {
    let mut c = lookup_client();
    let ann = seed_ann_and_post(&mut c).await;
    // FM-33: a soft-deleted child row is invisible to the batched fetch, the
    // same way it is invisible to a parent read — the single form reads null.
    c.mutate(&Mutation::new().delete("users", &ann).build(), None)
        .await
        .expect("soft delete ok");

    let docs = c
        .run_query(&Query {
            table: "posts".into(),
            lookups: vec![author_lookup()],
            ..Default::default()
        })
        .expect("query ok");
    let docs = docs.as_array().unwrap();
    assert!(
        docs[0]["author"].is_null(),
        "soft-deleted child reads as null"
    );
}

// ---- compile-time validation and error mapping ----------------------------

#[tokio::test]
async fn unknown_lookup_table_is_not_found() {
    let c = lookup_client();
    let err = c
        .run_query(&Query {
            table: "posts".into(),
            lookups: vec![LookupSpec {
                field: "authorId".into(),
                table: "ghost".into(),
                r#as: "author".into(),
                cap: None,
            }],
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::NotFound);
}

#[tokio::test]
async fn field_not_id_is_bad_request() {
    let c = lookup_client();
    let err = c
        .run_query(&Query {
            table: "posts".into(),
            lookups: vec![LookupSpec {
                field: "title".into(),
                table: "users".into(),
                r#as: "author".into(),
                cap: None,
            }],
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
}

#[tokio::test]
async fn declared_table_mismatch_is_bad_request() {
    let c = lookup_client();
    let err = c
        .run_query(&Query {
            table: "posts".into(),
            lookups: vec![LookupSpec {
                field: "authorId".into(),
                table: "tags".into(),
                r#as: "author".into(),
                cap: None,
            }],
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
}

#[tokio::test]
async fn cap_over_1000_is_bad_request() {
    let c = lookup_client();
    let err = c
        .run_query(&Query {
            table: "users".into(),
            lookups: vec![tags_lookup(Some(1001))],
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
}

#[tokio::test]
async fn alias_collisions_are_bad_request() {
    let c = lookup_client();
    // Collides with a declared field.
    let err = c
        .run_query(&Query {
            table: "posts".into(),
            lookups: vec![LookupSpec {
                r#as: "title".into(),
                ..author_lookup()
            }],
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    // Underscore-prefixed alias (system-field namespace).
    let err = c
        .run_query(&Query {
            table: "posts".into(),
            lookups: vec![LookupSpec {
                r#as: "_id".into(),
                ..author_lookup()
            }],
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    // Duplicate alias across two lookups.
    let err = c
        .run_query(&Query {
            table: "posts".into(),
            lookups: vec![author_lookup(), author_lookup()],
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
}

#[tokio::test]
async fn more_than_four_lookups_is_bad_request() {
    let c = lookup_client();
    let err = c
        .run_query(&Query {
            table: "posts".into(),
            lookups: vec![
                author_lookup(),
                author_lookup(),
                author_lookup(),
                author_lookup(),
                author_lookup(),
            ],
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
}

#[tokio::test]
async fn lookups_with_docless_terminals_are_bad_request() {
    let c = lookup_client();
    for mutator in [
        |q: &mut Query| q.count = true,
        |q: &mut Query| q.distinct = true,
        |q: &mut Query| {
            q.aggregate = Some(AggregateSpec {
                op: Some(AggregateOp::Count),
                aggregates: None,
                group_by: crate::wire::GroupBy::Bool(false),
            })
        },
    ] {
        let mut q = Query {
            table: "posts".into(),
            lookups: vec![author_lookup()],
            ..Default::default()
        };
        mutator(&mut q);
        let err = c.run_query(&q).unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest, "query: {q:?}");
    }
}

// ---- projection + terminal composition ------------------------------------

#[tokio::test]
async fn projection_may_name_the_alias_and_omitting_drops_it() {
    let mut c = lookup_client();
    seed_ann_and_post(&mut c).await;

    // Alias kept; unlisted parent field (title) dropped.
    let docs = c
        .run_query(&Query {
            table: "posts".into(),
            lookups: vec![author_lookup()],
            fields: Some(vec!["author".into()]),
            ..Default::default()
        })
        .expect("query ok");
    let docs = docs.as_array().unwrap();
    assert_eq!(docs[0]["author"]["name"], "Ann");
    assert!(docs[0].get("title").is_none());

    // Omitting the alias from fields drops the joined docs.
    let docs = c
        .run_query(&Query {
            table: "posts".into(),
            lookups: vec![author_lookup()],
            fields: Some(vec!["title".into()]),
            ..Default::default()
        })
        .expect("query ok");
    let docs = docs.as_array().unwrap();
    assert_eq!(docs[0]["title"], "p1");
    assert!(docs[0].get("author").is_none());
}

#[tokio::test]
async fn composes_with_paginate_and_search() {
    let mut c = lookup_client();
    let ann = seed_ann_and_post(&mut c).await;
    for title in ["alpha", "bravo", "charlie"] {
        put(&mut c, "posts", json!({"title": title, "authorId": ann})).await;
    }

    // paginate: expansion applies to the returned page docs only; cursor
    // semantics are unchanged.
    let page = c
        .run_query(&Query {
            table: "posts".into(),
            lookups: vec![author_lookup()],
            paginate: Some(Paginate {
                cursor: None,
                num_items: 2,
            }),
            ..Default::default()
        })
        .expect("paginate ok");
    assert_eq!(page["docs"].as_array().expect("docs").len(), 2);
    for doc in page["docs"].as_array().unwrap() {
        assert_eq!(doc["author"]["name"], "Ann");
    }
    assert!(page.get("nextCursor").is_some(), "has-more cursor survives");

    // search + lookups: ranked hits expand too (cursor derivation never reads
    // lookup fields, so paging stays sound).
    let docs = c
        .run_query(&Query {
            table: "posts".into(),
            search: Some(crate::wire::SearchQuery {
                index: "s_title".into(),
                query: "alpha".into(),
                filter: None,
                mode: None,
                snippet: None,
            }),
            lookups: vec![author_lookup()],
            ..Default::default()
        })
        .expect("search ok");
    let docs = docs.as_array().unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0]["author"]["name"], "Ann");
}

// ---- DSL builder ----------------------------------------------------------

#[tokio::test]
async fn builder_accumulates_lookups_and_serializes_the_wire_shape() {
    let q: Query = TableQuery::new("posts")
        .lookup("authorId", "users", "author", None)
        .lookup("authorId", "users", "alsoAuthor", Some(20))
        .into();
    let v = serde_json::to_value(&q).expect("serialize");
    // camelCase, the `as` rename, cap omitted when None (mirrors core
    // `lookups_omit_on_empty_and_carry_as_rename`).
    assert_eq!(
        v["lookups"],
        json!([
            {"field": "authorId", "table": "users", "as": "author"},
            {"field": "authorId", "table": "users", "as": "alsoAuthor", "cap": 20}
        ])
    );
    let bare: Query = TableQuery::new("posts").into();
    assert!(
        serde_json::to_value(&bare)
            .expect("serialize")
            .get("lookups")
            .is_none(),
        "empty lookups omit from the wire"
    );

    // The built query runs end to end through the in-memory engine.
    let mut c = lookup_client();
    seed_ann_and_post(&mut c).await;
    let docs = c.run_query(&q).expect("builder query ok");
    let docs = docs.as_array().unwrap();
    assert_eq!(docs[0]["author"]["name"], "Ann");
    assert_eq!(docs[0]["alsoAuthor"]["name"], "Ann");
}

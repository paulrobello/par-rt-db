//! ENH-033 micro-benchmark: `schema::validate_doc`, the full-document
//! validator every write path runs (insert/replace/upsert-insert, and every
//! declared field on patch's merged result). Pure, no Postgres. Benchmarks a
//! ~1 KB and a ~10 KB document against the same table schema.

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use rtdb_server::schema::{SchemaDef, validate_doc};

fn bench_schema() -> SchemaDef {
    serde_json::from_value(serde_json::json!({"tables":{
        "docs":{
            "fields":{
                "title":{"type":"string"},
                "body":{"type":"string"},
                "status":{"type":"union","variants":[
                    {"type":"literal","value":"draft"},
                    {"type":"literal","value":"published"},
                    {"type":"literal","value":"archived"}]},
                "tags":{"type":"array","element":{"type":"string"}},
                "score":{"type":"number"},
                "meta":{"type":"object","fields":{
                    "author":{"type":"string"},
                    "source":{"type":"string"},
                    "language":{"type":"string"}
                }},
                "extra":{"type":"record","value":{"type":"string"}}
            }
        }
    }}))
    .expect("parse bench schema")
}

/// A document of approximately `body_len` bytes, built by padding the `body`
/// field with a repeated filler string plus a handful of realistic sibling
/// fields (tags array, nested object, dynamic-key record).
fn bench_doc(body_len: usize) -> serde_json::Map<String, serde_json::Value> {
    let body: String = "the quick brown fox jumps over the lazy dog. "
        .chars()
        .cycle()
        .take(body_len)
        .collect();
    serde_json::json!({
        "title": "A representative document title",
        "body": body,
        "status": "published",
        "tags": ["rust", "postgres", "benchmark", "realtime", "db"],
        "score": 4.5,
        "meta": {
            "author": "ada",
            "source": "bench-harness",
            "language": "en",
        },
        "extra": {
            "k1": "v1",
            "k2": "v2",
            "k3": "v3",
        },
    })
    .as_object()
    .expect("bench doc is an object")
    .clone()
}

fn bench_validate_doc(c: &mut Criterion) {
    let schema = bench_schema();
    let table = schema.tables.get("docs").expect("docs table declared");

    let doc_1kb = bench_doc(850);
    let doc_10kb = bench_doc(9_600);

    let mut group = c.benchmark_group("validate_doc");
    group.bench_function("doc_1kb", |b| {
        b.iter(|| {
            let result = validate_doc(black_box(table), black_box(&doc_1kb));
            black_box(result).expect("validate 1kb doc");
        });
    });
    group.bench_function("doc_10kb", |b| {
        b.iter(|| {
            let result = validate_doc(black_box(table), black_box(&doc_10kb));
            black_box(result).expect("validate 10kb doc");
        });
    });
    group.finish();
}

criterion_group!(benches, bench_validate_doc);
criterion_main!(benches);

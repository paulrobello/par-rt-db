//! ENH-033 micro-benchmark: `value_expr::eval_value_expr`, the in-memory
//! interpreter run on every write for a table's computed fields (ENH-028). No
//! Postgres, no schema — the interpreter takes a doc `Map` and a `PrincipalCtx`
//! directly. Exercises the representative computed-field expression shapes:
//! field reads, string concat, arithmetic, coalesce, string casing, and a
//! `Case`/`when` predicate branch.

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use rtdb_server::auth::PrincipalCtx;
use rtdb_server::query::FilterExpr;
use rtdb_server::value_expr::{CaseWhen, ValueExpr, eval_value_expr};
use serde_json::json;

fn bench_doc() -> serde_json::Map<String, serde_json::Value> {
    json!({
        "firstName": "Ada",
        "lastName": "Lovelace",
        "status": "active",
        "views": 42,
        "shares": 7,
        "bio": null,
        "fallbackBio": "no bio yet",
    })
    .as_object()
    .expect("bench doc is an object")
    .clone()
}

/// ~8 representative computed-field expressions.
fn bench_exprs() -> Vec<(&'static str, ValueExpr)> {
    vec![
        ("field_read", ValueExpr::field("firstName")),
        (
            "concat",
            ValueExpr::concat([
                ValueExpr::field("firstName"),
                ValueExpr::literal(" "),
                ValueExpr::field("lastName"),
            ]),
        ),
        (
            "arithmetic",
            ValueExpr::add(
                ValueExpr::mul(ValueExpr::field("views"), ValueExpr::literal(2)),
                ValueExpr::field("shares"),
            ),
        ),
        (
            "coalesce",
            ValueExpr::coalesce([ValueExpr::field("bio"), ValueExpr::field("fallbackBio")]),
        ),
        (
            "lower_upper",
            ValueExpr::lower(ValueExpr::upper(ValueExpr::field("firstName"))),
        ),
        ("trim", ValueExpr::trim(ValueExpr::literal("  padded  "))),
        ("now", ValueExpr::now()),
        (
            "case_when",
            ValueExpr::case(
                vec![
                    CaseWhen {
                        when: FilterExpr::Eq {
                            field: "status".into(),
                            value: json!("active"),
                        },
                        then: ValueExpr::literal("live"),
                    },
                    CaseWhen {
                        when: FilterExpr::Eq {
                            field: "status".into(),
                            value: json!("paused"),
                        },
                        then: ValueExpr::literal("on-hold"),
                    },
                ],
                ValueExpr::literal("unknown"),
            ),
        ),
    ]
}

fn bench_eval_value_expr(c: &mut Criterion) {
    let doc = bench_doc();
    let ctx = PrincipalCtx::bypass();
    let exprs = bench_exprs();

    let mut group = c.benchmark_group("eval_value_expr");
    for (name, expr) in &exprs {
        group.bench_function(*name, |b| {
            b.iter(|| {
                let result = eval_value_expr(
                    black_box(expr),
                    black_box(&doc),
                    black_box(1_700_000_000_000_i64),
                    black_box(&ctx),
                );
                black_box(result).expect("eval value expr");
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_eval_value_expr);
criterion_main!(benches);

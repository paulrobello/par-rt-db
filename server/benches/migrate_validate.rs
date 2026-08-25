//! ENH-033 micro-benchmark: `migrate::plan_migration`, which folds each
//! directive through `validate_one` in order (pure, no Postgres) and derives
//! the resulting schema. Benchmarks an 8-directive migration plan (one of
//! every `Directive` kind) against a ~30-table schema — the shape a real
//! `POST /admin/db/{db}/migrate` dry-run validates before touching any row.

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use rtdb_server::migrate::{Directive, ExprSource, plan_migration};
use rtdb_server::schema::SchemaDef;
use rtdb_server::value_expr::ValueExpr;

const TABLE_COUNT: usize = 30;

/// `TABLE_COUNT` tables, each with a `name`/`value`/`status` field trio and a
/// `by_name` btree index — enough surface for one of every `Directive` kind
/// to apply to a distinct table.
fn bench_schema() -> SchemaDef {
    let mut tables = serde_json::Map::new();
    for i in 0..TABLE_COUNT {
        tables.insert(
            format!("table{i}"),
            serde_json::json!({
                "fields": {
                    "name": {"type": "string"},
                    "value": {"type": "number"},
                    "status": {"type": "string"},
                },
                "indexes": [{"name": "by_name", "fields": ["name"]}],
            }),
        );
    }
    serde_json::from_value(serde_json::json!({"tables": tables})).expect("parse bench schema")
}

/// One directive of every kind, each targeting a distinct table so none
/// depend on an earlier directive's result.
fn bench_directives() -> Vec<Directive> {
    vec![
        Directive::RenameField {
            table: "table0".into(),
            from: "name".into(),
            to: "title".into(),
        },
        Directive::RenameTable {
            from: "table1".into(),
            to: "table1_renamed".into(),
        },
        Directive::ChangeType {
            table: "table2".into(),
            field: "value".into(),
            to: rtdb_server::schema::FieldType::String,
            cast: rtdb_server::migrate::Cast::ToString,
            default: None,
        },
        Directive::DropField {
            table: "table3".into(),
            field: "status".into(),
        },
        Directive::DropTable {
            name: "table4".into(),
        },
        Directive::DropIndex {
            table: "table5".into(),
            name: "by_name".into(),
        },
        Directive::SetDefault {
            table: "table6".into(),
            field: "status".into(),
            value: serde_json::json!("active"),
        },
        Directive::EvalExpr {
            table: "table7".into(),
            set: "status".into(),
            expr: ExprSource::Typed(ValueExpr::literal("migrated")),
            where_clause: None,
        },
    ]
}

fn bench_plan_migration(c: &mut Criterion) {
    let schema = bench_schema();
    let directives = bench_directives();

    c.bench_function("migrate_plan_migration_8_directives", |b| {
        b.iter(|| {
            let result = plan_migration(black_box(&schema), black_box(&directives));
            black_box(result).expect("plan migration");
        });
    });
}

criterion_group!(benches, bench_plan_migration);
criterion_main!(benches);

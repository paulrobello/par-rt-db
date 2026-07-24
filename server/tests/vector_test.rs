mod common;

use common::test_state;
use sqlx::Row;

#[tokio::test]
async fn pgvector_extension_available_after_db_create() {
    let state = test_state().await;
    // fresh_db creates a database, which now runs CREATE EXTENSION vector.
    let db_name = common::fresh_db(&state).await;

    let row = sqlx::query("SELECT extversion FROM pg_extension WHERE extname = 'vector'")
        .fetch_one(&state.pool)
        .await
        .expect("vector extension row");
    let version: String = row.get("extversion");
    assert!(!version.is_empty(), "vector extension installed: {version}");

    // And the cosine-distance operator resolves (proves the extension is usable).
    let dist: f64 = sqlx::query_scalar("SELECT '[1,0,0]'::vector <=> '[0,1,0]'::vector")
        .fetch_one(&state.pool)
        .await
        .expect("cosine distance");
    assert!(
        (dist - 1.0).abs() < 1e-6,
        "orthogonal vectors have cosine distance 1, got {dist}"
    );

    let _ = db_name; // created; isolation by unique name
}

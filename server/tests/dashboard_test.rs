mod common;

// Seeding lowercases emails, is idempotent, and stores them with a NULL github_id.
#[tokio::test]
async fn seed_admin_emails_lowercases_and_is_idempotent() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let pool = state.pool.clone();

    rtdb_server::auth::seed_admin_emails(
        &pool,
        &[
            "Foo@Bar.com".to_string(),
            "  ".to_string(),
            "a@b.com".to_string(),
        ],
    )
    .await?;
    // Re-seed the same address: ON CONFLICT DO NOTHING keeps it a single row.
    rtdb_server::auth::seed_admin_emails(&pool, &["foo@bar.com".to_string()]).await?;

    let rows: Vec<(String, Option<i64>)> =
        sqlx::query_as("SELECT email, github_id FROM rtdb_auth.admins ORDER BY email")
            .fetch_all(&pool)
            .await?;
    assert_eq!(
        rows,
        vec![
            ("a@b.com".to_string(), None),
            ("foo@bar.com".to_string(), None),
        ]
    );
    Ok(())
}

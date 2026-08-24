//! Integration tests for on-the-fly image transforms on storage serve (ENH-014).
//!
//! These exercise the HTTP wiring only: `serve_public_handler`/`serve_authed_handler`
//! extract query params and route them through `serve_bytes`, which delegates to
//! `TransformCache::get_or_transform` or falls back to a raw passthrough serve.
//! The decode → resize → encode pipeline itself is unit-tested in
//! `src/image_transform.rs`.

use crate::common::{admin_post, spawn_app, test_state, wrap_test_db};
use std::net::SocketAddr;

use axum::http::StatusCode;
use image::{GenericImageView, ImageBuffer, Rgba};
use serde_json::json;

/// Mint a machine token for `db` via the admin route and return the bare token
/// string. Mirrors the private helper in `storage_test.rs` (private per-file, so
/// duplicated here rather than promoted to `common`).
async fn mint_token(addr: SocketAddr, db: &str) -> String {
    let resp = admin_post(
        addr,
        "/admin/mint-token",
        json!({"db": db, "name": "test-token"}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.expect("parse mint-token response");
    body["token"].as_str().expect("token").to_string()
}

/// Build a real PNG of `w`x`h` (all transparent), upload it with
/// `content-type: image/png`, return the server-assigned id.
async fn upload_png(addr: &SocketAddr, db: &str, token: &str, w: u32, h: u32) -> String {
    let img: ImageBuffer<Rgba<u8>, Vec<u8>> = ImageBuffer::new(w, h);
    let mut body = Vec::new();
    image::DynamicImage::ImageRgba8(img)
        .write_to(
            &mut std::io::Cursor::new(&mut body),
            image::ImageFormat::Png,
        )
        .expect("encode png");
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/storage/{db}"))
        .bearer_auth(token)
        .header("content-type", "image/png")
        .body(body)
        .send()
        .await
        .expect("upload");
    assert_eq!(resp.status(), StatusCode::OK);
    resp.json::<serde_json::Value>().await.expect("json")["id"]
        .as_str()
        .expect("id")
        .to_string()
}

/// Create a bare (schema-less) test database. Storage uses its own side table
/// (created by `db::create_database`), so no document schema is needed; using
/// `fresh_db` would unnecessarily push the kanban fixture.
async fn bare_db(state: &rtdb_server::AppState) -> crate::common::TestDb {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &name)
        .await
        .expect("create database");
    wrap_test_db(name)
}

#[tokio::test]
async fn serve_transform_resizes_png() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = bare_db(&state).await;
    let token = mint_token(addr, &db).await;
    let id = upload_png(&addr, &db, &token, 400, 200).await;

    let r = reqwest::get(format!("http://{addr}/storage/{id}?w=100&h=100&fit=cover")).await?;
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(r.headers().get("content-type").unwrap(), "image/png");
    assert_eq!(
        r.headers().get("cache-control").unwrap(),
        "public, max-age=31536000, immutable"
    );
    let out = r.bytes().await?;
    assert_eq!(image::load_from_memory(&out)?.dimensions(), (100, 100));
    Ok(())
}

#[tokio::test]
async fn serve_passthrough_when_no_params() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = bare_db(&state).await;
    let token = mint_token(addr, &db).await;
    let id = upload_png(&addr, &db, &token, 40, 40).await;

    let r = reqwest::get(format!("http://{addr}/storage/{id}")).await?;
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(r.headers().get("content-type").unwrap(), "image/png");
    assert_eq!(
        r.headers().get("cache-control").unwrap(),
        "public, max-age=31536000, immutable"
    );
    Ok(())
}

#[tokio::test]
async fn serve_bad_params_400() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = bare_db(&state).await;
    let token = mint_token(addr, &db).await;
    let id = upload_png(&addr, &db, &token, 40, 40).await;

    let r = reqwest::get(format!("http://{addr}/storage/{id}?w=99999")).await?;
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = r.json().await?;
    assert_eq!(body["code"], "BAD_REQUEST");
    Ok(())
}

#[tokio::test]
async fn serve_non_image_with_params_returns_raw() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = bare_db(&state).await;
    let token = mint_token(addr, &db).await;

    let payload = b"definitely not an image";
    let up = reqwest::Client::new()
        .post(format!("http://{addr}/api/storage/{db}"))
        .bearer_auth(&token)
        .header("content-type", "application/pdf")
        .body(payload.to_vec())
        .send()
        .await?;
    let id = up.json::<serde_json::Value>().await?["id"]
        .as_str()
        .unwrap()
        .to_string();

    let r = reqwest::get(format!("http://{addr}/storage/{id}?w=50")).await?;
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(r.bytes().await?, &payload[..]);
    Ok(())
}

#[tokio::test]
async fn authed_serve_also_transforms() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = bare_db(&state).await;
    let token = mint_token(addr, &db).await;
    let id = upload_png(&addr, &db, &token, 200, 100).await;

    let r = reqwest::Client::new()
        .get(format!(
            "http://{addr}/api/storage/{db}/{id}?w=50&format=jpeg"
        ))
        .bearer_auth(&token)
        .send()
        .await?;
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(r.headers().get("content-type").unwrap(), "image/jpeg");
    Ok(())
}

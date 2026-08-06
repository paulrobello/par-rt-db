# On-the-Fly Image Transforms Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add server-side image transforms (resize / quality / format) to the storage serve routes so one stored image yields any derived size on demand, with an in-memory cache — Convex parity (ENH-014).

**Architecture:** Both serve routes (`GET /storage/{id}` public, `GET /api/storage/{db}/{id}` authed) funnel through `serve_bytes`. With no transform query params they pass raw bytes through unchanged; with `?w=&h=&fit=&q=&format=` they decode → resize → re-encode under a bounded-concurrency `spawn_blocking`, caching derived bytes in `moka`. A new `server/src/image_transform.rs` holds the pure transform logic + cache; `AppState` holds an `Arc<TransformCache>`. HTTP-only — no protocol/wire/WS/committer change. The three clients gain a URL helper; the dashboard storage browser gains a size control.

**Tech Stack:** Rust server (axum 0.8, tokio) + `image` 0.25 (pure-Rust, default-features off) + `moka` 0.12; ts-client (bun/vitest/biome); rust-client (cargo); python-client (uv/pyright/ruff); dashboard (Vite/React/CSS Modules/bun).

## Global Constraints

- **Definition of done:** `make dev-db-up && make checkall` is green (env-drift-check + fmt-check + clippy `-D warnings` + typecheck + tests). Every task's verification implicitly includes this at the end.
- **No `unwrap()`/`expect()` outside `#[cfg(test)]`.** Zero clippy warnings.
- **`image` crate, pure-Rust, default-features off:** `image = { version = "0.25", default-features = false, features = ["png", "jpeg", "gif", "bmp", "webp"] }`. **Output formats are JPEG + PNG only** (no WebP/AVIF encode in v1).
- **Exact param vocabulary (server + all clients must match verbatim):** `w`, `h` (1..=MAX_DIM); `fit` ∈ `cover|contain|scale-down`; `q` (1..=100, JPEG only); `format` ∈ `jpeg|png|auto`.
- **Passthrough:** when no effective transform is requested, serve raw bytes with zero decode overhead. Transforms are a no-op when `RTDB_IMAGE_TRANSFORMS_ENABLED=false`.
- **`Cache-Control: public, max-depth=31536000, immutable`** on both raw and transformed serve (blobs are write-once).
- **env-drift rule:** every new `RTDB_*` var must appear in `docker-compose.yml`'s `environment:` block (indent 6, `NAME: ${NAME:-default}`); should appear in `.env.example` (`NAME=default`). `scripts/env-drift-check.sh` (run by `make checkall`) fails otherwise.
- **Clients mirror the core:** server + ts/rust/python each get the helper + tests. The server is the authority and re-validates every param.
- **Working dir for commands:** run cargo from `server/` (or `-p rtdb-server`), bun from `ts-client/` or `dashboard/`, uv from `python-client/`. Prefer `make -C /Users/probello/Repos/par-rt-db <target>` from anywhere.

## File Structure

- **Create** `server/src/image_transform.rs` — pure transform logic + `TransformCache`. Declared in `server/src/lib.rs`.
- **Modify** `server/Cargo.toml` — add `image`, `moka`.
- **Modify** `server/src/config.rs` — six `RTDB_IMAGE_*` fields.
- **Modify** `server/src/metrics.rs` — transform counters.
- **Modify** `server/src/lib.rs` — `AppState.image` field + construction.
- **Modify** `server/src/http_api.rs` — serve handlers + `serve_bytes`.
- **Modify** `server/tests/common/mod.rs` — `test_config()` literal.
- **Create** `server/tests/image_transform_test.rs` — integration tests.
- **Modify** `ts-client/src/{http,client,index}.ts` + `ts-client/tests/storage.test.ts`.
- **Modify** `rust-client/src/{http,lib}.rs`.
- **Modify** `python-client/src/par_rt_db/{http_client,aio_http_client,in_memory}.py` + tests.
- **Modify** `dashboard/src/pages/StoragePage.{tsx,module.css}` + `StoragePage.test.tsx`.
- **Modify** `.env.example`, `docker-compose.yml`, `FEATURE_MATRIX.md`, `CLAUDE.md`, `server/src/storage.rs` (module doc), `ENHANCEMENTS.md`.

---

## Shared type contract (defined in Task 2, consumed by Tasks 4–5)

```rust
// server/src/image_transform.rs
pub enum Fit { Cover, Contain, ScaleDown }            // fit=cover|contain|scale-down
pub enum OutFormat { Jpeg, Png, Auto }                // format=jpeg|png|auto
pub struct TransformParams { pub w: Option<u32>, pub h: Option<u32>, pub fit: Fit, pub q: Option<u8>, pub format: OutFormat }
pub struct TransformConfig { pub enabled: bool, pub max_dim: u32, pub max_pixels: u64, pub default_quality: u8 }
pub enum TransformError { NotImage, TooLarge, Internal(String) }
pub fn apply(bytes: &[u8], content_type: Option<&str>, params: &TransformParams, cfg: &TransformConfig)
    -> Result<(Vec<u8>, &'static str), TransformError>;
// Task 4 adds:
pub struct CachedImage { pub bytes: Arc<[u8]>, pub content_type: &'static str }
pub enum Resolved { Transformed(CachedImage), Raw { bytes: Arc<[u8]>, content_type: String } }
pub struct TransformCache { /* moka + semaphore + cfg + metrics */ }
```

---

### Task 1: Server deps + config + env-drift (plumbing; compiles, no behavior)

**Files:**
- Modify: `server/Cargo.toml`
- Modify: `server/src/config.rs:16-94` (fields) + `:242-276` (literal)
- Modify: `server/tests/common/mod.rs:11-48` (`test_config()` literal, ends `ttl_batch: 5000,`)
- Modify: `.env.example` + `docker-compose.yml` `environment:` block

**Interfaces:** Produces the six `Config` fields consumed by Task 2/4.

- [ ] **Step 1: Add deps to `server/Cargo.toml` `[dependencies]`**

```toml
image = { version = "0.25", default-features = false, features = ["png", "jpeg", "gif", "bmp", "webp"] }
moka = "0.12"
```

- [ ] **Step 2: Add six fields to `Config` (`config.rs`) after `ttl_batch`**

```rust
    // On-the-fly image transforms on storage serve (ENH-014). RTDB_IMAGE_*.
    // Boot-time operational knobs (not admin-mutable). All optional w/ defaults.
    pub image_transforms_enabled: bool, // RTDB_IMAGE_TRANSFORMS_ENABLED, default true
    pub image_max_dim: u32,             // RTDB_IMAGE_MAX_DIM, default 2048
    pub image_max_pixels: u64,          // RTDB_IMAGE_MAX_PIXELS, default 25_000_000
    pub image_cache_bytes: u64,         // RTDB_IMAGE_CACHE_BYTES, default 256 MiB
    pub image_concurrency: usize,       // RTDB_IMAGE_CONCURRENCY, default 4
    pub image_default_quality: u8,      // RTDB_IMAGE_DEFAULT_QUALITY, default 80
```

- [ ] **Step 3: Parse them in `from_env`** — copy the inline-match idiom. Bool default-on (mirror the login-CSRF block at `config.rs:183-186`):

```rust
        let image_transforms_enabled = match std::env::var("RTDB_IMAGE_TRANSFORMS_ENABLED") {
            Ok(v) => !matches!(v.trim().to_ascii_lowercase().as_str(), "false" | "0" | "no"),
            Err(_) => true,
        };
```

Numerics with defaults + clamp (mirror `ttl_batch` at `config.rs:234-240`, `.unwrap_or(default)` then clamp):

```rust
        let image_max_dim = match std::env::var("RTDB_IMAGE_MAX_DIM") {
            Ok(v) => v.trim().parse::<u32>().unwrap_or(2048).clamp(1, 8192),
            Err(_) => 2048,
        };
        let image_max_pixels = match std::env::var("RTDB_IMAGE_MAX_PIXELS") {
            Ok(v) => v.trim().parse::<u64>().unwrap_or(25_000_000).max(1_000_000),
            Err(_) => 25_000_000,
        };
        let image_cache_bytes = match std::env::var("RTDB_IMAGE_CACHE_BYTES") {
            Ok(v) => v.trim().parse::<u64>().unwrap_or(256 * 1024 * 1024),
            Err(_) => 256 * 1024 * 1024,
        };
        let image_concurrency = match std::env::var("RTDB_IMAGE_CONCURRENCY") {
            Ok(v) => v.trim().parse::<usize>().unwrap_or(4).max(1),
            Err(_) => 4,
        };
        let image_default_quality = match std::env::var("RTDB_IMAGE_DEFAULT_QUALITY") {
            Ok(v) => v.trim().parse::<u8>().unwrap_or(80).clamp(1, 100),
            Err(_) => 80,
        };
```

Add all six to the `Self { … }` literal at `config.rs:242-276` (`image_transforms_enabled, image_max_dim, …`).

- [ ] **Step 4: Add the six fields to `test_config()`** in `server/tests/common/mod.rs` (the literal ending at `:46` with `ttl_batch: 5000,`) — or every integration test fails to compile:

```rust
        image_transforms_enabled: true,
        image_max_dim: 2048,
        image_max_pixels: 25_000_000,
        image_cache_bytes: 256 * 1024 * 1024,
        image_concurrency: 4,
        image_default_quality: 80,
```

- [ ] **Step 5: env-drift — `.env.example`** (bare `NAME=default`, grouped near other `RTDB_` lines):

```
# On-the-fly image transforms on storage serve (ENH-014). All optional.
RTDB_IMAGE_TRANSFORMS_ENABLED=true
RTDB_IMAGE_MAX_DIM=2048
RTDB_IMAGE_MAX_PIXELS=25000000
RTDB_IMAGE_CACHE_BYTES=268435456
RTDB_IMAGE_CONCURRENCY=4
RTDB_IMAGE_DEFAULT_QUALITY=80
```

- [ ] **Step 6: env-drift — `docker-compose.yml`** `environment:` block (indent 6, `NAME: ${NAME:-default}` mirroring `RTDB_SUBS_VERIFY_SKIP_EVERY`):

```yaml
      RTDB_IMAGE_TRANSFORMS_ENABLED: ${RTDB_IMAGE_TRANSFORMS_ENABLED:-true}
      RTDB_IMAGE_MAX_DIM: ${RTDB_IMAGE_MAX_DIM:-2048}
      RTDB_IMAGE_MAX_PIXELS: ${RTDB_IMAGE_MAX_PIXELS:-25000000}
      RTDB_IMAGE_CACHE_BYTES: ${RTDB_IMAGE_CACHE_BYTES:-268435456}
      RTDB_IMAGE_CONCURRENCY: ${RTDB_IMAGE_CONCURRENCY:-4}
      RTDB_IMAGE_DEFAULT_QUALITY: ${RTDB_IMAGE_DEFAULT_QUALITY:-80}
```

- [ ] **Step 7: Verify + commit**

```bash
make -C /Users/probello/Repos/par-rt-db env-drift-check
cd /Users/probello/Repos/par-rt-db/server && cargo check
git add -A && git commit -m "feat(server): ENH-014 config + deps for image transforms"
```
Expected: env-drift-check passes; `cargo check` compiles (new deps fetch).

---

### Task 2: `image_transform.rs` pure transform logic (TDD; no axum, no cache yet)

**Files:**
- Create: `server/src/image_transform.rs`
- Modify: `server/src/lib.rs` module list (add `pub mod image_transform;` alongside other `pub mod` decls)

**Interfaces:**
- Consumes: `Config` fields from Task 1.
- Produces: `Fit`, `OutFormat`, `TransformParams` (+ `parse`), `TransformConfig` (+ `from_config`), `TransformError`, `apply()`.

- [ ] **Step 1: Write the failing unit tests** in `server/src/image_transform.rs` (`#[cfg(test)] mod tests`). Use the `image` crate to synthesize source images, then assert on decoded output dims:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use image::{ImageBuffer, Rgba};

    fn cfg() -> TransformConfig { TransformConfig { enabled: true, max_dim: 2048, max_pixels: 25_000_000, default_quality: 80 } }
    fn png_bytes(w: u32, h: u32) -> Vec<u8> {
        let img: ImageBuffer<Rgba<u8>, Vec<u8>> = ImageBuffer::new(w, h);
        let mut buf = Vec::new();
        image::DynamicImage::ImageRgba8(img).write_to(&mut std::io::Cursor::new(&mut buf), image::ImageFormat::Png).unwrap();
        buf
    }

    #[test]
    fn parse_none_when_no_params() {
        let mut q = std::collections::HashMap::new();
        assert!(TransformParams::parse(&q, &cfg()).unwrap().is_none());
    }
    #[test]
    fn parse_rejects_bad_values() {
        let c = cfg();
        let mk = |s: &str| { let mut q = std::collections::HashMap::new(); q.insert("w".into(), s.into()); q };
        assert!(TransformParams::parse(&mk("0"), &c).is_err());
        assert!(TransformParams::parse(&mk("99999"), &c).is_err());
        let mut q = std::collections::HashMap::new(); q.insert("fit".into(), "bogus".into());
        assert!(TransformParams::parse(&q, &c).is_err());
        let mut q = std::collections::HashMap::new(); q.insert("q".into(), "200".into());
        assert!(TransformParams::parse(&q, &c).is_err());
    }
    #[test]
    fn apply_contain_fits_within_box() {
        let c = cfg();
        let src = png_bytes(400, 200);
        let p = TransformParams { w: Some(100), h: Some(100), fit: Fit::Contain, q: None, format: OutFormat::Auto };
        let (out, ct) = apply(&src, Some("image/png"), &p, &c).unwrap();
        assert_eq!(ct, "image/png");
        let d = image::load_from_memory(&out).unwrap();
        assert_eq!(d.dimensions(), (100, 50)); // 2:1 aspect preserved, width-bound
    }
    #[test]
    fn apply_cover_crops_to_exact() {
        let c = cfg();
        let src = png_bytes(400, 200);
        let p = TransformParams { w: Some(100), h: Some(100), fit: Fit::Cover, q: None, format: OutFormat::Auto };
        let (out, _) = apply(&src, Some("image/png"), &p, &c).unwrap();
        assert_eq!(image::load_from_memory(&out).unwrap().dimensions(), (100, 100));
    }
    #[test]
    fn apply_scale_down_never_upscales() {
        let c = cfg();
        let src = png_bytes(50, 50);
        let p = TransformParams { w: Some(200), h: Some(200), fit: Fit::ScaleDown, q: None, format: OutFormat::Auto };
        let (out, _) = apply(&src, Some("image/png"), &p, &c).unwrap();
        assert_eq!(image::load_from_memory(&out).unwrap().dimensions(), (50, 50));
    }
    #[test]
    fn apply_format_jpeg_with_quality() {
        let c = cfg();
        let src = png_bytes(100, 100);
        let p = TransformParams { w: None, h: None, fit: Fit::Contain, q: Some(40), format: OutFormat::Jpeg };
        let (out, ct) = apply(&src, Some("image/png"), &p, &c).unwrap();
        assert_eq!(ct, "image/jpeg");
        assert_eq!(image::guess_format(&out).unwrap(), image::ImageFormat::Jpeg);
    }
    #[test]
    fn apply_non_image_returns_not_image() {
        let c = cfg();
        let p = TransformParams { w: Some(10), h: None, fit: Fit::Contain, q: None, format: OutFormat::Auto };
        assert!(matches!(apply(b"not an image", None, &p, &c), Err(TransformError::NotImage)));
    }
    #[test]
    fn apply_over_pixel_cap_returns_too_large() {
        let c = TransformConfig { enabled: true, max_dim: 2048, max_pixels: 10_000, default_quality: 80 };
        let src = png_bytes(200, 200); // 40k px > 10k cap
        let p = TransformParams { w: Some(50), h: None, fit: Fit::Contain, q: None, format: OutFormat::Auto };
        assert!(matches!(apply(&src, Some("image/png"), &p, &c), Err(TransformError::TooLarge)));
    }
}
```

- [ ] **Step 2: Run to verify failure**

```bash
cd /Users/probello/Repos/par-rt-db/server && cargo test image_transform
```
Expected: FAIL (module/types not defined).

- [ ] **Step 3: Implement `image_transform.rs`**

```rust
//! On-the-fly image transforms on storage serve (ENH-014). Pure-Rust decode →
//! resize → re-encode over the `image` crate, with a bounded-concurrency cache
//! (`TransformCache`, added later). HTTP-only; no committer/protocol involvement.

use std::io::Cursor;
use std::sync::Arc;

use image::{DynamicImage, GenericImageView, ImageFormat, ImageReader};

use crate::config::Config;
use crate::error::RtDbError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fit {
    Cover,
    Contain,
    ScaleDown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutFormat {
    Jpeg,
    Png,
    Auto,
}

#[derive(Debug, Clone)]
pub struct TransformParams {
    pub w: Option<u32>,
    pub h: Option<u32>,
    pub fit: Fit,
    pub q: Option<u8>,
    pub format: OutFormat,
}

#[derive(Debug, Clone)]
pub struct TransformConfig {
    pub enabled: bool,
    pub max_dim: u32,
    pub max_pixels: u64,
    pub default_quality: u8,
}

impl TransformConfig {
    pub fn from_config(c: &Config) -> Self {
        Self {
            enabled: c.image_transforms_enabled,
            max_dim: c.image_max_dim,
            max_pixels: c.image_max_pixels,
            default_quality: c.image_default_quality,
        }
    }
}

#[derive(Debug)]
pub enum TransformError {
    NotImage,
    TooLarge,
    Internal(String),
}

/// Parse transform params from a query map. `Ok(None)` ⇒ passthrough (no
/// transform requested). `Err(BadRequest)` ⇒ invalid value.
pub fn parse(
    q: &std::collections::HashMap<String, String>,
    cfg: &TransformConfig,
) -> Result<Option<TransformParams>, RtDbError> {
    let get = |k: &str| q.get(k).map(|s| s.trim()).filter(|s| !s.is_empty());
    let bad = |m: &str| RtDbError::bad_request(m);

    let w = match get("w") {
        Some(v) => Some(v.parse::<u32>().map_err(|_| bad("w must be a positive integer"))?),
        None => None,
    };
    let h = match get("h") {
        Some(v) => Some(v.parse::<u32>().map_err(|_| bad("h must be a positive integer"))?),
        None => None,
    };
    let q = match get("q") {
        Some(v) => Some(v.parse::<u8>().map_err(|_| bad("q must be 1..=100"))?),
        None => None,
    };
    if let Some(v) = q {
        if !(1..=100).contains(&v) {
            return Err(bad("q must be between 1 and 100"));
        }
    }
    for v in [w, h].into_iter().flatten() {
        if v < 1 || v > cfg.max_dim {
            return Err(bad("w/h must be between 1 and max dim"));
        }
    }
    let fit = match get("fit") {
        None => Fit::Contain, // refined below when both w/h present
        Some("cover") => Fit::Cover,
        Some("contain") => Fit::Contain,
        Some("scale-down") => Fit::ScaleDown,
        Some(_) => return Err(bad("fit must be cover|contain|scale-down")),
    };
    let format = match get("format") {
        None => OutFormat::Auto,
        Some("jpeg") => OutFormat::Jpeg,
        Some("png") => OutFormat::Png,
        Some("auto") => OutFormat::Auto,
        Some(_) => return Err(bad("format must be jpeg|png|auto")),
    };

    // Passthrough iff no resize, no quality, and format is auto/absent.
    let resizing = w.is_some() || h.is_some();
    if !resizing && q.is_none() && format == OutFormat::Auto {
        return Ok(None);
    }
    // Default fit when omitted: cover if both dims given, else contain.
    let fit = if get("fit").is_none() && w.is_some() && h.is_some() {
        Fit::Cover
    } else {
        fit
    };
    Ok(Some(TransformParams { w, h, fit, q, format }))
}

fn pick_format(params: &TransformParams, src: ImageFormat) -> ImageFormat {
    match params.format {
        OutFormat::Jpeg => ImageFormat::Jpeg,
        OutFormat::Png => ImageFormat::Png,
        OutFormat::Auto => match src {
            ImageFormat::Jpeg => ImageFormat::Jpeg,
            ImageFormat::Png => ImageFormat::Png,
            _ => ImageFormat::Jpeg, // gif/bmp/webp → jpeg
        },
    }
}

/// Decode → resize → re-encode. Pure + sync (run under `spawn_blocking`).
pub fn apply(
    bytes: &[u8],
    content_type: Option<&str>,
    params: &TransformParams,
    cfg: &TransformConfig,
) -> Result<(Vec<u8>, &'static str), TransformError> {
    let src_format = image::guess_format(bytes).ok().or_else(|| {
        content_type.and_then(|ct| match ct.split(';').next().unwrap_or("").trim() {
            "image/png" => Some(ImageFormat::Png),
            "image/jpeg" => Some(ImageFormat::Jpeg),
            "image/gif" => Some(ImageFormat::Gif),
            "image/bmp" => Some(ImageFormat::Bmp),
            "image/webp" => Some(ImageFormat::WebP),
            _ => None,
        })
    });
    let src_format = match src_format {
        Some(f) if matches!(f, ImageFormat::Png | ImageFormat::Jpeg | ImageFormat::Gif | ImageFormat::Bmp | ImageFormat::WebP) => f,
        _ => return Err(TransformError::NotImage),
    };

    let mut reader = ImageReader::new(Cursor::new(bytes));
    reader.set_format(src_format);
    // Cap decode cost: refuse sources over the pixel budget. The exact `image`
    // 0.25 Limits setters must match the installed crate; the values are
    // max_image_width/height (absolute guard) and max_alloc ≈ 4 bytes/px.
    let mut limits = image::Limits::default();
    limits.max_image_width = Some(16_384);
    limits.max_image_height = Some(16_384);
    limits.max_alloc = Some(cfg.max_pixels.saturating_mul(4));
    reader.set_limits(limits).map_err(|_| TransformError::Internal("limits unsupported".into()))?;
    let img = match reader.decode() {
        Ok(img) => img,
        Err(image::ImageError::Limits(_)) => return Err(TransformError::TooLarge),
        Err(image::ImageError::Decoding(_)) | Err(image::ImageError::Format(_)) => return Err(TransformError::NotImage),
        Err(e) => return Err(TransformError::Internal(e.to_string())),
    };

    let img = resize(&img, params);

    let out_format = pick_format(params, src_format);
    let mut out = Vec::with_capacity(bytes.len() / 2);
    match out_format {
        ImageFormat::Jpeg => {
            let quality = params.q.unwrap_or(cfg.default_quality);
            let rgb = DynamicImage::ImageRgb8(img.to_rgb8());
            let encoder = image::codecs::jpeg::JpegEncoder::new_with_quality(&mut out, quality);
            let (w, h) = rgb.dimensions();
            encoder
                .write_image(rgb.as_bytes(), w, h, image::ExtendedColorType::Rgb8)
                .map_err(|e| TransformError::Internal(e.to_string()))?;
            Ok((out, "image/jpeg"))
        }
        ImageFormat::Png => {
            img.write_to(&mut Cursor::new(&mut out), ImageFormat::Png)
                .map_err(|e| TransformError::Internal(e.to_string()))?;
            Ok((out, "image/png"))
        }
        _ => Err(TransformError::Internal("unsupported output format".into())),
    }
}

/// Resize per `fit`. Only-`w` scales to width; only-`h` to height.
fn resize(img: &DynamicImage, params: &TransformParams) -> DynamicImage {
    use image::imageops::FilterType;
    let filter = FilterType::Triangle;
    let (ow, oh) = img.dimensions();
    let want_w = params.w.unwrap_or(u32::MAX);
    let want_h = params.h.unwrap_or(u32::MAX);
    if params.w.is_none() && params.h.is_none() {
        return img.clone();
    }
    match params.fit {
        Fit::Contain => {
            let (tw, th) = contain_target(ow, oh, want_w, want_h);
            image::imageops::resize(img, tw, th, filter)
        }
        Fit::ScaleDown => {
            let (tw, th) = contain_target(ow, oh, want_w, want_h);
            if tw >= ow && th >= oh {
                img.clone() // smaller than target → keep
            } else {
                image::imageops::resize(img, tw, th, filter)
            }
        }
        Fit::Cover => {
            // Only crop when both dims given; else behave like contain.
            if params.w.is_some() && params.h.is_some() {
                let scale = ((want_w as f64) / (ow as f64)).max((want_h as f64) / (oh as f64));
                let sw = ((ow as f64) * scale).round() as u32;
                let sh = ((oh as f64) * scale).round() as u32;
                let scaled = image::imageops::resize(img, sw, sh, filter);
                let (cw, ch) = (want_w.min(sw), want_h.min(sh));
                let x = (sw - cw) / 2;
                let y = (sh - ch) / 2;
                image::imageops::crop_imm(&scaled, x, y, cw, ch).to_image().into()
            } else {
                let (tw, th) = contain_target(ow, oh, want_w, want_h);
                image::imageops::resize(img, tw, th, filter)
            }
        }
    }
}

/// Fit within the box preserving aspect (the smaller scale wins).
fn contain_target(ow: u32, oh: u32, want_w: u32, want_h: u32) -> (u32, u32) {
    if ow == 0 || oh == 0 {
        return (ow, oh);
    }
    let sw = if want_w != u32::MAX { (want_w as f64) / (ow as f64) } else { f64::MAX };
    let sh = if want_h != u32::MAX { (want_h as f64) / (oh as f64) } else { f64::MAX };
    let s = sw.min(sh).min(1.0); // contain never upscales
    if s == f64::MAX {
        return (ow, oh);
    }
    let tw = ((ow as f64) * s).round().max(1.0) as u32;
    let th = ((oh as f64) * s).round().max(1.0) as u32;
    (tw.min(want_w).max(1), th.min(want_h).max(1))
}

// `TransformCache`, `CachedImage`, `Resolved` are added in Task 4.
#[allow(dead_code)]
fn _placeholder_link(_pool: &sqlx::PgPool) {}
```

Add `pub mod image_transform;` to `server/src/lib.rs` (alphabetized with the other `pub mod` declarations).

- [ ] **Step 4: Run tests to verify pass**

```bash
cd /Users/probello/Repos/par-rt-db/server && cargo test image_transform
```
Expected: PASS. If `image::Limits` setters differ in the installed 0.25.x (e.g. `set_max_width` builder vs field assignment), adjust to the crate's actual API — the values (16384 / `max_pixels*4`) and the `Err(Limits)` ⇒ `TooLarge` mapping are the contract. If `JpegEncoder::new_with_quality`/`write_image` signature differs, adapt; the contract is "encode RGB8 at `quality`".

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(server): ENH-014 image transform decode/resize/encode logic"
```

---

### Task 3: Metrics counters (independent of Task 2)

**Files:** Modify `server/src/metrics.rs` (struct :117-154, record fns ~:167, snapshot :331, load ~:306, render ~:383-405, test literals :482 & :520).

**Interfaces:** Produces `Metrics::record_image_transform_hit/miss/error(bytes)` consumed by Task 4.

- [ ] **Step 1: Add four counter fields** to `Metrics` next to `uploads_total` (~:121):

```rust
    pub image_transforms_hit_total: AtomicU64,
    pub image_transforms_miss_total: AtomicU64,
    pub image_transforms_error_total: AtomicU64,
    pub image_transform_bytes_total: AtomicU64,
```

- [ ] **Step 2: Add record methods** next to `record_upload` (~:167):

```rust
    pub fn record_image_transform_hit(&self) {
        self.image_transforms_hit_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_image_transform_miss(&self, out_bytes: u64) {
        self.image_transforms_miss_total.fetch_add(1, Ordering::Relaxed);
        self.image_transform_bytes_total.fetch_add(out_bytes, Ordering::Relaxed);
    }
    pub fn record_image_transform_error(&self) {
        self.image_transforms_error_total.fetch_add(1, Ordering::Relaxed);
    }
```

(`Metrics::new`/`Default` derive initializes `AtomicU64::default()` = 0 — no change needed if `#[derive(Default)]`.)

- [ ] **Step 3: Add snapshot fields** to `MetricsSnapshot` (~:331) + load in `snapshot()` (~:306):

```rust
    pub image_transforms_hit_total: u64,
    pub image_transforms_miss_total: u64,
    pub image_transforms_error_total: u64,
    pub image_transform_bytes_total: u64,
```
In `snapshot()`: `image_transforms_hit_total: self.image_transforms_hit_total.load(Ordering::Relaxed),` (×4).

- [ ] **Step 4: Render in `render_prometheus`** (~:385), mirroring the labeled `subs_skips_total{class=…}` block (~:390-405):

```rust
    s.push_str("# HELP rtdb_image_transforms_total Image transforms served, by result.\n");
    s.push_str("# TYPE rtdb_image_transforms_total counter\n");
    s.push_str(&format!("rtdb_image_transforms_total{{result=\"hit\"}} {}\n", snap.image_transforms_hit_total));
    s.push_str(&format!("rtdb_image_transforms_total{{result=\"miss\"}} {}\n", snap.image_transforms_miss_total));
    s.push_str(&format!("rtdb_image_transforms_total{{result=\"error\"}} {}\n", snap.image_transforms_error_total));
    s.push_str("# HELP rtdb_image_transform_bytes_total Total bytes emitted by image transforms.\n");
    s.push_str("# TYPE rtdb_image_transform_bytes_total counter\n");
    s.push_str(&format!("rtdb_image_transform_bytes_total {}\n", snap.image_transform_bytes_total));
```

- [ ] **Step 5: Update the two `MetricsSnapshot { … }` test literals** at `metrics.rs:482` and `:520` (add the four fields = `0`) or compilation breaks.

- [ ] **Step 6: Verify + commit**

```bash
cd /Users/probello/Repos/par-rt-db/server && cargo test metrics
git add -A && git commit -m "feat(server): ENH-014 image transform metrics"
```

---

### Task 4: `TransformCache` + AppState wiring

**Files:**
- Modify: `server/src/image_transform.rs` (append `CachedImage`, `Resolved`, `TransformCache`).
- Modify: `server/src/lib.rs:80-97` (add `image` field) + `:100-145` (construct in `AppState::new`).

**Interfaces:**
- Consumes: `apply()` + `TransformConfig` (Task 2), `Metrics` recorders (Task 3), `Config.image_cache_bytes`/`image_concurrency` (Task 1).
- Produces: `AppState.image: Arc<TransformCache>` consumed by Task 5.

- [ ] **Step 1: Append to `image_transform.rs`**

```rust
use tokio::sync::Semaphore;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct CachedImage {
    pub bytes: Arc<[u8]>,
    pub content_type: &'static str,
}

/// Outcome of a transform-or-passthrough request.
pub enum Resolved {
    Transformed(CachedImage),
    Raw { bytes: Arc<[u8]>, content_type: String },
}

pub struct TransformCache {
    cache: moka::future::Cache<String, CachedImage>,
    sem: Semaphore,
    cfg: TransformConfig,
    metrics: Arc<crate::metrics::Metrics>,
}

impl TransformCache {
    pub fn new(cfg: TransformConfig, cache_bytes: u64, metrics: Arc<crate::metrics::Metrics>) -> Self {
        let cache = moka::future::Cache::builder()
            .max_capacity(cache_bytes) // weigher ⇒ weight = bytes
            .weigher(|_, v: &CachedImage| -> u32 { v.bytes.len().min(u32::MAX as usize) as u32 })
            .build();
        Self { cache, sem: Semaphore::new(cfg.enabled.then(|| 4).unwrap_or(1).max(1)), cfg, metrics }
    }
    pub fn cfg(&self) -> &TransformConfig { &self.cfg }

    /// `Ok(Transformed)` from cache or freshly computed; `Ok(Raw)` when the
    /// source is not a decodable image (serve the original bytes); `Err` for
    /// over-cap (`BadRequest`), not-found, or internal failure.
    pub async fn get_or_transform(
        &self,
        pool: &sqlx::PgPool,
        db: &str,
        id: &str,
        params: TransformParams,
    ) -> Result<Resolved, RtDbError> {
        let key = format!("{id}|{}|{}|{:?}|{:?}|{:?}", params.w, params.h, params.fit, params.q, params.format);
        if let Some(hit) = self.cache.get(&key).await {
            self.metrics.record_image_transform_hit();
            return Ok(Resolved::Transformed(hit));
        }
        // Bound concurrent decodes; 429 (Retry-After) if the queue stalls.
        let permit = match tokio::time::timeout(Duration::from_secs(5), self.sem.acquire()).await {
            Ok(Ok(p)) => p,
            _ => return Err(RtDbError::rate_limited(2)),
        };
        // Double-checked: a concurrent request may have populated the cache.
        if let Some(hit) = self.cache.get(&key).await {
            drop(permit);
            self.metrics.record_image_transform_hit();
            return Ok(Resolved::Transformed(hit));
        }
        let (bytes, ct) = crate::storage::get(pool, db, id)
            .await?
            .ok_or_else(|| RtDbError::not_found("unknown file"))?;
        let cfg = self.cfg.clone();
        let result = tokio::task::spawn_blocking(move || apply(&bytes, ct.as_deref(), &params, &cfg))
            .await
            .map_err(|e| RtDbError::internal(format!("transform join: {e}")))?;
        drop(permit);
        match result {
            Ok((tbytes, tct)) => {
                let n = tbytes.len() as u64;
                let cached = CachedImage { bytes: Arc::from(tbytes), content_type: tct };
                self.cache.insert(key, cached.clone()).await;
                self.metrics.record_image_transform_miss(n);
                Ok(Resolved::Transformed(cached))
            }
            Err(TransformError::NotImage) => Ok(Resolved::Raw {
                bytes: Arc::from(bytes),
                content_type: ct.unwrap_or_else(|| "application/octet-stream".to_string()),
            }),
            Err(TransformError::TooLarge) => {
                self.metrics.record_image_transform_error();
                Err(RtDbError::bad_request("image exceeds max pixels for transform"))
            }
            Err(TransformError::Internal(m)) => {
                self.metrics.record_image_transform_error();
                Err(RtDbError::internal(m))
            }
        }
    }
}
```

Remove the `_placeholder_link` stub from Task 2 (it seeded the `sqlx` import; now real).

- [ ] **Step 2: Add `image` to `AppState`** (`lib.rs:80-97`, next to `rate_limiter`):

```rust
    pub image: Arc<image_transform::TransformCache>,
```

- [ ] **Step 3: Construct it in `AppState::new`** (`lib.rs:100-145`). After `metrics` is built (~:102) add:

```rust
        let image = Arc::new(image_transform::TransformCache::new(
            image_transform::TransformConfig::from_config(&config),
            config.image_cache_bytes,
            Arc::clone(&metrics),
        ));
```
Then add `image,` to the `Arc::new(Self { … })` literal (`lib.rs:124-143`). **Confirm `metrics` is wrapped in `Arc<Metrics>` at this point** — the agent reported `Metrics::new` at :102 and `Arc`-wrapped elsewhere; if `metrics` is not yet an `Arc` here, wrap it (`Arc::new(Metrics::new())`) and thread that same `Arc` into both `Runtime` and `TransformCache` (adjust the `Runtime` construction to take the clone). The `AppState::new` signature `(pool, config, hot)` stays **unchanged**, so the 8 `AppState::new` call sites in `server/tests/common/mod.rs` and `main.rs` compile without edits.

- [ ] **Step 4: Verify + commit**

```bash
cd /Users/probello/Repos/par-rt-db/server && cargo test
git add -A && git commit -m "feat(server): ENH-014 TransformCache + AppState wiring"
```
Expected: full server test suite green (no behavior change yet — handlers not wired).

---

### Task 5: Wire serve handlers + integration tests

**Files:**
- Modify: `server/src/http_api.rs` (`serve_public_handler` :441, `serve_authed_handler` :453, `serve_bytes` :465; add `axum::extract::Query` import).
- Create: `server/tests/image_transform_test.rs`.

**Interfaces:** Consumes `AppState.image`, `TransformParams::parse`, `Resolved`.

- [ ] **Step 1: Write the failing integration test** `server/tests/image_transform_test.rs`. Copy the `storage_test.rs` scaffolding (`spawn_app`/`test_state`/`wrap_test_db`/`mint_token`/`upload`). Build a real PNG via the `image` crate, upload, then GET transforms.

```rust
mod common;
use common::{mint_token, spawn_app, test_state, wrap_test_db};
use axum::http::StatusCode;
use image::{ImageBuffer, Rgba};

async fn upload_png(addr: &str, db: &str, token: &str, w: u32, h: u32) -> String {
    let img: ImageBuffer<Rgba<u8>, Vec<u8>> = ImageBuffer::new(w, h);
    let mut body = Vec::new();
    image::DynamicImage::ImageRgba8(img)
        .write_to(&mut std::io::Cursor::new(&mut body), image::ImageFormat::Png).unwrap();
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/storage/{db}"))
        .bearer_auth(token).header("content-type", "image/png").body(body)
        .send().await.unwrap();
    resp.json::<serde_json::Value>().await.unwrap()["id"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn serve_transform_resizes_png() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = wrap_test_db(&state).await;
    let token = mint_token(addr, &db).await;
    let id = upload_png(&addr.to_string(), &db, &token, 400, 200).await;

    let r = reqwest::get(format!("http://{addr}/storage/{id}?w=100&h=100&fit=cover")).await?;
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(r.headers().get("content-type").unwrap(), "image/png");
    assert_eq!(r.headers().get("cache-control").unwrap(), "public, max-age=31536000, immutable");
    let out = r.bytes().await?;
    assert_eq!(image::load_from_memory(&out)?.dimensions(), (100, 100));
    Ok(())
}

#[tokio::test]
async fn serve_passthrough_when_no_params() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = wrap_test_db(&state).await;
    let token = mint_token(addr, &db).await;
    let id = upload_png(&addr.to_string(), &db, &token, 40, 40).await;
    let r = reqwest::get(format!("http://{addr}/storage/{id}")).await?;
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(r.headers().get("content-type").unwrap(), "image/png");
    Ok(())
}

#[tokio::test]
async fn serve_bad_params_400() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = wrap_test_db(&state).await;
    let token = mint_token(addr, &db).await;
    let id = upload_png(&addr.to_string(), &db, &token, 40, 40).await;
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
    let db = wrap_test_db(&state).await;
    let token = mint_token(addr, &db).await;
    let payload = b"definitely not an image";
    let up = reqwest::Client::new()
        .post(format!("http://{addr}/api/storage/{db}"))
        .bearer_auth(&token).header("content-type", "application/pdf").body(payload.to_vec())
        .send().await?;
    let id = up.json::<serde_json::Value>().await?["id"].as_str().unwrap().to_string();
    let r = reqwest::get(format!("http://{addr}/storage/{id}?w=50")).await?;
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(r.bytes().await?, &payload[..]);
    Ok(())
}

#[tokio::test]
async fn authed_serve_also_transforms() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = wrap_test_db(&state).await;
    let token = mint_token(addr, &db).await;
    let id = upload_png(&addr.to_string(), &db, &token, 200, 100).await;
    let r = reqwest::Client::new()
        .get(format!("http://{addr}/api/storage/{db}/{id}?w=50&format=jpeg"))
        .bearer_auth(&token).send().await?;
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(r.headers().get("content-type").unwrap(), "image/jpeg");
    Ok(())
}
```

(Confirm `wrap_test_db`, `mint_token`, and `upload`/`spawn_app` signatures in `server/tests/common/mod.rs` + `storage_test.rs` and adapt imports exactly.)

- [ ] **Step 2: Run to verify failure** — `make dev-db-up` then:

```bash
cd /Users/probello/Repos/par-rt-db/server && cargo test --test image_transform_test
```
Expected: FAIL (handlers ignore params).

- [ ] **Step 3: Wire the handlers in `http_api.rs`.** Add `use axum::extract::Query;` and `use std::collections::HashMap;` (if not present). Change the two serve handlers + `serve_bytes`:

```rust
async fn serve_public_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Query(q): Query<HashMap<String, String>>,
) -> Result<Response, RtDbError> {
    let db = storage::resolve_db(&state.pool, &id)
        .await?
        .ok_or_else(|| RtDbError::not_found("unknown file"))?;
    serve_bytes(&state, &db, &id, &q).await
}

async fn serve_authed_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((db, id)): Path<(String, String)>,
    Query(q): Query<HashMap<String, String>>,
) -> Result<Response, RtDbError> {
    let token = bearer_token(&headers)?;
    let principal = resolve_bearer(&state.pool, token).await?;
    authorize(&state.pool, &principal, &db).await?;
    check_http_rate_limits(&state, &principal, &db).await?;
    serve_bytes(&state, &db, &id, &q).await
}

async fn serve_bytes(
    state: &Arc<AppState>,
    db: &str,
    id: &str,
    q: &HashMap<String, String>,
) -> Result<Response, RtDbError> {
    const IMMUTABLE: &str = "public, max-age=31536000, immutable";
    let params = image_transform::parse(q, state.image.cfg())?;
    let resolved = match params {
        None => None,
        Some(p) if !state.image.cfg().enabled => None,
        Some(p) => Some(state.image.get_or_transform(&state.pool, db, id, p).await?),
    };
    let (bytes, content_type) = match resolved {
        Some(image_transform::Resolved::Transformed(c)) => (c.bytes, c.content_type.to_string()),
        Some(image_transform::Resolved::Raw { bytes, content_type }) => (bytes, content_type),
        None => {
            let (bytes, ct) = storage::get(&state.pool, db, id)
                .await?
                .ok_or_else(|| RtDbError::not_found("unknown file"))?;
            (Arc::from(bytes), ct.unwrap_or_else(|| "application/octet-stream".to_string()))
        }
    };
    Response::builder()
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CACHE_CONTROL, IMMUTABLE)
        .body(Body::from(bytes))
        .map_err(|err| RtDbError::internal(format!("failed to build serve response: {err}")))
}
```

- [ ] **Step 4: Run tests to verify pass**

```bash
make -C /Users/probello/Repos/par-rt-db dev-db-up
cd /Users/probello/Repos/par-rt-db/server && cargo test --test image_transform_test && cargo test --test storage_test
```
Expected: PASS (transforms + existing storage tests still green).

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(server): ENH-014 wire image transforms into storage serve"
```

---

### Task 6: ts-client `transformUrl` + `appendImageParams`

**Files:**
- Modify: `ts-client/src/http.ts` (add `TransformOpts` type ~:28, `appendImageParams` fn, `transformUrl` method next to `getUrl` :173-176).
- Modify: `ts-client/src/client.ts:385-387` (delegate `transformUrl`).
- Modify: `ts-client/src/index.ts:58` (export `TransformOpts`).
- Modify: `ts-client/tests/storage.test.ts` (unit tests).

**Interfaces:** Produces `appendImageParams(url, opts)` + `client.transformUrl(id, opts)` consumed by Task 9 (dashboard).

- [ ] **Step 1: Write the failing tests** in `ts-client/tests/storage.test.ts` (vitest, dynamic import, no server):

```ts
it("appendImageParams builds the canonical query string", async () => {
  const { appendImageParams } = await import("../src/http.js");
  const url = appendImageParams("https://rtdb.example/storage/abc", {
    w: 100, h: 50, fit: "cover", q: 80, format: "jpeg",
  });
  expect(url).toBe("https://rtdb.example/storage/abc?w=100&h=50&fit=cover&q=80&format=jpeg");
});

it("appendImageParams omits unset opts", async () => {
  const { appendImageParams } = await import("../src/http.js");
  expect(appendImageParams("https://rtdb.example/storage/abc", { w: 64 }))
    .toBe("https://rtdb.example/storage/abc?w=64");
});

it("transformUrl against the http client builds the URL with params", async () => {
  const { RtDbHttpClient } = await import("../src/http.js");
  const http = new RtDbHttpClient({ url: "https://rtdb.example.com/", db: "kanban", token: "t" });
  expect(http.transformUrl("abc", { w: 100, fit: "contain" }))
    .toBe("https://rtdb.example.com/storage/abc?w=100&fit=contain");
});
```

- [ ] **Step 2: Run to verify failure** — `cd ts-client && bunx vitest run tests/storage.test.ts`.

- [ ] **Step 3: Implement.** In `http.ts`, add the type next to `FileMetadata`:

```ts
/** Image-transform query params appended to a storage serve URL (ENH-014). */
export interface TransformOpts {
  w?: number;
  h?: number;
  fit?: "cover" | "contain" | "scale-down";
  q?: number;
  format?: "jpeg" | "png" | "auto";
}
```

Add the pure helper (deterministic key order `w, h, fit, q, format`):

```ts
/** Append image-transform query params to a storage URL. Omits unset opts. */
export function appendImageParams(url: string, opts: TransformOpts): string {
  const parts: string[] = [];
  const push = (k: string, v: string | undefined) => { if (v !== undefined) parts.push(`${k}=${encodeURIComponent(v)}`); };
  push("w", opts.w?.toString());
  push("h", opts.h?.toString());
  push("fit", opts.fit);
  push("q", opts.q?.toString());
  push("format", opts.format);
  return parts.length ? `${url}?${parts.join("&")}` : url;
}
```

Add the method next to `getUrl` (uses the already-normalized `this.url`):

```ts
  /** The public serve URL for `id` with image-transform params applied. */
  transformUrl(id: string, opts: TransformOpts): string {
    return appendImageParams(this.getUrl(id), opts);
  }
```

In `client.ts` (next to the `getUrl` delegate ~:385): `transformUrl(id: string, opts: TransformOpts) { return this.httpForStorage().transformUrl(id, opts); }` (import `TransformOpts` type). In `index.ts:58` add `TransformOpts` to the type-export list.

- [ ] **Step 4: Run to verify pass** — `cd ts-client && bun run test && bun run typecheck && bun run lint`.

- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat(ts-client): ENH-014 transformUrl helper"`.

---

### Task 7: rust-client `transform_url`

**Files:**
- Modify: `rust-client/src/http.rs` (add `TransformOpts`/`Fit`/`OutFormat` next to `UploadResult` :52; `transform_url` method next to `get_url` :463-465; inline test in `mod tests`).
- Modify: `rust-client/src/lib.rs:71-72` (re-export `Fit, OutFormat, TransformOpts`).

- [ ] **Step 1: Failing test** in `http.rs` `#[cfg(test)] mod tests` (sync `#[test]`, no MockServer — mirrors the `get_url` assertion at :1972-1995):

```rust
#[test]
fn transform_url_appends_query_params() {
    let client = RtDbHttpClient::new("https://rtdb.example", "db", "tok");
    let url = client.transform_url(
        "f1",
        &TransformOpts { w: Some(100), h: Some(50), fit: Some(Fit::Cover),
                         q: Some(80), format: Some(OutFormat::Auto) },
    );
    assert_eq!(url, "https://rtdb.example/storage/f1?w=100&h=50&fit=cover&q=80");
}
```

- [ ] **Step 2: Run to fail** — `cd rust-client && cargo test --all-features transform_url`.

- [ ] **Step 3: Implement.** Types next to `UploadResult` (enum style mirrors `DistanceMetric` at `schema.rs:150-157`):

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Fit {
    #[default]
    Contain,
    Cover,
    ScaleDown,
}
impl Fit {
    fn as_str(self) -> &'static str {
        match self { Fit::Cover => "cover", Fit::Contain => "contain", Fit::ScaleDown => "scale-down" }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OutFormat {
    #[default]
    Auto,
    Jpeg,
    Png,
}
#[derive(Debug, Clone, Default)]
pub struct TransformOpts {
    pub w: Option<u32>,
    pub h: Option<u32>,
    pub fit: Option<Fit>,
    pub q: Option<u8>,
    pub format: Option<OutFormat>,
}
```

Method next to `get_url` (hand-built query, fixed order, `Some` only — no new dep):

```rust
    /// The public serve URL for `id` with image-transform params (ENH-014).
    pub fn transform_url(&self, id: &str, opts: &TransformOpts) -> String {
        let base = format!("{}/storage/{id}", self.url);
        let mut parts: Vec<String> = Vec::new();
        if let Some(w) = opts.w { parts.push(format!("w={w}")); }
        if let Some(h) = opts.h { parts.push(format!("h={h}")); }
        if let Some(fit) = opts.fit { parts.push(format!("fit={}", fit.as_str())); }
        if let Some(q) = opts.q { parts.push(format!("q={q}")); }
        if let Some(f) = opts.format {
            parts.push(format!("format={}", match f { OutFormat::Auto => "auto", OutFormat::Jpeg => "jpeg", OutFormat::Png => "png" }));
        }
        if parts.is_empty() { base } else { format!("{base}?{}", parts.join("&")) }
    }
```

In `lib.rs:71-72`: `pub use http::{Fit, OutFormat, RtDbHttpClient, TransformOpts};`.

- [ ] **Step 4: Verify** — `cd rust-client && cargo test --all-features && cargo clippy --all-targets --all-features -- -D warnings && cargo fmt --all -- --check`.

- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat(rust-client): ENH-014 transform_url helper"`.

---

### Task 8: python-client `transform_url`

**Files:**
- Modify: `python-client/src/par_rt_db/http_client.py` (add `transform_url` after `get_url` :635), `aio_http_client.py` (:266), `in_memory.py` (:1523).
- Modify: `python-client/tests/test_http_client.py` + `test_aio_http_client.py` (unit tests).

- [ ] **Step 1: Failing tests** in `test_http_client.py` (pure, no request — mirror `test_get_url…` :426-429):

```python
from urllib.parse import parse_qs, urlparse

def test_transform_url_emits_params_in_order() -> None:
    client = _client(lambda r: httpx.Response(500))
    url = client.transform_url("f1", w=100, h=50, fit="cover", q=80, format="jpeg")
    assert url == "https://rtdb.example/storage/f1?w=100&h=50&fit=cover&q=80&format=jpeg"

def test_transform_url_omits_unset_opts() -> None:
    client = _client(lambda r: httpx.Response(500))
    qs = parse_qs(urlparse(client.transform_url("f1", w=64)).query)
    assert list(qs) == ["w"]
```

- [ ] **Step 2: Run to fail** — `cd python-client && uv run pytest -q tests/test_http_client.py -k transform_url`.

- [ ] **Step 3: Implement** (sync `http_client.py`, after `get_url`; `fit` is a `Literal`):

```python
from typing import Literal

def transform_url(
    self,
    id: str,
    *,
    w: int | None = None,
    h: int | None = None,
    fit: Literal["cover", "contain", "scale-down"] | None = None,
    q: int | None = None,
    format: Literal["jpeg", "png", "auto"] | None = None,
) -> str:
    """The public serve URL for ``id`` with image-transform params (ENH-014). No request is made."""
    parts: list[str] = []
    if w is not None:
        parts.append(f"w={w}")
    if h is not None:
        parts.append(f"h={h}")
    if fit is not None:
        parts.append(f"fit={fit}")
    if q is not None:
        parts.append(f"q={q}")
    if format is not None:
        parts.append(f"format={format}")
    base = f"{self._base}/storage/{id}"
    return f"{base}?{'&'.join(parts)}" if parts else base
```

Mirror verbatim in `aio_http_client.py`. For `in_memory.py` (synthetic `memory://{id}`), append the same query string:

```python
    def transform_url(self, id: str, **opts) -> str:  # same signature
        ...
        base = f"memory://{id}"
        return f"{base}?{'&'.join(parts)}" if parts else base
```

Add the matching async test in `test_aio_http_client.py`.

- [ ] **Step 4: Verify** — `cd python-client && uv run ruff format . && uv run ruff check . && uv run pyright && uv run pytest -q`.

- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat(python-client): ENH-014 transform_url helper"`.

---

### Task 9: Dashboard storage-browser size control

**Files:**
- Modify: `dashboard/src/pages/StoragePage.tsx` (size dropdown + transformed copy; import `appendImageParams` from `@par-rt-db/client`).
- Modify: `dashboard/src/pages/StoragePage.module.css` (reuse `--inset`/`--rule-strong`/`--mono` tokens).
- Modify: `dashboard/src/pages/StoragePage.test.tsx` (mock `@par-rt-db/client`'s `appendImageParams`).

**Depends on:** Task 6 (dashboard `bun run build` runs `build:sdk` → builds ts-client `dist`).

- [ ] **Step 1: Failing test** — extend the existing `StoragePage.test.tsx` mock. Add `vi.mock("@par-rt-db/client", () => ({ appendImageParams: (u: string) => u + "?w=100" }))` (or assert the dropdown renders + the copy handler calls `appendImageParams`). Assert a size `<select>` renders and selecting it + copy produces a URL containing `?w=`.

- [ ] **Step 2: Run to fail** — `cd dashboard && bun run test`.

- [ ] **Step 3: Implement.** In `StoragePage.tsx`: add a small per-image-size selector state (e.g. `const [size, setSize] = useState<"orig" | "lg" | "md" | "sm">("orig")`) and change `copyPublicUrl` to apply a transform when a size is chosen. Map sizes to opts: `md → {w:512, fit:"contain"}`, `sm → {w:128, fit:"cover"}`, `lg → {w:1024, fit:"contain"}`, `orig → none`. Import and use `appendImageParams`:

```tsx
import { appendImageParams } from "@par-rt-db/client";

async function copyPublicUrl(file: FileMeta) {
  const base = `${window.location.origin}/storage/${file.id}`;
  const opts = sizeOpts(size); // {w,fit} or null
  const url = opts ? appendImageParams(base, opts) : base;
  await navigator.clipboard.writeText(url);
  setCopiedId(file.id);
  setTimeout(() => setCopiedId(null), 1500);
}
```

Add a `<select className={s.select}>` (options: original / large 1024 / medium 512 / small 128) beside the copy `Button` in the row-actions area (~:163 `s.rowActions`), styled with the existing `--inset`/`--rule-strong`/`--mono` tokens (copy the `.select` rule already in `StoragePage.module.css:36-60`).

- [ ] **Step 4: Verify** — `cd dashboard && bun run typecheck && bun run test && bun run build` (build runs `build:sdk` first). Also `cd ts-client && bun run build` if `dist` is stale.

- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat(dashboard): ENH-014 storage browser image size control"`.

---

### Task 10: Docs + parity

**Files:** `FEATURE_MATRIX.md` (row #16), `CLAUDE.md`, `server/src/storage.rs` (module doc), `ENHANCEMENTS.md` (ENH-014 checkbox), vault note.

- [ ] **Step 1: FEATURE_MATRIX.md row #16** — append to the File storage cell: "On-the-fly image transforms (`?w=&h=&fit=cover|contain|scale-down&q=&format=jpeg|png|auto`) on both serve routes, server-side via the pure-Rust `image` crate with an in-memory `moka` cache + bounded decode concurrency (ENH-014). Mirrored: ts-client `transformUrl`/`appendImageParams`, rust-client `transform_url`, python-client `transform_url`, dashboard storage-browser size control."

- [ ] **Step 2: CLAUDE.md** — extend the **File storage** invariant bullet to note transforms are read-time-only on the serve routes (no committer/protocol involvement) and the `RTDB_IMAGE_*` boot knobs + safety guards. Add `image_transform.rs` to the "Architecture — what spans files" note.

- [ ] **Step 3: `server/src/storage.rs` module doc** — add a sentence pointing to `image_transform.rs` for on-the-fly transforms on serve.

- [ ] **Step 4: `ENHANCEMENTS.md`** — check the `[ ]` for ENH-014 → `[x]`.

- [ ] **Step 5: Vault note** — save the reusable pattern (pure-Rust `image` crate + `moka` cache + `spawn_blocking` + decode `Limits` for DoS-safe on-the-fly transforms) to the Parsidion vault `Patterns/` per CLAUDE-VAULT.md, then rebuild the index.

- [ ] **Step 6: Commit** — `git add -A && git commit -m "docs: ENH-014 image transforms parity + vault note"`.

---

## Self-Review (run after writing)

**Spec coverage:** every spec section maps to a task — API/behavior rules (T2 parse + T5 handlers), resize semantics (T2 `resize`/`contain_target`/cover-crop), HTTP caching (T5 `Cache-Control`), safety/concurrency (T4 semaphore + T2 `Limits`), cache (T4 moka), config (T1), module layout (T2/T4), clients (T6–T8), dashboard (T9), metrics (T3), tests (T2/T5/T6–T9), docs (T10). Non-goals (WebP/AVIF output, EXIF orientation, persistent cache) are explicitly out.

**Type consistency:** `Fit { Cover, Contain, ScaleDown }`, `OutFormat { Jpeg, Png, Auto }`, `TransformParams`, `TransformConfig`, `TransformError`, `apply`, `CachedImage`, `Resolved`, `TransformCache` — names match across T2/T4/T5. `appendImageParams`/`transformUrl` match across T6/T9. `transform_url` + `TransformOpts`/`Fit`/`OutFormat` match across T7/T8.

**Watch-items for implementers:** (a) `image` 0.25 `Limits` + `JpegEncoder` API — pin to the installed version; tests are the contract. (b) `moka::future::Cache` builder + weigher — confirm against installed 0.12.x. (c) `AppState::new` must keep its `(pool, config, hot)` signature so the 8 test sites compile unchanged. (d) `cargo` cannot run concurrently across subagents — sequence server tasks.

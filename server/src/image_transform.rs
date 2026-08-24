//! On-the-fly image transforms on storage serve (ENH-014). Pure-Rust decode →
//! resize → re-encode over the `image` crate, with a bounded-concurrency cache
//! (`TransformCache`, added later). HTTP-only; no committer/protocol involvement.

use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use image::{DynamicImage, GenericImageView, ImageEncoder, ImageFormat, ImageReader};
use tokio::sync::Semaphore;

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
    /// SEC-002: the payload is a **diagnostic only** — it carries `image` crate
    /// error text that can name codecs and internal buffer state. It is logged
    /// at the single call site in `resolve` and never placed in an `RtDbError`
    /// message, which is a fixed string.
    Internal(String),
}

/// Parse transform params from a query map. `Ok(None)` ⇒ passthrough (no
/// transform requested). `Err(BadRequest)` ⇒ invalid value.
impl TransformParams {
    pub fn parse(
        q: &std::collections::HashMap<String, String>,
        cfg: &TransformConfig,
    ) -> Result<Option<TransformParams>, RtDbError> {
        let get = |k: &str| q.get(k).map(|s| s.trim()).filter(|s| !s.is_empty());
        let bad = |m: &str| RtDbError::bad_request(m);

        let w = match get("w") {
            Some(v) => Some(
                v.parse::<u32>()
                    .map_err(|_| bad("w must be a positive integer"))?,
            ),
            None => None,
        };
        let h = match get("h") {
            Some(v) => Some(
                v.parse::<u32>()
                    .map_err(|_| bad("h must be a positive integer"))?,
            ),
            None => None,
        };
        let q = match get("q") {
            Some(v) => Some(v.parse::<u8>().map_err(|_| bad("q must be 1..=100"))?),
            None => None,
        };
        if let Some(v) = q
            && !(1..=100).contains(&v)
        {
            return Err(bad("q must be between 1 and 100"));
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
        Ok(Some(TransformParams {
            w,
            h,
            fit,
            q,
            format,
        }))
    }
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
        Some(f)
            if matches!(
                f,
                ImageFormat::Png
                    | ImageFormat::Jpeg
                    | ImageFormat::Gif
                    | ImageFormat::Bmp
                    | ImageFormat::WebP
            ) =>
        {
            f
        }
        _ => return Err(TransformError::NotImage),
    };

    let mut reader = ImageReader::new(Cursor::new(bytes));
    reader.set_format(src_format);
    // Cap decode cost: refuse sources over the pixel budget. `max_image_width`
    // and `max_image_height` are absolute guards; `max_alloc` ≈ 4 bytes/px.
    // The `image` 0.25 API uses field assignment + the non-Result `limits()` setter.
    let mut limits = image::Limits::default();
    limits.max_image_width = Some(16_384);
    limits.max_image_height = Some(16_384);
    limits.max_alloc = Some(cfg.max_pixels.saturating_mul(4));
    reader.limits(limits);
    let img = match reader.decode() {
        Ok(img) => img,
        Err(image::ImageError::Limits(_)) => return Err(TransformError::TooLarge),
        Err(image::ImageError::Decoding(_)) | Err(image::ImageError::Unsupported(_)) => {
            return Err(TransformError::NotImage);
        }
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
            image::imageops::resize(img, tw, th, filter).into()
        }
        Fit::ScaleDown => {
            let (tw, th) = contain_target(ow, oh, want_w, want_h);
            if tw >= ow && th >= oh {
                img.clone() // smaller than target → keep
            } else {
                image::imageops::resize(img, tw, th, filter).into()
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
                image::imageops::crop_imm(&scaled, x, y, cw, ch)
                    .to_image()
                    .into()
            } else {
                let (tw, th) = contain_target(ow, oh, want_w, want_h);
                image::imageops::resize(img, tw, th, filter).into()
            }
        }
    }
}

/// Fit within the box preserving aspect (the smaller scale wins).
fn contain_target(ow: u32, oh: u32, want_w: u32, want_h: u32) -> (u32, u32) {
    if ow == 0 || oh == 0 {
        return (ow, oh);
    }
    let sw = if want_w != u32::MAX {
        (want_w as f64) / (ow as f64)
    } else {
        f64::MAX
    };
    let sh = if want_h != u32::MAX {
        (want_h as f64) / (oh as f64)
    } else {
        f64::MAX
    };
    let s = sw.min(sh).min(1.0); // contain never upscales
    if s == f64::MAX {
        return (ow, oh);
    }
    let tw = ((ow as f64) * s).round().max(1.0) as u32;
    let th = ((oh as f64) * s).round().max(1.0) as u32;
    (tw.min(want_w).max(1), th.min(want_h).max(1))
}

/// Cached transformed bytes plus the derived content type. `Bytes` so a cache
/// hit hands out a cheap reference-counted handle rather than a copy.
#[derive(Debug, Clone)]
pub struct CachedImage {
    pub bytes: Bytes,
    pub content_type: &'static str,
}

/// SEC-002: the single place a `TransformError::Internal` becomes a client
/// error. The `image` crate's error text is logged against the blob it came
/// from; the returned envelope carries a fixed message (CWE-209).
fn internal_transform_error(db: &str, id: &str, detail: &str) -> RtDbError {
    tracing::warn!(db = %db, file_id = %id, detail = %detail, "image transform failed");
    RtDbError::internal("image transform failed")
}

/// Outcome of a transform-or-passthrough request.
pub enum Resolved {
    Transformed(CachedImage),
    Raw { bytes: Bytes, content_type: String },
}

/// Bounded-concurrency transform cache. Limits the number of in-flight
/// decode→resize→encode pipelines via an internal semaphore, memoizes the
/// results in a byte-weighted `moka` cache, and records hit/miss/error
/// counters on the shared `Metrics` instance.
pub struct TransformCache {
    cache: moka::future::Cache<String, CachedImage>,
    sem: Semaphore,
    cfg: TransformConfig,
    metrics: Arc<crate::metrics::Metrics>,
}

impl TransformCache {
    /// `cache_bytes` is the byte-weight cap (passed as `max_capacity`); the
    /// weigher makes it a proxy for total cached raster bytes. `concurrency`
    /// is the permit count for in-flight transforms (sourced from
    /// `Config::image_concurrency`).
    pub fn new(
        cfg: TransformConfig,
        cache_bytes: u64,
        concurrency: usize,
        metrics: Arc<crate::metrics::Metrics>,
    ) -> Self {
        let cache = moka::future::Cache::builder()
            .max_capacity(cache_bytes)
            .weigher(|_, v: &CachedImage| -> u32 { v.bytes.len().min(u32::MAX as usize) as u32 })
            .build();
        Self {
            cache,
            sem: Semaphore::new(concurrency.max(1)),
            cfg,
            metrics,
        }
    }

    pub fn cfg(&self) -> &TransformConfig {
        &self.cfg
    }

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
        // Cache key is the params' Debug repr — stable only while each field's
        // `{:?}` output stays fixed. A new param or a changed Debug repr must
        // extend/alter this format, or the cache will silently serve stale
        // entries for the old key shape.
        let key = format!(
            "{id}|{:?}|{:?}|{:?}|{:?}|{:?}",
            params.w, params.h, params.fit, params.q, params.format
        );
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
        // `bytes` and `ct` are reused in the `NotImage` branch below, so clone
        // the references the closure needs before moving them.
        let bytes_for_closure = bytes.clone();
        let ct_for_closure = ct.clone();
        let result = tokio::task::spawn_blocking(move || {
            apply(&bytes_for_closure, ct_for_closure.as_deref(), &params, &cfg)
        })
        .await
        .map_err(|e| RtDbError::internal(format!("transform join: {e}")))?;
        drop(permit);
        match result {
            Ok((tbytes, tct)) => {
                let n = tbytes.len() as u64;
                let cached = CachedImage {
                    bytes: Bytes::from(tbytes),
                    content_type: tct,
                };
                self.cache.insert(key, cached.clone()).await;
                self.metrics.record_image_transform_miss(n);
                Ok(Resolved::Transformed(cached))
            }
            Err(TransformError::NotImage) => Ok(Resolved::Raw {
                bytes,
                content_type: ct.unwrap_or_else(|| "application/octet-stream".to_string()),
            }),
            Err(TransformError::TooLarge) => {
                self.metrics.record_image_transform_error();
                Err(RtDbError::bad_request(
                    "image exceeds max pixels for transform",
                ))
            }
            Err(TransformError::Internal(detail)) => {
                self.metrics.record_image_transform_error();
                Err(internal_transform_error(db, id, &detail))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use image::{ImageBuffer, Rgba};

    fn cfg() -> TransformConfig {
        TransformConfig {
            enabled: true,
            max_dim: 2048,
            max_pixels: 25_000_000,
            default_quality: 80,
        }
    }

    fn png_bytes(w: u32, h: u32) -> Vec<u8> {
        let img: ImageBuffer<Rgba<u8>, Vec<u8>> = ImageBuffer::new(w, h);
        let mut buf = Vec::new();
        image::DynamicImage::ImageRgba8(img)
            .write_to(&mut std::io::Cursor::new(&mut buf), image::ImageFormat::Png)
            .unwrap();
        buf
    }

    #[test]
    fn parse_none_when_no_params() {
        let q = std::collections::HashMap::new();
        assert!(TransformParams::parse(&q, &cfg()).unwrap().is_none());
    }

    #[test]
    fn parse_rejects_bad_values() {
        let c = cfg();
        let mk = |s: &str| {
            let mut q = std::collections::HashMap::new();
            q.insert("w".into(), s.into());
            q
        };
        assert!(TransformParams::parse(&mk("0"), &c).is_err());
        assert!(TransformParams::parse(&mk("99999"), &c).is_err());
        let mut q = std::collections::HashMap::new();
        q.insert("fit".into(), "bogus".into());
        assert!(TransformParams::parse(&q, &c).is_err());
        let mut q = std::collections::HashMap::new();
        q.insert("q".into(), "200".into());
        assert!(TransformParams::parse(&q, &c).is_err());
    }

    #[test]
    fn apply_contain_fits_within_box() {
        let c = cfg();
        let src = png_bytes(400, 200);
        let p = TransformParams {
            w: Some(100),
            h: Some(100),
            fit: Fit::Contain,
            q: None,
            format: OutFormat::Auto,
        };
        let (out, ct) = apply(&src, Some("image/png"), &p, &c).unwrap();
        assert_eq!(ct, "image/png");
        let d = image::load_from_memory(&out).unwrap();
        assert_eq!(d.dimensions(), (100, 50)); // 2:1 aspect preserved, width-bound
    }

    #[test]
    fn apply_cover_crops_to_exact() {
        let c = cfg();
        let src = png_bytes(400, 200);
        let p = TransformParams {
            w: Some(100),
            h: Some(100),
            fit: Fit::Cover,
            q: None,
            format: OutFormat::Auto,
        };
        let (out, _) = apply(&src, Some("image/png"), &p, &c).unwrap();
        assert_eq!(
            image::load_from_memory(&out).unwrap().dimensions(),
            (100, 100)
        );
    }

    #[test]
    fn apply_scale_down_never_upscales() {
        let c = cfg();
        let src = png_bytes(50, 50);
        let p = TransformParams {
            w: Some(200),
            h: Some(200),
            fit: Fit::ScaleDown,
            q: None,
            format: OutFormat::Auto,
        };
        let (out, _) = apply(&src, Some("image/png"), &p, &c).unwrap();
        assert_eq!(
            image::load_from_memory(&out).unwrap().dimensions(),
            (50, 50)
        );
    }

    #[test]
    fn apply_format_jpeg_with_quality() {
        let c = cfg();
        let src = png_bytes(100, 100);
        let p = TransformParams {
            w: None,
            h: None,
            fit: Fit::Contain,
            q: Some(40),
            format: OutFormat::Jpeg,
        };
        let (out, ct) = apply(&src, Some("image/png"), &p, &c).unwrap();
        assert_eq!(ct, "image/jpeg");
        assert_eq!(image::guess_format(&out).unwrap(), image::ImageFormat::Jpeg);
    }

    #[test]
    fn apply_non_image_returns_not_image() {
        let c = cfg();
        let p = TransformParams {
            w: Some(10),
            h: None,
            fit: Fit::Contain,
            q: None,
            format: OutFormat::Auto,
        };
        assert!(matches!(
            apply(b"not an image", None, &p, &c),
            Err(TransformError::NotImage)
        ));
    }

    #[test]
    fn apply_over_pixel_cap_returns_too_large() {
        let c = TransformConfig {
            enabled: true,
            max_dim: 2048,
            max_pixels: 10_000,
            default_quality: 80,
        };
        let src = png_bytes(200, 200); // 40k px > 10k cap
        let p = TransformParams {
            w: Some(50),
            h: None,
            fit: Fit::Contain,
            q: None,
            format: OutFormat::Auto,
        };
        assert!(matches!(
            apply(&src, Some("image/png"), &p, &c),
            Err(TransformError::TooLarge)
        ));
    }

    /// SEC-002 (CWE-209): the `image` crate's error text must never reach the
    /// client. `internal_transform_error` is the sole place a
    /// `TransformError::Internal` becomes an `RtDbError`, so asserting its
    /// message is fixed covers every leak path out of `apply`.
    #[test]
    fn internal_transform_error_message_is_generic() {
        let detail = "Format error decoding Jpeg: /secret/path/blob.jpg: invalid marker 0xFF";
        let err = internal_transform_error("acme", "file_123", detail);
        assert_eq!(err.message, "image transform failed");
        assert!(
            !err.message.contains("Jpeg") && !err.message.contains("/secret/path"),
            "internal detail leaked into the client envelope: {}",
            err.message
        );
    }

    /// A corrupt image body is classified as `NotImage` (served raw), not as an
    /// `Internal` carrying decoder text.
    #[test]
    fn apply_on_corrupt_jpeg_does_not_produce_internal() {
        let c = cfg();
        let p = TransformParams {
            w: Some(10),
            h: None,
            fit: Fit::Contain,
            q: None,
            format: OutFormat::Auto,
        };
        // A valid JPEG SOI marker followed by garbage: sniffs as JPEG, fails to
        // decode.
        let mut corrupt = vec![0xFF, 0xD8, 0xFF, 0xE0];
        corrupt.extend_from_slice(&[0u8; 64]);
        assert!(
            !matches!(
                apply(&corrupt, Some("image/jpeg"), &p, &c),
                Err(TransformError::Internal(_))
            ),
            "a corrupt source must not surface decoder text as Internal"
        );
    }
}

use super::RtDbHttpClient;
use crate::error::RtDbError;
use crate::http_common::AuthedRequest;

/// Result of [`RtDbHttpClient::upload`] — the server-computed file identity,
/// content hash, size in bytes, and (if the upload carried one) the stored
/// `contentType`. `contentType` is `#[serde(default)]` so an older server
/// omitting the field deserializes to `None`.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadResult {
    /// Server-assigned opaque file id (usable in `get_url`/serve routes).
    pub id: String,
    /// SHA-256 hex digest of the stored bytes.
    pub sha256: String,
    /// Size in bytes.
    pub size: i64,
    #[serde(default)]
    /// The upload's `Content-Type`, when the server recorded one.
    pub content_type: Option<String>,
}

/// File metadata returned by [`RtDbHttpClient::get_file_metadata`]. Mirrors
/// `UploadResult` plus the server-recorded `creationTime`.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileMetadata {
    /// Server-assigned opaque file id.
    pub id: String,
    /// SHA-256 hex digest of the stored bytes.
    pub sha256: String,
    /// Size in bytes.
    pub size: i64,
    #[serde(default)]
    /// The stored `Content-Type`, when the server recorded one.
    pub content_type: Option<String>,
    /// Upload timestamp, epoch milliseconds.
    pub creation_time: i64,
}

/// A signed, time-limited storage URL minted by the server
/// (`GET /api/storage/{db}/{id}/signed-url`). `expires_at` is epoch
/// milliseconds; the URL stops working after it.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignedUrl {
    /// The signed public URL.
    pub url: String,
    /// Absolute expiry, epoch milliseconds — the URL stops working after it.
    pub expires_at: i64,
}

/// Image-transform `fit` mode for [`RtDbHttpClient::transform_url`] (ENH-014).
/// The serde rename is `kebab-case` so `ScaleDown` serializes as `scale-down`
/// on the wire; `as_str` mirrors that for the hand-built query string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Fit {
    #[default]
    /// Fit entirely inside the target box, preserving aspect ratio (default).
    Contain,
    /// Fill the box, cropping the overflow.
    Cover,
    /// Never upscale; downscale as `Contain` would.
    ScaleDown,
}

impl Fit {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Fit::Cover => "cover",
            Fit::Contain => "contain",
            Fit::ScaleDown => "scale-down",
        }
    }
}

/// Output format for [`RtDbHttpClient::transform_url`] (ENH-014).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OutFormat {
    #[default]
    /// Let the server pick (default).
    Auto,
    /// JPEG output.
    Jpeg,
    /// PNG output.
    Png,
}

/// Optional image-transform parameters for [`RtDbHttpClient::transform_url`]
/// (ENH-014). All fields are optional; only `Some` fields appear in the query.
#[derive(Debug, Clone, Default)]
pub struct TransformOpts {
    /// Target width in pixels.
    pub w: Option<u32>,
    /// Target height in pixels.
    pub h: Option<u32>,
    /// Resize fit mode.
    pub fit: Option<Fit>,
    /// JPEG quality (1-100).
    pub q: Option<u8>,
    /// Output format.
    pub format: Option<OutFormat>,
}

impl RtDbHttpClient {
    /// Upload raw bytes; `content_type` sets the Content-Type header and is
    /// stored as the file's type. Returns the server-computed metadata.
    ///
    /// For large files prefer [`Self::upload_stream`], which streams the body
    /// through reqwest instead of buffering it whole in memory.
    pub async fn upload(
        &self,
        bytes: &[u8],
        content_type: Option<&str>,
    ) -> Result<UploadResult, RtDbError> {
        let mut req = self
            .client
            .post(format!("{}/api/storage/{}", self.url, self.db))
            .authed(&self.token)
            .body(bytes.to_vec());
        if let Some(ct) = content_type {
            req = req.header(reqwest::header::CONTENT_TYPE, ct);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("upload request failed: {e}")))?;
        crate::http_common::deserialize::<UploadResult>(resp).await
    }

    /// Upload a streaming body without buffering it whole in memory. The server
    /// (ENH-021) streams the request through to storage, so a file of any size
    /// only occupies one chunk in flight at a time.
    ///
    /// `stream` is any `TryStream` whose `Ok` item converts into `bytes::Bytes`
    /// (e.g. `Bytes`, `Vec<u8>`, `&'static [u8]`); chunks are forwarded to
    /// reqwest's streaming body as they arrive. Pass `content_type` to set the
    /// Content-Type header and the file's stored type, exactly like
    /// [`Self::upload`].
    pub async fn upload_stream<S>(
        &self,
        stream: S,
        content_type: Option<&str>,
    ) -> Result<UploadResult, RtDbError>
    where
        S: futures_util::stream::TryStream + Send + 'static,
        bytes::Bytes: From<S::Ok>,
        S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let body = reqwest::Body::wrap_stream(stream);
        let mut req = self
            .client
            .post(format!("{}/api/storage/{}", self.url, self.db))
            .authed(&self.token)
            .body(body);
        if let Some(ct) = content_type {
            req = req.header(reqwest::header::CONTENT_TYPE, ct);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("upload request failed: {e}")))?;
        crate::http_common::deserialize::<UploadResult>(resp).await
    }

    /// Delete the file `id` (`DELETE /api/storage/{db}/{id}`) — also revokes
    /// its public serve URL. Idempotent: deleting an unknown id still returns
    /// `Ok(())`.
    pub async fn delete_file(&self, id: &str) -> Result<(), RtDbError> {
        let resp = self
            .client
            .delete(format!("{}/api/storage/{}/{id}", self.url, self.db))
            .authed(&self.token)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("delete file request failed: {e}")))?;
        #[derive(serde::Deserialize)]
        struct OkResp {
            ok: bool,
        }
        let parsed = crate::http_common::deserialize::<OkResp>(resp).await?;
        if !parsed.ok {
            return Err(RtDbError::internal("delete file returned ok=false"));
        }
        Ok(())
    }

    /// Fetch stored metadata for `id` via `GET /api/storage/{db}/{id}/metadata`.
    pub async fn get_file_metadata(&self, id: &str) -> Result<FileMetadata, RtDbError> {
        let resp = self
            .client
            .get(format!(
                "{}/api/storage/{}/{id}/metadata",
                self.url, self.db
            ))
            .authed(&self.token)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("file metadata request failed: {e}")))?;
        crate::http_common::deserialize::<FileMetadata>(resp).await
    }

    /// Mint an HMAC-signed, time-limited public URL for `id` via
    /// `GET /api/storage/{db}/{id}/signed-url`. Pass `ttl_seconds` to override
    /// the server default lifetime; `None` omits the query parameter. Unlike
    /// [`get_url`](Self::get_url), this makes a network request.
    pub async fn get_signed_url(
        &self,
        id: &str,
        ttl_seconds: Option<u64>,
    ) -> Result<SignedUrl, RtDbError> {
        let mut req = self
            .client
            .get(format!(
                "{}/api/storage/{}/{id}/signed-url",
                self.url, self.db
            ))
            .authed(&self.token);
        if let Some(ttl) = ttl_seconds {
            req = req.query(&[("ttlSeconds", ttl)]);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("signed url request failed: {e}")))?;
        crate::http_common::deserialize::<SignedUrl>(resp).await
    }

    /// The public serve URL — no request is made.
    pub fn get_url(&self, id: &str) -> String {
        format!("{}/storage/{id}", self.url)
    }

    /// The public serve URL for `id` with image-transform query params appended
    /// (ENH-014). Params are emitted in fixed order `w, h, fit, q, format` and
    /// only when `Some`. No request is made.
    pub fn transform_url(&self, id: &str, opts: &TransformOpts) -> String {
        let base = format!("{}/storage/{id}", self.url);
        let mut parts: Vec<String> = Vec::new();
        if let Some(w) = opts.w {
            parts.push(format!("w={w}"));
        }
        if let Some(h) = opts.h {
            parts.push(format!("h={h}"));
        }
        if let Some(fit) = opts.fit {
            parts.push(format!("fit={}", fit.as_str()));
        }
        if let Some(q) = opts.q {
            parts.push(format!("q={q}"));
        }
        if let Some(f) = opts.format {
            // `Auto` is the server default; omit it so the URL stays minimal.
            let s = match f {
                OutFormat::Auto => "",
                OutFormat::Jpeg => "jpeg",
                OutFormat::Png => "png",
            };
            if !s.is_empty() {
                parts.push(format!("format={s}"));
            }
        }
        if parts.is_empty() {
            base
        } else {
            format!("{base}?{}", parts.join("&"))
        }
    }
}

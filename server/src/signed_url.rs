//! Signed, time-limited storage URLs.
//!
//! A signed URL is `GET /storage/{id}?exp=<unix-ms>&sig=<hex>` where
//! `sig = HMAC-SHA256(signing_key, "{id}.{exp}")`. The signing key is derived
//! once at boot from the server's required `admin_key` and held on `AppState`,
//! so the feature needs no extra configuration. Rotating `admin_key` changes the
//! derived key and invalidates every outstanding signed URL (a desirable
//! "revoke all signed access" side effect). See
//! docs/superpowers/specs/2026-08-08-signed-storage-urls-design.md.

/// Domain-separation label mixed into key derivation so a signed URL (which
/// exposes only `id`, `exp`, and the signature — never the key) cannot be
/// turned into an admin credential, and `admin_key` is never placed directly on
/// the public serve path.
const LABEL: &[u8] = b"rtdb-storage-signing-v1";

/// Default TTL when a mint request omits `ttlSeconds`: 1 hour.
pub const DEFAULT_SIGNED_URL_TTL_SECS: u64 = 3600;

/// Upper bound on a minted TTL: 7 days. A compile-time const (not an env knob)
/// keeps the feature zero-config; raising it is a code change.
pub const MAX_SIGNED_URL_TTL_SECS: u64 = 7 * 24 * 60 * 60;

/// Derives the storage signing key from `admin_key`. Two-level HKDF-style
/// derivation: the label is HMAC'd under the raw admin key, and the result
/// becomes the HMAC key for signing URLs. `ring::hmac::Key` is `Send + Sync`,
/// so it is safe to share via `Arc<ring::hmac::Key>` on `AppState`.
pub fn derive_key(admin_key: &str) -> ring::hmac::Key {
    let seed = ring::hmac::sign(
        &ring::hmac::Key::new(ring::hmac::HMAC_SHA256, admin_key.as_bytes()),
        LABEL,
    );
    ring::hmac::Key::new(ring::hmac::HMAC_SHA256, seed.as_ref())
}

/// Hex HMAC-SHA256 over `"{id}.{exp}"`. Hex (not base64) keeps the URL free of
/// `+/=` URL-encoding hazards.
pub fn sign(key: &ring::hmac::Key, id: &str, exp_ms: i64) -> String {
    let msg = format!("{id}.{exp_ms}");
    hex::encode(ring::hmac::sign(key, msg.as_bytes()).as_ref())
}

/// Constant-time verification. Returns `false` for a non-hex signature, a
/// mismatched key, or any difference in `id`/`exp` (the compare itself is
/// constant-time via `ring::hmac::verify`; the `false` return for bad hex is
/// not timing-sensitive because it reveals only "malformed", not a near-miss).
pub fn verify(key: &ring::hmac::Key, id: &str, exp_ms: i64, sig_hex: &str) -> bool {
    let Ok(sig_bytes) = hex::decode(sig_hex) else {
        return false;
    };
    let msg = format!("{id}.{exp_ms}");
    ring::hmac::verify(key, msg.as_bytes(), &sig_bytes).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXP: i64 = 1_700_000_000_000;

    #[test]
    fn sign_verify_roundtrip() {
        let key = derive_key("secret-admin-key");
        let sig = sign(&key, "fileid123", EXP);
        assert!(verify(&key, "fileid123", EXP, &sig));
    }

    #[test]
    fn verify_rejects_tampered_signature() {
        let key = derive_key("secret-admin-key");
        let sig = sign(&key, "fileid123", EXP);
        // Flip the last hex digit to a different valid hex char.
        let mut chars: Vec<char> = sig.chars().collect();
        let last_idx = chars.len() - 1;
        let last = chars[last_idx];
        chars[last_idx] = if last == '0' { '1' } else { '0' };
        let tampered: String = chars.into_iter().collect();
        assert!(!verify(&key, "fileid123", EXP, &tampered));
    }

    #[test]
    fn verify_rejects_tampered_id() {
        let key = derive_key("secret-admin-key");
        let sig = sign(&key, "fileid123", EXP);
        assert!(!verify(&key, "tampered", EXP, &sig));
    }

    #[test]
    fn verify_rejects_tampered_exp() {
        let key = derive_key("secret-admin-key");
        let sig = sign(&key, "fileid123", EXP);
        assert!(!verify(&key, "fileid123", EXP + 1, &sig));
    }

    #[test]
    fn verify_rejects_different_key() {
        let sig = sign(&derive_key("key-a"), "fileid123", EXP);
        assert!(!verify(&derive_key("key-b"), "fileid123", EXP, &sig));
    }

    #[test]
    fn verify_rejects_non_hex_signature() {
        let key = derive_key("secret-admin-key");
        assert!(!verify(&key, "fileid123", EXP, "not-hex!!"));
    }
}

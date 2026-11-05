/// Percent-encode `s` the way the browser's `encodeURIComponent` does, so a
/// schedule id interpolated into a URL path segment is byte-identical to what
/// the TS client sends (`encodeURIComponent(id)`). Today ids are server-
/// generated uuid v7 (`[0-9a-f-]`, all unescaped), so this is a no-op in
/// practice — it exists for cross-client consistency if that ever changes.
pub(super) fn encode_uri_component(s: &str) -> String {
    const UNESCAPED: &str = "-_.!~*'()";
    let mut out = String::with_capacity(s.len());
    // Byte-wise iteration reproduces `encodeURIComponent`: ASCII chars are
    // encoded one-for-one, and every byte of a multi-byte UTF-8 sequence is
    // >= 0x80 (non-alphanumeric) so it is percent-encoded individually — which
    // is exactly the UTF-8 percent-encoding the browser emits.
    for &b in s.as_bytes() {
        let c = b as char;
        if c.is_ascii_alphanumeric() || UNESCAPED.contains(c) {
            out.push(c);
        } else {
            out.push('%');
            // uppercase hex, matching encodeURIComponent
            out.push_str(&format!("{:02X}", b));
        }
    }
    out
}

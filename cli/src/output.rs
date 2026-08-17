//! Output formatting helpers shared by the subcommand handlers.

use anyhow::anyhow;
use par_rt_db_client::RtDbError;

/// Surface an `RtDbError` as `<CODE>: <message>`. `RtDbError`'s own Display
/// (via thiserror) is just the message, so the wire code is recovered here by
/// serializing `ErrorCode` (it carries `#[serde(rename_all = "SCREAMING_SNAKE_CASE")]`).
pub(crate) fn map_err(e: RtDbError) -> anyhow::Error {
    let code = serde_json::to_value(e.code)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| format!("{:?}", e.code));
    anyhow!("{code}: {}", e.message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use par_rt_db_client::ErrorCode;

    #[test]
    fn map_err_surfaces_code_and_message() {
        let e = RtDbError::new(ErrorCode::NotFound, "missing thing");
        assert_eq!(map_err(e).to_string(), "NOT_FOUND: missing thing");
    }
}

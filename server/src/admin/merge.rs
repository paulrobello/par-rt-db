//! FM-27 admin escape hatch: run the anon→real merge synchronously and return
//! the full report. Use: crash-window cleanup (the inert-orphan case between
//! steps 3 and 4 of the merge order), manual consolidation, testing.

use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use axum::http::HeaderMap;
use serde::Deserialize;

use crate::error::RtDbError;
use crate::http_api::ApiJson;
use crate::{AppState, merge};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct MergeUsersRequest {
    anon_user_id: String,
    real_user_id: String,
    confirm: String,
}

/// `POST /admin/merge-users` — admin-gated (router layer), typed-confirmation-
/// guarded anon→real merge. `confirm` must equal `realUserId` exactly (same
/// guard pattern as `delete-db`/`restore`).
///
/// A missing anon row is refused HERE, not by the orchestrator: `merge_users`
/// treats a missing row as a completed merge (the idempotency the OAuth
/// callback path requires), so the endpoint does its own existence check to
/// honor the spec's "refuses when the anon row does not exist" at the route.
/// A non-anon source row falls through and surfaces the orchestrator's own
/// 400 refusal.
pub(super) async fn merge_users_handler(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    ApiJson(body): ApiJson<MergeUsersRequest>,
) -> Result<Json<merge::MergeReport>, RtDbError> {
    if body.confirm != body.real_user_id {
        return Err(RtDbError::bad_request("confirm must equal realUserId"));
    }
    let anon: Option<(bool,)> =
        sqlx::query_as("SELECT anonymous FROM rtdb_auth.users WHERE id = $1")
            .bind(&body.anon_user_id)
            .fetch_optional(&state.pool)
            .await?;
    if anon.is_none() {
        return Err(RtDbError::not_found(
            "anonymous user not found; nothing to merge",
        ));
    }
    let report = merge::merge_users(&state, &body.anon_user_id, &body.real_user_id).await?;
    Ok(Json(report))
}

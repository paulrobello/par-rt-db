use super::RtDbHttpClient;
use super::url::encode_uri_component;
use crate::error::RtDbError;
use crate::http_common::AuthedRequest;
use crate::wire::{BulkOpAccepted, BulkOpInfo, FilterExpr};
use serde::Serialize;

impl RtDbHttpClient {
    /// Start a background bulk delete (`POST /api/bulk/delete`): the server
    /// matches `filter` against `table`, reports the match count in the ack,
    /// then deletes the matches asynchronously in chunks. Returns the op's
    /// [`BulkOpAccepted::op_id`] for [`bulk_status`](Self::bulk_status)
    /// polling. Mirrors `ts-client`'s `bulkDelete`.
    pub async fn bulk_delete(
        &self,
        table: &str,
        filter: FilterExpr,
    ) -> Result<BulkOpAccepted, RtDbError> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Body<'a> {
            db: &'a str,
            table: &'a str,
            filter: &'a FilterExpr,
        }
        let body = Body {
            db: &self.db,
            table,
            filter: &filter,
        };
        let resp = self
            .client
            .post(format!("{}/api/bulk/delete", self.url))
            .authed(&self.token)
            .json(&body)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("bulk delete request failed: {e}")))?;
        let parsed = crate::http_common::deserialize::<BulkOpAccepted>(resp).await?;
        Ok(parsed)
    }

    /// Poll a background bulk op's current state (`GET /api/bulk/{id}?db=<db>`
    /// — the same route for delete ops; id is caller-supplied, so the path
    /// segment is percent-encoded). `error` on the returned [`BulkOpInfo`] is
    /// present only on the `failed` state.
    pub async fn bulk_status(&self, id: &str) -> Result<BulkOpInfo, RtDbError> {
        let resp = self
            .client
            .get(format!(
                "{}/api/bulk/{}",
                self.url,
                encode_uri_component(id)
            ))
            .authed(&self.token)
            .query(&[("db", self.db.as_str())])
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("bulk status request failed: {e}")))?;
        let parsed = crate::http_common::deserialize::<BulkOpInfo>(resp).await?;
        Ok(parsed)
    }
}

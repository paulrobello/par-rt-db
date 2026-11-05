use super::RtDbHttpClient;
use super::url::encode_uri_component;
use crate::error::RtDbError;
use crate::http_common::AuthedRequest;
use crate::mutation::Transaction;
use crate::wire::{
    ClaimedSchedule, ScheduleInfo, ScheduleWhen, WorkflowInfo, WorkflowSpec, WorkflowStatus,
};
use serde::Serialize;

impl RtDbHttpClient {
    /// Schedule `txn` to fire at `when`. The server validates cron expressions
    /// and resolves the due time; the client does no schedule arithmetic. Returns
    /// the new schedule's id. Mirrors `ts-client`'s `schedule`. Pass `external =
    /// true` to create an external job instead: one never executed by the server
    /// — an application worker claims it via `POST /api/schedule/claim`
    /// ([`claim_schedules`](Self::claim_schedules) is TS-only; the rust worker
    /// calls the route directly) and finalizes it with the returned
    /// `leaseGeneration` fencing token. The field is omitted on the wire when
    /// false, matching the server's `#[serde(default)]`.
    pub async fn schedule(
        &self,
        txn: &Transaction,
        when: ScheduleWhen,
        external: bool,
    ) -> Result<String, RtDbError> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Body<'a> {
            db: &'a str,
            when: ScheduleWhen,
            txn: &'a Transaction,
            #[serde(skip_serializing_if = "std::ops::Not::not")]
            external: bool,
        }
        let body = Body {
            db: &self.db,
            when,
            txn,
            external,
        };
        let resp = self
            .client
            .post(format!("{}/api/schedule", self.url))
            .authed(&self.token)
            .json(&body)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("schedule request failed: {e}")))?;
        #[derive(serde::Deserialize)]
        struct ScheduleResponse {
            id: String,
        }
        let parsed = crate::http_common::deserialize::<ScheduleResponse>(resp).await?;
        Ok(parsed.id)
    }

    /// Cancel a scheduled job (`POST /api/schedule/{id}/cancel`). Resolves
    /// `true` on `{ok:true}`; resolves `false` on `{ok:false}` (unknown or
    /// already-terminal id — a no-op, not a failure, matching the WS ack
    /// contract of `RtDbClient::cancel_schedule` (feature `ws`));
    /// rejects with [`RtDbError`] on a non-2xx error envelope.
    pub async fn cancel_schedule(&self, id: &str) -> Result<bool, RtDbError> {
        self.manage_schedule(id, "cancel").await
    }

    /// Pause a scheduled job until [`resume_schedule`](Self::resume_schedule) is
    /// called (`POST /api/schedule/{id}/pause`). Same ack contract as
    /// [`cancel_schedule`](Self::cancel_schedule).
    pub async fn pause_schedule(&self, id: &str) -> Result<bool, RtDbError> {
        self.manage_schedule(id, "pause").await
    }

    /// Resume a paused scheduled job (`POST /api/schedule/{id}/resume`). Same
    /// ack contract as [`cancel_schedule`](Self::cancel_schedule).
    pub async fn resume_schedule(&self, id: &str) -> Result<bool, RtDbError> {
        self.manage_schedule(id, "resume").await
    }

    /// Shared authorize-then-op body for the three boolean manage handlers. `op`
    /// is always a hardcoded literal ("cancel" | "pause" | "resume"), never
    /// caller-supplied, so interpolating it into the path is safe. A 200
    /// `{ok:false}` is the no-op signal — resolves `false`, not an error.
    async fn manage_schedule(&self, id: &str, op: &str) -> Result<bool, RtDbError> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Body<'a> {
            db: &'a str,
        }
        let body = Body { db: &self.db };
        let resp = self
            .client
            // `id` is caller-supplied, so percent-encode the path segment;
            // `op` is always a hardcoded literal, never interpolated raw from input.
            .post(format!(
                "{}/api/schedule/{}/{op}",
                self.url,
                encode_uri_component(id)
            ))
            .authed(&self.token)
            .json(&body)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("schedule {op} request failed: {e}")))?;
        #[derive(serde::Deserialize)]
        struct ManageResponse {
            ok: bool,
        }
        let parsed = crate::http_common::deserialize::<ManageResponse>(resp).await?;
        Ok(parsed.ok)
    }

    /// List scheduled jobs for this client's database (`POST /api/schedules`).
    pub async fn list_schedules(&self) -> Result<Vec<ScheduleInfo>, RtDbError> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Body<'a> {
            db: &'a str,
        }
        #[derive(serde::Deserialize)]
        struct ListResponse {
            schedules: Vec<ScheduleInfo>,
        }
        let body = Body { db: &self.db };
        let resp = self
            .client
            .post(format!("{}/api/schedules", self.url))
            .authed(&self.token)
            .json(&body)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("list schedules request failed: {e}")))?;
        let parsed = crate::http_common::deserialize::<ListResponse>(resp).await?;
        Ok(parsed.schedules)
    }

    /// Atomically claim due external jobs for an application worker
    /// (`POST /api/schedule/claim`). Each claim assigns the per-job monotonic
    /// `lease_generation` fencing token and stamps the lease deadline; the
    /// finalize methods ([`complete_schedule`](Self::complete_schedule) /
    /// [`retry_schedule`](Self::retry_schedule) /
    /// [`fail_schedule`](Self::fail_schedule)) reject with `CONFLICT` once a
    /// newer generation exists. External jobs are never executed by the
    /// server's internal scheduler. Mirrors `ts-client`'s `claimSchedules`.
    pub async fn claim_schedules(
        &self,
        limit: Option<i64>,
        lease_ms: Option<i64>,
    ) -> Result<Vec<ClaimedSchedule>, RtDbError> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Body<'a> {
            db: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            limit: Option<i64>,
            #[serde(skip_serializing_if = "Option::is_none")]
            lease_ms: Option<i64>,
        }
        let body = Body {
            db: &self.db,
            limit,
            lease_ms,
        };
        let resp = self
            .client
            .post(format!("{}/api/schedule/claim", self.url))
            .authed(&self.token)
            .json(&body)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("claim schedules request failed: {e}")))?;
        #[derive(serde::Deserialize)]
        struct ClaimResponse {
            jobs: Vec<ClaimedSchedule>,
        }
        let parsed = crate::http_common::deserialize::<ClaimResponse>(resp).await?;
        Ok(parsed.jobs)
    }

    /// Finalize one external claim: shared body for
    /// [`complete_schedule`](Self::complete_schedule) /
    /// [`retry_schedule`](Self::retry_schedule) /
    /// [`fail_schedule`](Self::fail_schedule). `op` is always a hardcoded
    /// literal, never caller-supplied, so interpolating it into the path is
    /// safe. A stale or non-current `lease` arrives as a non-2xx `CONFLICT`
    /// error envelope and rejects.
    async fn finalize_schedule(
        &self,
        id: &str,
        op: &str,
        lease: i64,
        delay_ms: Option<i64>,
        error: Option<&str>,
    ) -> Result<(), RtDbError> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Body<'a> {
            db: &'a str,
            lease: i64,
            #[serde(skip_serializing_if = "Option::is_none")]
            delay_ms: Option<i64>,
            #[serde(skip_serializing_if = "Option::is_none")]
            error: Option<&'a str>,
        }
        let body = Body {
            db: &self.db,
            lease,
            delay_ms,
            error,
        };
        let resp = self
            .client
            .post(format!(
                "{}/api/schedule/{}/{op}",
                self.url,
                encode_uri_component(id)
            ))
            .authed(&self.token)
            .json(&body)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("schedule {op} request failed: {e}")))?;
        #[derive(serde::Deserialize)]
        struct FinalizeResponse {
            ok: bool,
        }
        let parsed = crate::http_common::deserialize::<FinalizeResponse>(resp).await?;
        // The server only ever answers `{ok:true}` here — a stale token or a
        // non-running row arrives as a non-2xx `CONFLICT` error envelope.
        debug_assert!(parsed.ok);
        Ok(())
    }

    /// Complete an externally-claimed job with its claim's fencing token
    /// (`POST /api/schedule/{id}/complete`). One-shot jobs are deleted (the
    /// same terminal as internal one-shot success); cron/interval jobs advance
    /// to their next due instant. A stale lease rejects with `CONFLICT`.
    pub async fn complete_schedule(&self, id: &str, lease: i64) -> Result<(), RtDbError> {
        self.finalize_schedule(id, "complete", lease, None, None)
            .await
    }

    /// Re-arm an externally-claimed job that failed mid-execution
    /// (`POST /api/schedule/{id}/retry`). Re-arms at `now + delay_ms`
    /// (server default 60_000) and records `error` on the job as `last_error`
    /// while keeping the generation history. A stale lease rejects with
    /// `CONFLICT`.
    pub async fn retry_schedule(
        &self,
        id: &str,
        lease: i64,
        delay_ms: Option<i64>,
        error: Option<&str>,
    ) -> Result<(), RtDbError> {
        self.finalize_schedule(id, "retry", lease, delay_ms, error)
            .await
    }

    /// Mark an externally-claimed job as terminally failed with its claim's
    /// fencing token (`POST /api/schedule/{id}/fail`); `error` is recorded as
    /// `last_error`. A stale lease rejects with `CONFLICT`.
    pub async fn fail_schedule(&self, id: &str, lease: i64, error: &str) -> Result<(), RtDbError> {
        self.finalize_schedule(id, "fail", lease, None, Some(error))
            .await
    }

    /// Start a durable workflow run (`POST /api/workflows`, FM-29). Returns
    /// the new run's id. Mirrors `ts-client`'s `startWorkflow`.
    pub async fn start_workflow(&self, spec: &WorkflowSpec) -> Result<String, RtDbError> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Body<'a> {
            db: &'a str,
            spec: &'a WorkflowSpec,
        }
        let body = Body { db: &self.db, spec };
        let resp = self
            .client
            .post(format!("{}/api/workflows", self.url))
            .authed(&self.token)
            .json(&body)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("start workflow request failed: {e}")))?;
        #[derive(serde::Deserialize)]
        struct StartResponse {
            id: String,
        }
        let parsed = crate::http_common::deserialize::<StartResponse>(resp).await?;
        Ok(parsed.id)
    }

    /// Cancel a workflow run (`POST /api/workflows/{id}/cancel`, FM-29).
    /// Returns `false` for a missing or already-terminal run — a no-op, not
    /// an error.
    pub async fn cancel_workflow(&self, id: &str) -> Result<bool, RtDbError> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Body<'a> {
            db: &'a str,
        }
        let body = Body { db: &self.db };
        let resp = self
            .client
            // `id` is caller-supplied, so percent-encode the path segment
            // (same guard as `manage_schedule`).
            .post(format!(
                "{}/api/workflows/{}/cancel",
                self.url,
                encode_uri_component(id)
            ))
            .authed(&self.token)
            .json(&body)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("cancel workflow request failed: {e}")))?;
        #[derive(serde::Deserialize)]
        struct CancelResponse {
            cancelled: bool,
        }
        let parsed = crate::http_common::deserialize::<CancelResponse>(resp).await?;
        Ok(parsed.cancelled)
    }

    /// Deliver a named signal to a waiting run (`POST /api/workflows/{id}/signal`
    /// — an `awaitSignal` step's release). Typed 404 (unknown run) / 409 (not
    /// waiting, or waiting on a different name) reject as [`RtDbError`];
    /// success resolves `true` (delivered). `payload` is latest-wins — a
    /// second delivery overwrites the first — and rides the step's outcome
    /// as `signal`.
    pub async fn signal_workflow(
        &self,
        id: &str,
        name: &str,
        payload: Option<serde_json::Value>,
    ) -> Result<bool, RtDbError> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Body<'a> {
            db: &'a str,
            name: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            payload: Option<&'a serde_json::Value>,
        }
        let body = Body {
            db: &self.db,
            name,
            payload: payload.as_ref(),
        };
        let resp = self
            .client
            // `id` is caller-supplied, so percent-encode the path segment
            // (same guard as `cancel_workflow`).
            .post(format!(
                "{}/api/workflows/{}/signal",
                self.url,
                encode_uri_component(id)
            ))
            .authed(&self.token)
            .json(&body)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("signal workflow request failed: {e}")))?;
        #[derive(serde::Deserialize)]
        struct SignalResponse {
            delivered: bool,
        }
        let parsed = crate::http_common::deserialize::<SignalResponse>(resp).await?;
        Ok(parsed.delivered)
    }

    /// List this database's workflow runs, newest first
    /// (`POST /api/workflows/list`, FM-29). `status` optionally filters by
    /// run state. Mirrors `ts-client`'s `listWorkflows`.
    pub async fn list_workflows(
        &self,
        status: Option<WorkflowStatus>,
    ) -> Result<Vec<WorkflowInfo>, RtDbError> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Body<'a> {
            db: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            status: Option<WorkflowStatus>,
        }
        #[derive(serde::Deserialize)]
        struct ListResponse {
            workflows: Vec<WorkflowInfo>,
        }
        let body = Body {
            db: &self.db,
            status,
        };
        let resp = self
            .client
            .post(format!("{}/api/workflows/list", self.url))
            .authed(&self.token)
            .json(&body)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("list workflows request failed: {e}")))?;
        let parsed = crate::http_common::deserialize::<ListResponse>(resp).await?;
        Ok(parsed.workflows)
    }
}

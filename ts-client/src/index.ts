/**
 * `@par-rt-db/client` — TypeScript client for [par-rt-db](https://github.com/paulrobello/par-rt-db).
 *
 * Speaks the server's declarative query/transaction protocol over WebSocket
 * (reactive — `RtDbClient`) and HTTP (one-shot — `RtDbHttpClient`); ships an
 * admin control-plane client (`RtDbAdminClient`), React bindings
 * (`RtDbProvider`, `useQuery`, `useMutation`, `usePresence`, …), a schema
 * builder (`defineSchema`/`defineTable`/`t`) that is both pushed to the server
 * and the source of inferred types, and an in-memory test harness
 * (`InMemoryRtDbClient`) that mirrors server query/transaction/subscription
 * semantics with no network.
 *
 * No codegen: the schema object is both pushed to the server and the source of
 * inferred types. This module is the public entry point — re-exports the
 * surface from the surrounding modules (`./client.js`, `./http.js`,
 * `./admin.js`, `./schema.js`, `./query.js`, `./mutation.js`,
 * `./migration.js`, `./optimistic.js`, `./retry.js`, `./pagination.js`,
 * `./in_memory.js`, `./protocol.js`, `./errors.js`).
 *
 * @example
 * ```typescript
 * import { RtDbClient, createApi, defineSchema, defineTable, t } from "@par-rt-db/client";
 *
 * const schema = defineSchema({
 *   items: defineTable({ title: t.string() }).index("by_title", ["title"]),
 * });
 * const api = createApi(schema);
 * const client = new RtDbClient({ url: "wss://rtdb.pardev.net", db: "kanban" });
 * ```
 */

export const VERSION = "0.1.0";

export type {
  AdminStreamFrame,
  AuditEntry,
  BackupFile,
  BackupsListResponse,
  CreateWebhookOptions,
  DbSubCounters,
  EditWebhookOptions,
  ExplainResult,
  GetAuditOptions,
  ListDeliveriesOptions,
  RestoreResult,
  SlowQueriesResponse,
  SlowQueryEntry,
  RtDbAdminClientOptions,
  SchemaPreviewColumnAdd,
  SchemaPreviewDiff,
  SchemaPreviewIndexAdd,
  SchemaPreviewRejection,
  SchemaPreviewTableAdd,
  SessionInfo,
  SubscriptionInfo,
  SubscriptionsPrincipal,
  SubscriptionsResponse,
  Webhook,
  WebhookDelivery,
} from "./admin.js";
export { RtDbAdminClient } from "./admin.js";
export type { AuthState, ConnectionState, RtDbClientOptions, WebSocketLike } from "./client.js";
export { RtDbClient } from "./client.js";
export type { RtDbErrorCode, RtDbErrorEnvelope } from "./errors.js";
export { RtDbError } from "./errors.js";
export type {
  FileMetadata,
  RtDbHttpClientOptions,
  SignedUrl,
  TransformOpts,
  UploadResult,
} from "./http.js";
export { appendImageParams, RtDbHttpClient } from "./http.js";
export type { InMemoryRtDbClientOptions } from "./in_memory.js";
export { InMemoryAdminClient, InMemoryRtDbClient, PresenceRooms } from "./in_memory.js";
export { Migration } from "./migration.js";
export type { StepInsertResult, StepResult, StepUpsertResult } from "./mutation.js";
export { mutation, parseStepResults, TxnBuilder } from "./mutation.js";
export type { OptimisticProjection } from "./optimistic.js";
export { projectOptimisticUpdate } from "./optimistic.js";
export { decodeCursor, encodeCursor } from "./pagination.js";
export type {
  AuthedUser,
  Cast,
  CastFailureJson,
  CaseWhenJson,
  ClientMessage,
  DirectiveJson,
  DirectiveReportJson,
  FieldTypeJson,
  FilterExpr,
  IndexJson,
  MigrateRequestJson,
  MigrateResultJson,
  Order,
  PresenceMember,
  QueryJson,
  SampleChangeJson,
  ScheduleInfo,
  ScheduleWhen,
  SchemaHistoryEntry,
  SchemaHistoryEntrySummary,
  SchemaJson,
  SearchQuery,
  ServerMessage,
  StepJson,
  TableJson,
  TransactionJson,
  TtlDef,
  ValueExprJson,
  VectorIndexSpec,
  VectorQuery,
} from "./protocol.js";
export type { ClientApi, RtQuery, TableApi } from "./query.js";
export { createApi, TableQuery } from "./query.js";
export { retryOnPrecondition } from "./retry.js";
export type {
  Doc,
  DocFields,
  Id,
  IndexNamesOf,
  Infer,
  SystemFields,
  TableNames,
  Validator,
  WithoutSystemFields,
} from "./schema.js";
export { defineSchema, defineTable, SchemaDefinition, TableDefinition, t } from "./schema.js";

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

export { RtDbError } from "./errors.js";
export type { RtDbErrorCode, RtDbErrorEnvelope } from "./errors.js";
export type {
  AuthedUser,
  ClientMessage,
  FilterExpr,
  FieldTypeJson,
  IndexJson,
  Order,
  PresenceMember,
  QueryJson,
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
  VectorIndexSpec,
  VectorQuery,
} from "./protocol.js";
export { defineSchema, defineTable, SchemaDefinition, TableDefinition, t } from "./schema.js";
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
export { createApi, TableQuery } from "./query.js";
export type { ClientApi, RtQuery, TableApi } from "./query.js";
export { mutation, parseStepResults, TxnBuilder } from "./mutation.js";
export type { StepInsertResult, StepResult, StepUpsertResult } from "./mutation.js";
export { Migration } from "./migration.js";
export type {
  Cast,
  CastFailureJson,
  DirectiveJson,
  DirectiveReportJson,
  MigrateRequestJson,
  MigrateResultJson,
  SampleChangeJson,
} from "./protocol.js";
export { projectOptimisticUpdate } from "./optimistic.js";
export type { OptimisticProjection } from "./optimistic.js";
export { retryOnPrecondition } from "./retry.js";
export { encodeCursor, decodeCursor } from "./pagination.js";
export { RtDbHttpClient, appendImageParams } from "./http.js";
export type {
  RtDbHttpClientOptions,
  UploadResult,
  FileMetadata,
  SignedUrl,
  TransformOpts,
} from "./http.js";
export { RtDbAdminClient } from "./admin.js";
export type {
  AdminStreamFrame,
  AuditEntry,
  BackupFile,
  BackupsListResponse,
  CreateWebhookOptions,
  EditWebhookOptions,
  GetAuditOptions,
  ListDeliveriesOptions,
  RestoreResult,
  RtDbAdminClientOptions,
  Webhook,
  WebhookDelivery,
} from "./admin.js";
export { RtDbClient } from "./client.js";
export type { AuthState, ConnectionState, RtDbClientOptions, WebSocketLike } from "./client.js";
export { InMemoryRtDbClient, PresenceRooms } from "./in_memory.js";
export type { InMemoryRtDbClientOptions } from "./in_memory.js";

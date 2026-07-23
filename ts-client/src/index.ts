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
  QueryJson,
  SchemaJson,
  SearchQuery,
  ServerMessage,
  StepJson,
  TableJson,
  TransactionJson,
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
export { mutation, TxnBuilder } from "./mutation.js";
export { projectOptimisticUpdate } from "./optimistic.js";
export type { OptimisticProjection } from "./optimistic.js";
export { retryOnPrecondition } from "./retry.js";
export { encodeCursor, decodeCursor } from "./pagination.js";
export { RtDbHttpClient } from "./http.js";
export type { RtDbHttpClientOptions } from "./http.js";
export { RtDbAdminClient } from "./admin.js";
export type { RtDbAdminClientOptions } from "./admin.js";
export { RtDbClient } from "./client.js";
export type { AuthState, ConnectionState, RtDbClientOptions, WebSocketLike } from "./client.js";
export { InMemoryRtDbClient } from "./in_memory.js";
export type { InMemoryRtDbClientOptions } from "./in_memory.js";

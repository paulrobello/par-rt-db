import type { RtDbErrorEnvelope } from "./errors.js";

export type Order = "asc" | "desc";

/** Mirrors server `query::Query` (serde `deny_unknown_fields`). */
export interface QueryJson {
  table: string;
  get?: string;
  index?: string;
  eq?: unknown[];
  gt?: unknown;
  gte?: unknown;
  lt?: unknown;
  lte?: unknown;
  order?: Order;
  take?: number;
  unique?: boolean;
  first?: boolean;
  count?: boolean;
}

/** Mirrors server `txn::Step` (tag `op`, every step carries `table`). */
export type StepJson =
  | { op: "insert"; table: string; doc: Record<string, unknown> }
  | { op: "patch"; table: string; id: string; fields: Record<string, unknown> }
  | { op: "replace"; table: string; id: string; doc: Record<string, unknown> }
  | { op: "delete"; table: string; id: string }
  | { op: "expectVersion"; table: string; id: string; version: number }
  | { op: "expectAbsent"; table: string; index: string; eq: unknown[] }
  | {
      op: "upsert";
      table: string;
      index: string;
      eq: unknown[];
      insert: Record<string, unknown>;
      patch: Record<string, unknown>;
    };

export interface TransactionJson {
  steps: StepJson[];
}

export interface AuthedUser {
  kind: string;
  email?: string | null;
  name?: string | null;
}

/** Client -> server WS vocabulary. Tags/fields match server `protocol::ClientMessage`. */
export type ClientMessage =
  | { type: "auth"; token: string; db: string }
  | { type: "subscribe"; queryId: string; query: QueryJson }
  | { type: "unsubscribe"; queryId: string }
  | { type: "mutate"; mutId: string; txn: TransactionJson }
  | { type: "ping" };

/** Server -> client WS vocabulary. Tags/fields match server `protocol::ServerMessage`. */
export type ServerMessage =
  | { type: "authOk"; user: AuthedUser }
  | { type: "authErr"; error: RtDbErrorEnvelope }
  | { type: "queryUpdate"; queryId: string; result: unknown }
  | { type: "mutateOk"; mutId: string; results: unknown[] }
  | { type: "mutateErr"; mutId: string; error: RtDbErrorEnvelope }
  | { type: "subscribeErr"; queryId: string; error: RtDbErrorEnvelope }
  | { type: "pong" };

/** Mirrors server `schema::FieldType` (tag `type`). */
export type FieldTypeJson =
  | { type: "string" }
  | { type: "number" }
  | { type: "boolean" }
  | { type: "null" }
  | { type: "id"; table: string }
  | { type: "literal"; value: string | number | boolean }
  | { type: "optional"; inner: FieldTypeJson }
  | { type: "union"; variants: FieldTypeJson[] }
  | { type: "array"; element: FieldTypeJson }
  | { type: "object"; fields: Record<string, FieldTypeJson> }
  | { type: "int64" }
  | { type: "bytes" }
  | { type: "any" }
  | { type: "record"; value: FieldTypeJson };

export interface IndexJson {
  name: string;
  fields: string[];
}

export interface TableJson {
  fields: Record<string, FieldTypeJson>;
  indexes?: IndexJson[];
}

export interface SchemaJson {
  tables: Record<string, TableJson>;
}

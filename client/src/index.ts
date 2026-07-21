export const VERSION = "0.1.0";

export { RtDbError } from "./errors.js";
export type { RtDbErrorCode, RtDbErrorEnvelope } from "./errors.js";
export type {
  AuthedUser,
  ClientMessage,
  FieldTypeJson,
  IndexJson,
  Order,
  QueryJson,
  SchemaJson,
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

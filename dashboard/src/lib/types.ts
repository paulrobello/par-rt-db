// Admin API data contract — mirrors server/src/admin.rs, op_feed.rs, metrics.rs.
// Keep field names in sync with the server's serde output (camelCase).

import type { AuthedUser, QueryJson, TransactionJson } from "@par-rt-db/client";

export type { AuthedUser, QueryJson, TransactionJson };

export type OpKind = "insert" | "patch" | "replace" | "delete";

export interface OpEvent {
  db: string;
  table: string;
  docId: string;
  kind: OpKind;
  ts: number;
  owner?: string | null;
}

export interface MetricsSnapshot {
  queriesTotal: number;
  mutationsTotal: number;
  uploadsTotal: number;
  wsConnections: number;
  activeSubscriptions: number;
  poolSize: number;
  poolIdle: number;
  uptimeSeconds: number;
}

export interface TableStat {
  name: string;
  rowCount: number;
  sizeBytes: number;
}

export interface DbStats {
  tables: TableStat[];
  totalSizeBytes: number;
}

export interface AdminMember {
  email: string;
  githubId?: number;
}

export interface HotConfig {
  allowedOrigins: string[];
  sessionTtlDays: number;
  maxFileSize: number;
  idempotencyTtlMs: number;
}

export interface ConfigResponse {
  port: number;
  publicUrl: string;
  githubBaseUrl: string;
  githubApiUrl: string;
  databaseUrlConfigured: boolean;
  adminKeyConfigured: boolean;
  githubConfigured: boolean;
  googleConfigured: boolean;
  hot: HotConfig;
  version: string;
  gitCommit: string;
  admins: AdminMember[];
}

export interface TokenRow {
  id: string;
  name: string;
  createdAt: number;
  revoked: boolean;
}

export interface RtDbErrorEnvelope {
  code: string;
  message: string;
}

export interface AdminQueryResult {
  result: unknown;
}
export interface AdminMutateResult {
  results: unknown[];
}

export interface HotConfigPatch {
  allowedOrigins?: string[];
  sessionTtlDays?: number;
  maxFileSize?: number;
  idempotencyTtlMs?: number;
}

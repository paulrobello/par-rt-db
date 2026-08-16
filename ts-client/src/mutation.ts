import { RtDbError } from "./errors.js";
import type {
  FilterExpr,
  ScheduleWhen,
  StepJson,
  TransactionJson,
  WorkflowSpec,
} from "./protocol.js";
import type { IndexNamesOf, SchemaDefinition, TableNames, WithoutSystemFields } from "./schema.js";

/**
 * One entry of a mutation's `results`, positionally aligned with `steps`.
 *
 * Mirrors `rust-client::StepResult` and `python-client.StepResult`: untagged,
 * with the shapes the server may emit. The richer upsert shape
 * (`{ id, inserted }`) is a separate variant from the plain `{ id }` so callers
 * can narrow with `'inserted' in r`; the by-query shapes (`{ patched }`/
 * `{ deleted }`, each with `truncated`) narrow via those own fields; the
 * scheduler shapes (`{ scheduleId }`/`{ cancelled }`, FM-28) narrow via their
 * own fields, as do the workflow shapes (`{ workflowId }`/`{ cancelled }`,
 * FM-29); `null` covers `delete` of an absent doc and any other no-op.
 * Variant order matters for the rust/python decoders (richest first); here it
 * is just documentation since TS narrows structurally.
 */
export type StepResult =
  | StepUpsertResult
  | StepInsertResult
  | StepPatchByQueryResult
  | StepDeleteByQueryResult
  | StepScheduleResult
  | StepCancelScheduleResult
  | StepStartWorkflowResult
  | null;

/** Result of an `upsert` step: the doc id and whether the insert branch ran. */
export interface StepUpsertResult {
  id: string;
  inserted: boolean;
}

/** Result of an `insert`/`patch`/`replace`/`delete`(of a present doc) step. */
export interface StepInsertResult {
  id: string;
}

/** Result of a `patchByQuery` step: rows patched and whether the match set
 * exceeded `limit` (server cap 1000). */
export interface StepPatchByQueryResult {
  patched: number;
  truncated: boolean;
}

/** Result of a `deleteByQuery` step: rows deleted and whether the match set
 * exceeded `limit` (server cap 1000). */
export interface StepDeleteByQueryResult {
  deleted: number;
  truncated: boolean;
}

/** Result of a `schedule` step: the id of the created scheduled job. */
export interface StepScheduleResult {
  scheduleId: string;
}

/** Result of a `cancelSchedule` step: whether a pending job was cancelled.
 * The `cancelWorkflow` step (FM-29) emits the same `{ cancelled }` shape. */
export interface StepCancelScheduleResult {
  cancelled: boolean;
}

/** Result of a `startWorkflow` step (FM-29): the id of the created run. */
export interface StepStartWorkflowResult {
  workflowId: string;
}

/**
 * Decodes one `mutateOk.results` entry (raw server JSON) into a {@link StepResult}.
 *
 * The server emits these shapes per the contract: `{ id, inserted }` (upsert),
 * `{ id }` (insert/patch/replace/delete of a present doc), `{ patched, truncated }`
 * (patchByQuery), `{ deleted, truncated }` (deleteByQuery), `{ scheduleId }`
 * (schedule), `{ cancelled }` (cancelSchedule, and cancelWorkflow FM-29), or
 * `null` (delete of an absent doc / no-op). Anything else is a server contract
 * violation and is surfaced as an `RtDbError` rather than silently passed
 * through — mirroring the rust/python clients' strict untagged decoding.
 */
function parseStepResult(value: unknown): StepResult {
  if (value === null) {
    return null;
  }
  if (typeof value === "object" && value !== null) {
    const v = value as {
      id?: unknown;
      inserted?: unknown;
      patched?: unknown;
      deleted?: unknown;
      truncated?: unknown;
      scheduleId?: unknown;
      cancelled?: unknown;
      workflowId?: unknown;
    };
    if (typeof v.id === "string") {
      if (typeof v.inserted === "boolean") {
        return { id: v.id, inserted: v.inserted };
      }
      return { id: v.id };
    }
    if (typeof v.patched === "number" && typeof v.truncated === "boolean") {
      return { patched: v.patched, truncated: v.truncated };
    }
    if (typeof v.deleted === "number" && typeof v.truncated === "boolean") {
      return { deleted: v.deleted, truncated: v.truncated };
    }
    if (typeof v.scheduleId === "string") {
      return { scheduleId: v.scheduleId };
    }
    if (typeof v.workflowId === "string") {
      return { workflowId: v.workflowId };
    }
    if (typeof v.cancelled === "boolean") {
      return { cancelled: v.cancelled };
    }
  }
  throw new RtDbError(
    "INTERNAL",
    "malformed step result: expected {id}, {id, inserted}, {patched, truncated}, {deleted, truncated}, {scheduleId}, {workflowId}, {cancelled}, or null",
  );
}

/**
 * Decodes a `mutateOk.results` array (raw server JSON) into `StepResult[]`,
 * positionally aligned with the submitted transaction's steps. Throws
 * `RtDbError` on a shape the server contract does not permit.
 */
export function parseStepResults(results: unknown[]): StepResult[] {
  return results.map(parseStepResult);
}

/**
 * Chainable builder for an atomic multi-step transaction. `S` is a phantom
 * schema type used only to type-check table/field names — never read at
 * runtime, the same pattern `RtQuery<Result>` uses for its result type.
 */
export class TxnBuilder<S extends SchemaDefinition<any> = SchemaDefinition<any>> {
  private readonly steps: StepJson[] = [];

  insert<T extends TableNames<S>>(table: T, doc: WithoutSystemFields<S, T>): this {
    this.steps.push({ op: "insert", table, doc });
    return this;
  }

  patch<T extends TableNames<S>>(
    table: T,
    id: string,
    fields: Partial<WithoutSystemFields<S, T>>,
  ): this {
    this.steps.push({ op: "patch", table, id, fields });
    return this;
  }

  replace<T extends TableNames<S>>(table: T, id: string, doc: WithoutSystemFields<S, T>): this {
    this.steps.push({ op: "replace", table, id, doc });
    return this;
  }

  delete<T extends TableNames<S>>(table: T, id: string): this {
    this.steps.push({ op: "delete", table, id });
    return this;
  }

  expectVersion<T extends TableNames<S>>(table: T, id: string, version: number): this {
    this.steps.push({ op: "expectVersion", table, id, version });
    return this;
  }

  expectAbsent<T extends TableNames<S>>(table: T, index: IndexNamesOf<S, T>, eq: unknown[]): this {
    this.steps.push({ op: "expectAbsent", table, index, eq });
    return this;
  }

  upsert<T extends TableNames<S>>(
    table: T,
    args: {
      index: IndexNamesOf<S, T>;
      eq: unknown[];
      insert: WithoutSystemFields<S, T>;
      patch: Partial<WithoutSystemFields<S, T>>;
    },
  ): this {
    this.steps.push({ op: "upsert", table, ...args });
    return this;
  }

  patchByQuery<T extends TableNames<S>>(
    table: T,
    filter: FilterExpr,
    patch: Partial<WithoutSystemFields<S, T>>,
    limit?: number,
  ): this {
    this.steps.push({
      op: "patchByQuery",
      table,
      filter,
      patch,
      ...(limit !== undefined && { limit }),
    });
    return this;
  }

  deleteByQuery<T extends TableNames<S>>(table: T, filter: FilterExpr, limit?: number): this {
    this.steps.push({ op: "deleteByQuery", table, filter, ...(limit !== undefined && { limit }) });
    return this;
  }

  /** Schedules `txn` to run later (FM-28). The inner transaction is executed
   * by the server's per-db scheduler, not in this transaction's turn. */
  schedule(when: ScheduleWhen, txn: TransactionJson): this {
    this.steps.push({ op: "schedule", when, txn });
    return this;
  }

  /** Cancels a pending scheduled job by id (FM-28). */
  cancelSchedule(id: string): this {
    this.steps.push({ op: "cancelSchedule", id });
    return this;
  }

  /** Starts a durable workflow run from `spec` (FM-29). The run advances on
   * the server's per-db worker, not in this transaction's turn. */
  startWorkflow(spec: WorkflowSpec): this {
    this.steps.push({ op: "startWorkflow", spec });
    return this;
  }

  /** Cancels a pending/running workflow by id (FM-29). */
  cancelWorkflow(id: string): this {
    this.steps.push({ op: "cancelWorkflow", id });
    return this;
  }

  build(): TransactionJson {
    return { steps: [...this.steps] };
  }
}

export function mutation(): TxnBuilder<SchemaDefinition<any>>;
export function mutation<S extends SchemaDefinition<any>>(schema: S): TxnBuilder<S>;
export function mutation<S extends SchemaDefinition<any>>(_schema?: S): TxnBuilder<S> {
  return new TxnBuilder<S>();
}

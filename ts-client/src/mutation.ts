import { RtDbError } from "./errors.js";
import type { StepJson, TransactionJson } from "./protocol.js";
import type { IndexNamesOf, SchemaDefinition, TableNames, WithoutSystemFields } from "./schema.js";

/**
 * One entry of a mutation's `results`, positionally aligned with `steps`.
 *
 * Mirrors `rust-client::StepResult` and `python-client.StepResult`: untagged,
 * with three shapes the server may emit. The richer upsert shape
 * (`{ id, inserted }`) is a separate variant from the plain `{ id }` so callers
 * can narrow with `'inserted' in r`; `null` covers `delete` of an absent doc
 * and any other no-op. Variant order matters for the rust/python decoders
 * (richest first); here it is just documentation since TS narrows structurally.
 */
export type StepResult = StepUpsertResult | StepInsertResult | null;

/** Result of an `upsert` step: the doc id and whether the insert branch ran. */
export interface StepUpsertResult {
  id: string;
  inserted: boolean;
}

/** Result of an `insert`/`patch`/`replace`/`delete`(of a present doc) step. */
export interface StepInsertResult {
  id: string;
}

/**
 * Decodes one `mutateOk.results` entry (raw server JSON) into a {@link StepResult}.
 *
 * The server emits exactly three shapes per the contract: `{ id, inserted }`
 * (upsert), `{ id }` (insert/patch/replace/delete of a present doc), or `null`
 * (delete of an absent doc / no-op). Anything else is a server contract
 * violation and is surfaced as an `RtDbError` rather than silently passed
 * through — mirroring the rust/python clients' strict untagged decoding.
 */
function parseStepResult(value: unknown): StepResult {
  if (value === null) {
    return null;
  }
  if (typeof value === "object" && value !== null) {
    const v = value as { id?: unknown; inserted?: unknown };
    if (typeof v.id === "string") {
      if (typeof v.inserted === "boolean") {
        return { id: v.id, inserted: v.inserted };
      }
      return { id: v.id };
    }
  }
  throw new RtDbError("INTERNAL", "malformed step result: expected {id}, {id, inserted}, or null");
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

  build(): TransactionJson {
    return { steps: [...this.steps] };
  }
}

export function mutation(): TxnBuilder<SchemaDefinition<any>>;
export function mutation<S extends SchemaDefinition<any>>(schema: S): TxnBuilder<S>;
export function mutation<S extends SchemaDefinition<any>>(_schema?: S): TxnBuilder<S> {
  return new TxnBuilder<S>();
}

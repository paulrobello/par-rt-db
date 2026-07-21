import type { StepJson, TransactionJson } from "./protocol.js";

/** Chainable builder for an atomic multi-step transaction. */
export class TxnBuilder {
  private readonly steps: StepJson[] = [];

  insert(table: string, doc: Record<string, unknown>): this {
    this.steps.push({ op: "insert", table, doc });
    return this;
  }

  patch(table: string, id: string, fields: Record<string, unknown>): this {
    this.steps.push({ op: "patch", table, id, fields });
    return this;
  }

  delete(table: string, id: string): this {
    this.steps.push({ op: "delete", table, id });
    return this;
  }

  expectVersion(table: string, id: string, version: number): this {
    this.steps.push({ op: "expectVersion", table, id, version });
    return this;
  }

  expectAbsent(table: string, index: string, eq: unknown[]): this {
    this.steps.push({ op: "expectAbsent", table, index, eq });
    return this;
  }

  upsert(
    table: string,
    args: {
      index: string;
      eq: unknown[];
      insert: Record<string, unknown>;
      patch: Record<string, unknown>;
    },
  ): this {
    this.steps.push({ op: "upsert", table, ...args });
    return this;
  }

  build(): TransactionJson {
    return { steps: [...this.steps] };
  }
}

export function mutation(): TxnBuilder {
  return new TxnBuilder();
}

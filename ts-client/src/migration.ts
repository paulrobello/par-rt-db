import type {
  Cast,
  DirectiveJson,
  FieldTypeJson,
  FilterExpr,
  MigrateRequestJson,
  ValueExprJson,
} from "./protocol.js";

/**
 * Chainable builder for a declarative schema migration. Mirrors `TxnBuilder`:
 * a mutable accumulator whose methods return `this`, and `.build()` emits the
 * wire JSON ({@link MigrateRequestJson}) consumed by `POST /admin/db/{db}/migrate`.
 *
 * The wire shape is the authoritative server contract (`server/src/migrate.rs`):
 * directives are a discriminated union tagged on `op` with camelCase fields,
 * `evalExpr.where` aliases the server's `where_clause`, and `Cast` is one of
 * `"toString" | "toNumber" | "toInt64" | "toBoolean"`. `evalExpr` is
 * dual-accept (ENH-020): `expr` is either a typed `ValueExprJson` (the safe
 * path — use {@link Migration.evalExprTyped}) or a legacy raw-SQL string
 * (deprecated — use {@link Migration.evalExpr}); `where` follows the same
 * discipline. The two sources may not mix. Optional fields
 * (`changeType.default`, `evalExpr.where`) are omitted on the wire when unset,
 * matching the SDK's omit-when-default convention.
 */
export class Migration {
  private readonly directives: DirectiveJson[] = [];
  private dry = false;

  /** Renames a declared field on `table`, preserving its data. */
  renameField(table: string, from: string, to: string): this {
    this.directives.push({ op: "renameField", table, from, to });
    return this;
  }

  /** Renames a table, preserving its data and indexes. */
  renameTable(from: string, to: string): this {
    this.directives.push({ op: "renameTable", from, to });
    return this;
  }

  /** Changes `field`'s declared type to `to`, coercing existing values with
   * `cast`. `def` substitutes for values that fail to coerce; without it, a
   * single bad value rolls the whole migration back atomically. */
  changeType(table: string, field: string, to: FieldTypeJson, cast: Cast, def?: unknown): this {
    this.directives.push({
      op: "changeType",
      table,
      field,
      to,
      cast,
      ...(def !== undefined ? { default: def } : {}),
    });
    return this;
  }

  /** Drops a declared field from `table`, discarding its data. */
  dropField(table: string, field: string): this {
    this.directives.push({ op: "dropField", table, field });
    return this;
  }

  /** Drops a table and all of its data. */
  dropTable(name: string): this {
    this.directives.push({ op: "dropTable", name });
    return this;
  }

  /** Drops a declared index by name, leaving the underlying field data
   * untouched. */
  dropIndex(table: string, name: string): this {
    this.directives.push({ op: "dropIndex", table, name });
    return this;
  }

  /** Backfills `value` into `field` on every existing row that is missing
   * it. Does not change the schema's declared default for new rows — pair
   * with `.defaults()` on the schema for that. */
  setDefault(table: string, field: string, value: unknown): this {
    this.directives.push({ op: "setDefault", table, field, value });
    return this;
  }

  /** Legacy raw-SQL `evalExpr` (deprecated, ENH-020 / SEC-107): `expr` and
   * `where` are raw SQL strings, gated to the root admin_key on the server.
   * Prefer {@link Migration.evalExprTyped} for new code — the typed grammar is
   * injection-safe by construction. The two sources may not mix. */
  evalExpr(table: string, set: string, expr: string, where?: string): this {
    this.directives.push({
      op: "evalExpr",
      table,
      set,
      expr,
      ...(where !== undefined ? { where } : {}),
    });
    return this;
  }

  /** Typed `evalExpr` (ENH-020, the SEC-107-safe path): `expr` is a closed
   * {@link ValueExprJson} grammar and `where` is a typed {@link FilterExpr}.
   * The two sources may not mix — pass both typed or both legacy (use
   * {@link Migration.evalExpr} for the legacy raw-SQL form). */
  evalExprTyped(table: string, set: string, expr: ValueExprJson, where?: FilterExpr): this {
    this.directives.push({
      op: "evalExpr",
      table,
      set,
      expr,
      ...(where !== undefined ? { where } : {}),
    });
    return this;
  }

  /** Mark this migration as a dry run: the server validates and reports
   * `affectedRows` (and returns the derived `schema`) but commits nothing —
   * `MigrateResultJson.applied` comes back `false`. */
  dryRun(): this {
    this.dry = true;
    return this;
  }

  /** Emits the wire request body consumed by `POST /admin/db/{db}/migrate`. */
  build(): MigrateRequestJson {
    return { directives: [...this.directives], dryRun: this.dry };
  }
}

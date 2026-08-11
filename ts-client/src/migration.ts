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

  renameField(table: string, from: string, to: string): this {
    this.directives.push({ op: "renameField", table, from, to });
    return this;
  }

  renameTable(from: string, to: string): this {
    this.directives.push({ op: "renameTable", from, to });
    return this;
  }

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

  dropField(table: string, field: string): this {
    this.directives.push({ op: "dropField", table, field });
    return this;
  }

  dropTable(name: string): this {
    this.directives.push({ op: "dropTable", name });
    return this;
  }

  dropIndex(table: string, name: string): this {
    this.directives.push({ op: "dropIndex", table, name });
    return this;
  }

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

  build(): MigrateRequestJson {
    return { directives: [...this.directives], dryRun: this.dry };
  }
}

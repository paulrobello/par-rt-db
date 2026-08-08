# Biome Lint Policy

## `noExplicitAny: off` (QA-004)

`biome.json` disables Biome's `suspicious/noExplicitAny` rule across the whole
package. This is a deliberate policy decision, not a relaxation for convenience.

### Rationale

The dashboard consumes the par-rt-db **JSON wire contract**: server protocol
types, live query results, op-feed `Docs`/`Values`, and the dynamic JSON the
admin API returns. Explicit `any` accurately describes JSON the runtime
validates structurally.

Tightening would force artificial narrowing that obscures the wire shape, and
new protocol fields would otherwise churn types that legitimately should accept
any JSON value. The dashboard mirrors the `ts-client`'s policy for the same
reason — both packages reason about the same wire types.

### Why not scope to JSON-shaped files?

The `any` usages are spread across route handlers, op-feed consumers, and the
live-data browser pages. Scoping via `overrides` to a subset would leave the
rule half-on/half-off in a way that's harder to reason about than the simple
"off repo-wide, document the reason here" policy.

### Where the rule still applies

All other `suspicious` rules remain on (Biome `recommended` profile). Adding a
new `any` should still be questioned in code review — it's just not a lint
failure.

### Why no inline comment in biome.json?

Biome v2.5.4's `biome.json` parser does not accept `//` or `/* */` comments
even though the docs advertise JSONC support; this sibling file is the
documented home for the policy rationale.

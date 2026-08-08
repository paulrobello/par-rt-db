# Biome Lint Policy

## `noExplicitAny: off` (QA-004)

`biome.json` disables Biome's `suspicious/noExplicitAny` rule across the whole
package. This is a deliberate policy decision, not a relaxation for convenience.

### Rationale

The `@par-rt-db/client` SDK models the par-rt-db **JSON wire contract**: tag
unions, `serde_json::Value` shapes, and the TypeScript mirror of
`server/src/protocol.rs`. The explicit `any` usages (≈17 today) describe JSON
values that the runtime validates structurally and that would lose flexibility
under a tighter static type.

Concrete examples:

- `JSONSchemaValue` / wire-shape types where the value is genuinely any JSON.
- Builder type parameters (`SchemaDefinition<any>`) where the constraint is on
  shape, not on a specific inferred type.
- Result types where the server returns arbitrary JSON validated at the
  protocol layer, not at the type layer.

Tightening would force artificial narrowing that obscures the wire shape, and
adding a new protocol field would otherwise churn unrelated types.

### Why not scope to JSON-shaped files?

The `any` usages are spread across `protocol.ts`, `mutation.ts`, `query.ts`,
`schema.ts`, `in_memory.ts`, and `client.ts`. Scoping via `overrides` to a
subset would leave the rule half-on/half-off in a way that's harder to reason
about than the simple "off repo-wide, document the reason here" policy.

### Where the rule still applies

All other `suspicious` rules remain on (Biome `recommended` profile). Adding a
new `any` should still be questioned in code review — it's just not a lint
failure.

### Why no inline comment in biome.json?

Biome v2.5.4's `biome.json` parser does not accept `//` or `/* */` comments
even though the docs advertise JSONC support; this sibling file is the
documented home for the policy rationale.

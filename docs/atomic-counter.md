# Atomic bounded counters

`adjustCounter` updates a counter inside the same transaction as related
document writes. It removes the client read/version race when independent
writers reserve or release queue slots and retained bytes.

```json
{
  "op": "adjustCounter",
  "table": "storageUsage",
  "id": "existing-document-id",
  "field": "usedBytes",
  "delta": 128,
  "min": 0,
  "max": 1073741824,
  "expected": { "key": "retained-data", "initialized": true, "limitBytes": 1073741824 }
}
```

The required fields are `table`, `id`, `field`, and `delta`. `min`, `max`, and
the `expected` field-value map are optional. The operation addresses an existing
document using the same ID and alias rules as `patch`. Its result has the same
shape as a normal patch result.

- The current counter, delta, bounds, and result must be safe integers in
  `[-9007199254740991, 9007199254740991]`. Invalid numeric inputs or inverted
  bounds are `BAD_REQUEST`.
- `field` names a declared, writable, numeric field. Normal row authorization,
  schema validation, immutable/auto-increment field restrictions, computed-field
  stamping, write-set tracking, and subscription behavior are preserved.
- Every `expected` entry names a declared field that must exist and equal its
  supplied JSON value. Object-key order is irrelevant. Missing does not equal
  JSON null. A mismatch is `PRECONDITION_FAILED`.
- `min` and `max` are inclusive bounds on the resulting counter. A failed bound
  is `PRECONDITION_FAILED`. The entire surrounding transaction rolls back,
  including earlier inserts, other counters, and scheduled work.
- Negative deltas release capacity. An application that permits gradual cleanup
  of already over-limit data omits the upper bound for a negative delta while
  retaining the lower bound of zero. Stable configuration values, such as the
  limit and initialization flag, belong in `expected` rather than an exact
  document-version guard.

The server and every supported in-memory client share these semantics. Shared
corpus cases cover increment/decrement, bounds, expected fields, safe integers,
and transaction rollback. Live database tests cover concurrent admission and
release. This operation does not bypass authorization or increase a configured
capacity limit.

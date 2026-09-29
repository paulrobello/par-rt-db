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
live document by its string `id`, as `patch` does, so a missing or soft-deleted
document is `NOT_FOUND`. Its result has the same shape as a normal patch result
(`null`).

- The current counter, delta, bounds, and result must be safe integers in
  `[-9007199254740991, 9007199254740991]`. Invalid numeric inputs or inverted
  bounds are `BAD_REQUEST`.
- `field` names a declared `number` or `optional(number)` field that is already
  present on the document with an integer value. An undeclared or non-numeric
  field is `SCHEMA_VIOLATION`. A missing counter value, a computed field, the
  `autoIncrementField`, or the `updatedAtField` is `BAD_REQUEST`. Normal row
  authorization, schema validation, computed-field stamping, write-set
  tracking, and subscription behavior are preserved.
- Every `expected` entry names a declared field (an undeclared name is
  `SCHEMA_VIOLATION`) that must exist and equal its supplied JSON value. Object-key order is irrelevant. Missing does not equal
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
and computed-field rejection. Transaction rollback is covered by each client's
in-memory tests and by the server's live database test, which also covers
concurrent admission. This operation does not bypass authorization or increase a configured
capacity limit.

# Read-only row-field transaction guards

`expectFields(table, id, expected)` requires selected values on one visible,
live document to match exactly. It returns `null` and does not patch the row,
increment its version, stamp `updatedAt`, or produce document operations,
subscription invalidations, audit entries or webhook deliveries.

Use it when unrelated fields may change while a transaction still requires a
stable identity or state. Use `expectVersion` when every intervening row update
must reject the transaction. These guards run inside the same serialized
committer transaction as the following writes.

```json
{
  "op": "expectFields",
  "table": "rooms",
  "id": "physical-document-id",
  "expected": [
    { "path": ["ownerGeneration"], "value": 7 },
    { "path": ["journal", "scope"], "value": { "backend": "wasm", "version": 1 } }
  ]
}
```

Paths contain literal object keys. A key containing a dot is one component,
not dotted-path syntax. Array indexing is unsupported. The root field must be
declared. Further traversal follows `object`, `record`, `optional` and `union`
schema declarations, or object keys within `any`. A union permits a path that
at least one branch declares. The actual document must still contain it.

Each selected object or array compares in full, including its keys and length.
Values are not recursive subsets. Numeric integer/float spellings compare by
value (`1` equals `1.0`), while booleans remain distinct from numbers. Strings
and object keys compare by UTF-8 bytes without Unicode normalization.

Missing fields never match, including against an explicit `null`. The assertion's
`value` key is required and is retained when null. Existing write normalization
still applies: an `optional(object)` written as null becomes absent, whereas
`optional(any)`, whose inner type accepts null, can retain it. This guard does
not change storage normalization or the SDKs' existing JSON value types.

## Bounds and errors

Bounds apply per guard step across all its assertions:

| Bound | Limit |
| --- | --- |
| Assertions | 1–64, with distinct paths |
| Path components | 1–16 per path |
| Component size | 1–256 decoded UTF-8 bytes |
| Selected-value nesting depth | 0–32, with the value itself at depth zero |
| JSON nodes | 8,192 total, counting containers and scalars |
| String bytes | 65,536 total across values, object keys and path components |

Existing transaction step, request-body and affected-row limits still apply.
Malformed wire shapes and exceeded bounds return `BAD_REQUEST`. Undeclared
paths return `SCHEMA_VIOLATION`. A well-formed assertion that does not match
returns `PRECONDITION_FAILED` and rolls back all preceding transaction writes.

Table scope is checked before the step executes. Row visibility matches
`expectVersion`, including owners, collaborators and `authorize` predicates.
Missing, soft-deleted and invisible rows return the same `NOT_FOUND` error.
Semantic path/budget validation follows visibility, so it cannot expose hidden
row values. No new permission or authentication configuration is required.

## Ordering, deadlines and retries

An assertion sees earlier writes in the same transaction. Later steps may
intentionally change asserted fields. The server does not recheck equality
against the transaction's own later writes.

An equality assertion does not impose a time deadline. Combine it with
[`expectBefore(deadlineMs)`](transaction-deadlines.md) when authority expires.
The existing deadline checks apply after queue wait and at the final commit
decision, including asynchronous SQL work.

Existing idempotency semantics remain unchanged: reusing a key replays its
cached successful result instead of executing the transaction again. The server
does not fingerprint transaction bodies. A caller must use the same key only
for the same operation, and still validate current authority before subsequent
operations. A guard does not hold authority across separate requests.

Normal transaction accounting, read-only freeze behavior and idempotency
metadata still apply to a transaction containing only guards. The step itself
produces no document writes.

## SDK surface

| SDK | Builder method | Assertion type |
| --- | --- | --- |
| TypeScript | `.expectFields(table, id, expected)` | `FieldExpectationJson` |
| Rust | `.expect_fields(table, id, &expected)` | `FieldExpectation` |
| Python | `.expect_fields(table, id, expected)` | `FieldExpectation` |
| Go | `.ExpectFields(table, id, expected...)` | `wire.FieldExpectation` or root alias |
| Swift | `.expectFields(table, id, expected)` | `FieldExpectation` |

The wire shape and in-memory implementations match the server contract.
Shared semantics fixtures cover bounds, typed/nested paths, exact values,
null/missing distinctions, Unicode, ordered writes and deadline composition.
Real PostgreSQL tests also hold writes in the committer queue to verify stable
tail acceptance, authority/base rejection and rollback after deadline loss.

# Host-scoped machine authorization and enrollment

## Intent and approved boundary

Implement card `01a16218bfd177fe8b3a4075531b557d` for par-mesh. One runtime credential represents one authenticated host. Agents on the same host intentionally trust each other. No per-agent keys or shared HMAC are introduced. Legacy machine credentials and user authorization retain their current semantics. Existing credentials are never replaced, rotated, or retired automatically. Deployment enables the capability, not downstream cutover.

The user requested implementation, push, deployment, and a completion notice to pardeck. Security changes must be presented for manual review before committing product code.

## Design decisions proposed for approval

### Credential capability

Add an optional `hostCapability` to machine-token minting and listing. Its versioned discriminated shape is `{"type":"parMeshV1","host":"macdaddy"}`. The database binding remains the existing token database. This is an explicit, narrow application capability, not arbitrary client-supplied authorization code or a generic policy language. The host follows par-mesh's component grammar: lowercase ASCII letters/digits followed by lowercase letters/digits/hyphens/underscores, maximum 32 characters. Unknown capability types and extra capability properties fail validation.

Persist the capability as nullable JSON on the machine-token row. NULL means legacy behavior. Carry it from bearer resolution into `Principal::Machine`, `PrincipalCtx`, forwarded transactions, and stored subscription contexts. A restricted token cannot become admin. `readOnly` and table restrictions compose with the capability and never broaden it.

A `parMeshV1` token can access only `agents` and `messages`. There is no schema DSL change and no hardcoded database name. The capability explicitly fixes the two table/field contracts below. Missing or incompatible schema fields fail closed, never fall back to unrestricted access. A future incompatible policy needs a new capability version.

### Directory

All scoped hosts can read the agents directory, including scans, aggregates, direct-ID queries and subscriptions. Insert and patch are allowed only for the authenticated host. Both the previous row and resulting row must belong to it. The resulting address must parse as `name@host:project` with the host exactly matching both the credential and row `host`, and name/project matching the row's declared values. Reject wildcard agent names. Patch cannot transfer an agent between hosts.

Other agent mutation forms, including replace/delete/upsert/counters/bulk operations/undelete, are rejected for this initial capability. Current par-mesh uses insert and patch for registration, heartbeat, offline status and recovery after TTL deletion. Server-internal TTL cleanup retains its existing system authority.

### Messages

A host may read a message if it is the recipient host (`host`) or the host parsed from `from_addr`. Sender visibility preserves par-mesh's outgoing delivery/acknowledgment waits. Recipient visibility is host-wide, not agent-specific. Invalid or inconsistent routing rows are invisible to restricted readers.

Insert requires:
- A concrete, valid `from_addr` belonging to the authenticated host.
- A concrete, valid `target` whose parsed host equals row `host`.
- A valid `to_addr`, concrete or wildcard, whose host and project match the target and whose concrete name, if supplied, matches the target.
- `state = "queued"`, `delivered_ms = 0`, `acked_ms = 0`, `ack_note = ""`.
- A valid priority (`next-turn` or `interrupt`). Existing schema validation still checks all document field types.

Cross-host sends are explicitly permitted. Routing, content, sender, message identity, trace and creation time become immutable to restricted tokens after insertion.

Only the recipient host may patch a message, with exactly one of these shapes:
- queued to delivered: fields are `state = "delivered"` and a finite positive `delivered_ms`.
- queued or delivered to acked: fields are `state = "acked"`, a finite positive `acked_ms`, and string `ack_note`.

Queued-to-acked is intentional existing behavior. Reject rewriting an already completed receipt, extra fields, arbitrary state transitions, and sender-side forged receipts. `expectFields`, `expectVersion`, and `expectAbsent` enforce the same read visibility, including direct-ID and unique-index probes. They do not confer mutation rights. All message mutation variants other than insert and the constrained patch are denied.

### Enforcement coverage

Compose a server-owned visibility predicate into SQL before pagination, limits, aggregation, distinct, search and counts. Do not fetch unscoped rows and filter aggregate results afterward. Point reads and transaction preconditions use the equivalent visibility check. Subscription initial reads and every rerun use this same policy. Enforce write rules inside the serialized transaction against actual current rows and candidate resulting rows, so a denial rolls back all earlier steps.

Deny schedules, workflow management, presence, blob/storage operations, change feeds and other non-document surfaces to restricted credentials unless explicitly allowed by this design. Reject nested scheduling/workflows before enqueue. The initial capability is restricted to the document query/mutate surfaces and WS auth/subscribe/mutate/unsubscribe lifecycle. Existing legacy/user/system behavior is unchanged.

Reuse the live machine-token revocation/expiry gate for each REST and WS operation. For existing subscriptions, ensure revocation prevents further data pushes as well as subsequent client requests. Forwarded execution must carry and enforce the capability. Mixed-version fleet rollout must not route restricted requests to an old owner that would ignore the new restriction: document an all-replicas-upgraded prerequisite or introduce an explicit version-support rejection at forwarding.

### Enrollment

Add admin-only `POST /admin/enrollment-grants` to create a grant fixed to database and `hostCapability`, with explicit expiry no more than 15 minutes from creation and greater than now. The response returns grant ID and plaintext once. Persist only its SHA-256 digest, fixed capability, database, token name, expiry, token expiry (optional), creation timestamp and redeemed/revoked state. Never log grant or token plaintext.

Add `POST /auth/enroll` accepting only `grant`. Redemption accepts no host, database, capability, admin credential, or scope override. In one SQL transaction, atomically claim one unexpired/unrevoked/unredeemed grant and insert exactly one restricted machine credential using the stored grant scope. Return token ID, token, database, and capability after commit. Concurrent requests yield at most one success. Invalid, expired, revoked, or used grants receive a generic unauthorized response. If the successful response is lost, an admin must issue another grant and revoke the orphaned token using existing token APIs. Do not store a recoverable plaintext token to enable retries.

Provide admin-only grant revocation. Runtime host tokens cannot issue grants or mint credentials. Apply existing anonymous/auth endpoint request limits and request-body protections to redemption.

## Client contract and documentation

Mirror host capability types, mint/list metadata, grant create/revoke, and redemption methods in all five clients. Rust shares core types where appropriate. Preserve backward-compatible defaults and omit absent optional capability fields where existing strict clients require it. Add an executable wire-corpus case for new public wire vocabulary or explicitly document why an HTTP-only admin/auth shape is covered by transport tests instead of the DSL corpus.

Update README auth/API references, FEATURE_MATRIX, architecture auth notes and rollout documentation. State that legacy tokens still bypass row restrictions until the operator explicitly cuts consumers over and retires legacy credentials. No par-mesh schema migration or consumer configuration changes are included in this upstream card.

## Verification and release

Acceptance evidence must cover each card criterion independently:
1. Persisted identity, REST/WS/forward propagation and unchanged user/legacy authorization.
2. Adversarial spoof/read/write/ack and alternate-operation denials plus legitimate directory, cross-host send, outgoing receipt visibility and subscriptions.
3. Host-bound digest-only expiring grants with exactly one concurrent redemption success and no admin rights on the redeemed host.
4. Revocation on REST and already-open WS, subscription push denial, and explicit legacy-bypass rollout documentation.

Tests must include positive controls, whole-transaction rollback, direct-ID and aggregate visibility, raw WS operations, upsert/bulk/counter/replace/delete/undelete and scheduled/workflow bypass attempts. Run scoped tests while iterating, then make checkall and the CI-equivalent parity gate, secret scanning, and an independent security review. Flag the final security diff for manual review before committing it. After approval, commit, rebase/squash merge, push, verify CI, deploy on the x86_64 host with the repository deployment workflow, verify the running revision and health, and only then notify pardeck with the revision and rollout limits.

## Sources

- Card notes and four acceptance criteria, read 2026-10-21.
- server/src/auth/mod.rs, auth/tokens.rs, admin/tokens.rs, query/row_auth.rs, query/terminals.rs, txn/auth.rs, txn/steps.rs, db.rs.
- par-mesh/schema/schema.json, src/address.ts, src/directory.ts, src/messages.ts and src/db.ts inspected by a read-only scout.

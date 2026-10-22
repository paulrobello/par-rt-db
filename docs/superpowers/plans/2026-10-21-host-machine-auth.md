# Host Machine Authorization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add opt-in authenticated host isolation and atomically single-use enrollment without changing legacy credentials.

**Architecture:** Persist a versioned parMeshV1 capability on machine credentials and propagate it through PrincipalCtx. Central policy helpers define read predicates and permitted transaction operations. Enrollment consumes a hashed grant and inserts a restricted credential in one database transaction.

**Tech Stack:** Rust, axum, sqlx, Postgres, existing five client SDKs.

**Spec:** ../specs/2026-10-21-host-machine-auth-design.md

## Global Constraints

- One host credential, same-host agents intentionally trusted, no per-agent keys or shared HMAC.
- Preserve all existing credentials and legacy/user behavior. No consumer cutover.
- Host components have maximum 32 characters and par-mesh's lowercase component grammar.
- Enrollment expiry is explicit, future, and at most 15 minutes.
- No plaintext credential persistence or logging.
- Reject background and unsupported operations for restricted credentials.
- No product-code commit before manual security review. Push, deploy and completion notice are user-authorized for this change.

## Review Focus

- Sender-side outgoing receipt reads must work without sender-side receipt patch rights.
- Invalid stored routing and token capability JSON must fail closed.
- Revocation must stop pushes on already-open subscriptions, not just new operations.
- Forwarding to older owners must never erase restrictions.
- A lost enrollment response must not create a recoverable plaintext credential store.

## Task 1: Credential and enrollment persistence

**Files:** core/src/host_capability.rs (new), core/src/lib.rs, server/src/auth/mod.rs, server/src/auth/tokens.rs, server/src/auth/enrollment.rs (new), server/src/db.rs, server/src/admin/tokens.rs, server/src/admin/mod.rs, server/src/auth/host_policy.rs (new), server/tests/host_auth_test.rs (new), server/tests/main.rs. Locate the auth router and rate-limit attachment through parsight before editing.

**Interfaces:** `HostCapability::ParMeshV1 { host: String }`, serialized with type tag `parMeshV1`; `Principal::Machine.host_capability: Option<HostCapability>`; `PrincipalCtx.host_capability: Option<HostCapability>`. Preserve existing `mint_token` as a legacy wrapper. Add typed restricted minting and enrollment helpers rather than changing legacy callers' semantics.

- [ ] Add failing tests for valid/invalid host capability parsing, nullable legacy defaults, persistence and serde propagation. Assert unknown type/fields and malformed host deny.
- [ ] Run the targeted tests and record the expected failure.
- [ ] Add optional capability storage and bump RTDB_AUTH_BOOTSTRAP_VERSION from 1 to 2. Preserve existing columns and values. Wire principal resolution and context propagation.
- [ ] Add failing REST tests for admin grant creation/revocation, non-admin denial, fixed-scope redemption, expiry, digest-only storage and concurrent redemption. Assert exactly one success and one created token.
- [ ] Implement POST /admin/enrollment-grants, an admin grant-revocation route, and POST /auth/enroll with strict request DTOs, transactional consume-and-mint, generic unauthorized failures and existing request protections.
- [ ] Run targeted tests, then make checkall. Hold product-code commits for manual security review.

## Task 2: Operation-specific enforcement

**Files:** server/src/auth/host_policy.rs, server/src/query/mod.rs, server/src/query/row_auth.rs, server/src/query/terminals.rs, server/src/txn/mod.rs, server/src/txn/auth.rs, server/src/txn/steps.rs, server/src/http_api/mod.rs, server/src/ws.rs, server/src/subs.rs, server/src/forward.rs, server/tests/host_auth_test.rs. Add route guards at existing shared seams, not independently guessed per-handler rules.

**Interfaces:** pure host/address validation and row-visibility functions; a read-policy compiler accepting table, PrincipalCtx and shared SQL bind accumulator; operation gate accepting the authenticated context and each Step; row-aware insert/patch authorization executed within the active PgConnection transaction. Task 1's capability type is authoritative.

- [ ] Add failing positive controls: directory scans and writes to own agent, cross-host send, sender/recipient reads, queued-to-delivered and queued-to-acked/delivered-to-acked transitions.
- [ ] Add failing adversarial REST tests for sender spoofing, inconsistent addresses, cross-host agent patch, receipt forgery, extra receipt fields, invalid transitions, direct-ID/count/distinct/aggregate/search exposure and precondition side channels.
- [ ] Add denial tests for upsert, replace, delete, undelete, counters, bulk operations, nested schedules/workflows, standalone background-management routes, change feeds, storage and presence. Assert denied transactions have zero committed side effects.
- [ ] Implement central policy enforcement, with SQL visibility before aggregate/pagination limits and current/candidate row checks inside the committer transaction. Preserve legacy and user positive controls.
- [ ] Add raw WS tests covering initial subscriptions, reruns, mutate denial and already-open revocation. Ensure revoked host subscriptions cannot receive later writes.
- [ ] Add forwarded-context security tests and a fail-closed mixed-version strategy. Document any all-replicas-upgraded prerequisite.
- [ ] Run scoped tests then make checkall. Hold product-code commits for manual security review.

## Task 3: Five-client contract and release documentation

**Files:** client admin/auth types and methods discovered through parsight in ts-client, rust-client, python-client, swift-client and go-client; matching tests; README.md; FEATURE_MATRIX.md; docs/ARCHITECTURE.md; deploy/README.md; wire-corpus/README.md and an appropriate executable corpus case where the new public shape fits.

**Interfaces:** optional hostCapability on mint/list options; typed grant creation/revocation DTOs; redemption method accepting only grant and returning token ID/token/database/capability. Preserve legacy API overloads/defaults. Exact endpoint and DTO names from Task 1 must be carried verbatim into each disjoint client brief.

- [ ] After Task 1 freezes DTOs, dispatch disjoint client mirrors in parallel isolated worktrees, based on the verified task SHA. No shared-file ownership and no subagent board changes.
- [ ] Each mirror adds failing serialization/request tests, implements types/methods, and runs package typecheck and tests.
- [ ] Merge verified mirrors sequentially and run the combined make checkall only after all writers finish.
- [ ] Update matrix, API docs and architecture. Document sender-read semantics, host trust, 15-minute grants, response-loss recovery, revocation, unsupported operations, full-fleet upgrade and legacy-token bypass until explicit cutover.
- [ ] Run make checkall and make checkall-ci if present, otherwise the actual CI-equivalent commands. Run secret scans and a fresh independent security review.
- [ ] Present final security diff and test evidence for required manual review before committing.

## Task 4: Authorized rollout and handoff

- [ ] After manual review, commit the verified unit, rebase on latest main and squash merge. Re-run named acceptance checks on the merged tree.
- [ ] Push to origin, observe CI to green and report any failed job faithfully.
- [ ] Deploy using the repository's canonical x86_64-host make deploy workflow, preserving host configuration and the two-CPU builder window.
- [ ] Verify public health plus admin-authenticated running revision, then use a disposable scoped test fixture to verify enrollment and authorized/denied operations without rotating production credentials.
- [ ] Check each card criterion with concrete evidence and close only after all four are met.
- [ ] Notify pardeck over par-mesh with revision, verified deployment and explicit legacy-cutover limitation. No completion claim before these checks.
- [ ] Remove merged worktree/branch and verify worktree cleanup.

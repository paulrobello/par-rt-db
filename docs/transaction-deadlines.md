# Transaction deadlines

Implementation status: full repository verification and actual-source review passed. Not pushed or deployed. No authentication policy, credential or live database configuration changes are included.

`{"op":"expectBefore","deadlineMs":2000}` requires the authoritative clock to be strictly less than an absolute epoch-millisecond deadline. The step returns `null` and touches no document. A missing, negative, fractional, nonnumeric or unsafe-integer deadline fails with `BAD_REQUEST`. Equality or expiry fails with `PRECONDITION_FAILED` and rolls back the whole transaction.

The server validates guards in the existing serialized committer turn, after any queue wait. It checks the earliest deadline before applying effects and again after all asynchronous document, idempotency, audit, webhook and change-feed work, immediately before the database commit decision. That final sample is the deadline's decision boundary. It is not a promise that a disk flush or network response completes before the timestamp. In-memory engines use their injected clock and restore staged effects when the final check fails.

For lease fencing, pass the `leaseExpiresAt` from the same stored room version used by `expectVersion`, ownership/generation and other publication guards. An independently chosen future timestamp proves no lease ownership. This step neither acquires nor renews a lease.

Builders expose `expectBefore(deadlineMs)` in TypeScript and Swift, `expect_before(deadline_ms)` in Rust and Python, and `ExpectBefore(deadlineMs)` in Go. Existing transactions omit the step and retain their behavior. Guarded transactions do not produce client optimistic overlays because a client cannot establish the authoritative clock decision.

A scheduled or workflow child is a separate execution. The parent's deadline constrains enqueueing it, while a deadline stored inside the child constrains its later execution. It is not evaluated against request-receipt or scheduling time. Existing workflow retry limits still apply to a failed child.

An already committed idempotency result may be replayed after the deadline without executing the transaction again. That is historical data, not renewed writer authority. Callers must keep the existing idempotency-key identity contract and use a new identity when changing the transaction or deadline. A failed, rolled-back request stores no successful result and must pass its guards on retry.

## Review and verification scope

The implementation retains the existing committer as the only transaction executor. Its final deadline check is unconditional for new executions, including transactions with an empty write set. An early successful return is reserved for an already committed idempotency result. Guarded transactions preserve existing resource caps and table authorization, and the new step counts toward the step cap while affecting zero documents.

The real PostgreSQL regressions use isolated databases and rollback-on-drop lock holders. They cover an initially expired request, expiry while an insert is blocked in SQL, expiry while queued behind another request, a guarded read-only transaction, historical idempotent replay and owner-version replacement. Client tests cover exact equality, staged rollback without subscription notification, multiple guards, child execution boundaries, malformed values and conservative optimistic presentation. Eleven shared semantics cases and two wire entries retain agreement across all six implementations. Malformed transaction decoding is classified as `BAD_REQUEST` by the corpus adapters instead of becoming an out-of-band harness failure.

The root-owned full `make checkall` run passed against 1,035 frozen inputs, including the six real deadline regressions within 1,013 passing server integration tests, 1,320 TypeScript client tests, 127 dashboard tests, 1,444 Python tests, every Go package and 559 Swift tests. All Rust workspace tests, client feature combinations, documentation, format, lint and type checks passed. Existing opt-in live tests remained disabled. This repository has no separate `checkall-ci` target.

The run exited zero after 217,213 ms with host load 2.3594 before and 4.6636 after. No frozen input changed. Manifest SHA-256: `926dc045e1a30882e3b79e95a99c6a20574c4c6367d92106809403a14e71616e`. This acceptance text is the only post-gate source change. Application lease binding, actor recovery and production deployment remain separate integration requirements.

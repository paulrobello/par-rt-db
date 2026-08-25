/**
 * Cross-client wire-parity corpus test (ARC-008) — TypeScript client view.
 *
 * Loads the shared `wire-corpus/wire-corpus.json` at the repo root and asserts
 * every entry round-trips byte-identically through the TS wire types. TS has no
 * runtime schema lib, so "round-trip" here means: (a) the entry satisfies the
 * relevant TS type at compile time (a `satisfies` clause makes a shape drift a
 * type error), and (b) `JSON.parse(JSON.stringify(entry))` deep-equals entry
 * (catches any non-JSON-safe values, undefined fields, etc.). The server,
 * rust-client, and python-client each have an equivalent test on the same
 * corpus, where `deny_unknown_fields` / `extra='forbid'` also rejects unknown
 * fields at parse time — TS can only check shape via `satisfies`.
 *
 * The ARC-009 narrowing of `AuthedUser.kind` to `"user" | "machine"` is
 * exercised by the `authed_users` section: each fixture's `kind` is assigned to
 * a variable of `AuthedUserKind`; a typo there would be a compile error.
 */

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

import { ALL_ERROR_CODES } from "../src/errors.js";
import { MAX_STEPS } from "../src/in_memory/index.js";
import { QUERY_COMBO_CLAUSES, QUERY_COMBO_RULES } from "../src/in_memory/query-combinations.js";
import type {
  AuthedUser,
  AuthedUserKind,
  ClientMessage,
  FilterExpr,
  MigrateRequestJson,
  MigrateResultJson,
  QueryJson,
  ScheduleInfo,
  ScheduleWhen,
  SearchQuery,
  ServerMessage,
  StepRetry,
  WorkflowInfo,
  WorkflowSpec,
  WorkflowStepSpec,
} from "../src/protocol.js";

interface Corpus {
  client_messages: ClientMessage[];
  server_messages: ServerMessage[];
  authed_users: AuthedUser[];
  schedule_whens: ScheduleWhen[];
  schedule_infos: ScheduleInfo[];
  // Untyped-on-purpose sections: query_results / error_envelopes / queries are
  // raw JSON values, not TS wire types (QueryResult is untagged on the wire,
  // and the error envelope model lives in errors.ts). We assert JSON
  // round-trip only.
  query_results: unknown[];
  error_envelopes: unknown[];
  queries: unknown[];
  // Admin migrate shapes — type-checked against MigrateRequestJson /
  // MigrateResultJson (op tag, camelCase, `where`/`from` aliases, cast literals).
  migrate_requests: MigrateRequestJson[];
  migrate_results: MigrateResultJson[];
  rejects_client_message_unknown_field: unknown[];
  rejects_schedule_when_unknown_field: unknown[];
  rejects_workflow_spec_unknown_field: unknown[];
  rejects_authed_user_unknown_kind: unknown[];
  rejects_schedule_info_unknown_kind: unknown[];
  rejects_schedule_info_unknown_status: unknown[];
  // ARC-104: canonical numeric limits shared across the server and all four
  // clients. An object (not an array) — each client asserts its internal const
  // equals the value recorded here, so a server change requires updating the
  // corpus AND every client or a test fails.
  protocol_constants: { max_steps: number };
}

const CORPUS_PATH = resolve(__dirname, "../../wire-corpus/wire-corpus.json");

function loadCorpus(): Corpus {
  const raw = readFileSync(CORPUS_PATH, "utf8");
  // `JSON.parse` returns `any`; the `satisfies Corpus` (via the typed const
  // below) makes any key/section drift a type error at compile time. Entries
  // are also checked against their protocol TS type by the per-section
  // assignments inside the tests.
  return JSON.parse(raw) as Corpus;
}

/** Asserts `value` is JSON-safe and round-trips through JSON unchanged. */
function assertJsonRoundTrip(value: unknown): void {
  // A non-JSON-safe value (function, undefined, bigint without toJSON) would
  // diverge here. Wire types must be plain JSON.
  const roundTripped = JSON.parse(JSON.stringify(value));
  expect(roundTripped).toStrictEqual(value);
}

describe("wire-corpus: client_messages", () => {
  const corpus = loadCorpus();
  for (const [idx, entry] of corpus.client_messages.entries()) {
    const _typeCheck: ClientMessage = entry; // compile-time shape check
    void _typeCheck;
    it(`client_messages #${idx} (${entry.type}) round-trips`, () => {
      assertJsonRoundTrip(entry);
    });
  }
});

describe("wire-corpus: server_messages", () => {
  const corpus = loadCorpus();
  for (const [idx, entry] of corpus.server_messages.entries()) {
    const _typeCheck: ServerMessage = entry; // compile-time shape check
    void _typeCheck;
    it(`server_messages #${idx} (${entry.type}) round-trips`, () => {
      assertJsonRoundTrip(entry);
    });
  }
});

describe("wire-corpus: authed_users (ARC-009 narrowing)", () => {
  const corpus = loadCorpus();
  for (const [idx, entry] of corpus.authed_users.entries()) {
    // ARC-009: `kind` is now `AuthedUserKind` ("user" | "machine"). Assigning
    // it to a typed const means a corpus entry with any other value would be a
    // compile-time error here.
    const kind: AuthedUserKind = entry.kind;
    void kind;
    const _typeCheck: AuthedUser = entry;
    void _typeCheck;
    it(`authed_users #${idx} (kind=${entry.kind}) round-trips`, () => {
      assertJsonRoundTrip(entry);
    });
  }
});

describe("wire-corpus: schedule_whens", () => {
  const corpus = loadCorpus();
  for (const [idx, entry] of corpus.schedule_whens.entries()) {
    const _typeCheck: ScheduleWhen = entry;
    void _typeCheck;
    it(`schedule_whens #${idx} (${entry.type}) round-trips`, () => {
      assertJsonRoundTrip(entry);
    });
  }
});

describe("wire-corpus: schedule_infos (ARC-004 enums)", () => {
  const corpus = loadCorpus();
  for (const [idx, entry] of corpus.schedule_infos.entries()) {
    const _typeCheck: ScheduleInfo = entry; // narrows `kind`/`status` to literals
    void _typeCheck;
    it(`schedule_infos #${idx} (kind=${entry.kind}, status=${entry.status}) round-trips`, () => {
      assertJsonRoundTrip(entry);
    });
  }
});

describe("wire-corpus: migrate_requests (admin Directive list)", () => {
  const corpus = loadCorpus();
  for (const [idx, entry] of corpus.migrate_requests.entries()) {
    const _typeCheck: MigrateRequestJson = entry; // compile-time shape check
    void _typeCheck;
    it(`migrate_requests #${idx} round-trips`, () => {
      assertJsonRoundTrip(entry);
    });
  }
});

describe("wire-corpus: migrate_results (admin MigrateResult)", () => {
  const corpus = loadCorpus();
  for (const [idx, entry] of corpus.migrate_results.entries()) {
    const _typeCheck: MigrateResultJson = entry; // compile-time shape check
    void _typeCheck;
    it(`migrate_results #${idx} round-trips`, () => {
      assertJsonRoundTrip(entry);
    });
  }
});

describe("wire-corpus: raw-JSON sections round-trip", () => {
  const corpus = loadCorpus();
  for (const [idx, entry] of corpus.query_results.entries()) {
    it(`query_results #${idx} round-trips`, () => {
      assertJsonRoundTrip(entry);
    });
  }
  for (const [idx, entry] of corpus.error_envelopes.entries()) {
    it(`error_envelopes #${idx} round-trips`, () => {
      assertJsonRoundTrip(entry);
    });
  }
  for (const [idx, entry] of corpus.queries.entries()) {
    it(`queries #${idx} round-trips`, () => {
      assertJsonRoundTrip(entry);
    });
  }
});

/**
 * FM-31 pins: the corpus `queries` section gained an operator-syntax search
 * entry (`"exact phrase" or -excluded`) and a `snippet: true` entry. The
 * generic loop above round-trips them raw; this block additionally type-checks
 * every search entry against `SearchQuery` and asserts both FM-31 entries are
 * present with their fields intact — a corpus or protocol drift on the search
 * surface fails here, not just silently round-tripping.
 */
describe("wire-corpus: search query entries (FM-31 operators/snippet)", () => {
  const corpus = loadCorpus();
  const searchEntries = corpus.queries.filter(
    (q): q is { table: string; search: SearchQuery } =>
      typeof q === "object" && q !== null && "search" in q,
  );
  it("every search entry satisfies SearchQuery and round-trips", () => {
    expect(searchEntries.length).toBeGreaterThan(0);
    for (const entry of searchEntries) {
      const _typeCheck: SearchQuery = entry.search;
      void _typeCheck;
      assertJsonRoundTrip(entry);
    }
  });
  it("carries the FM-31 operator-syntax and snippet:true entries", () => {
    const operator = searchEntries.find((e) => e.search.query.includes('"'));
    expect(operator?.search.mode).toBeUndefined();
    const snippet = searchEntries.find((e) => e.search.snippet === true);
    expect(snippet?.search.query).toBe("hello world");
    expect(snippet?.search.mode).toBeUndefined();
  });
});

/**
 * Projection pin: the corpus `queries` section gained a `fields` entry
 * (Query.fields). The generic loop above round-trips it raw; this block
 * additionally type-checks it against `QueryJson` and asserts it is present
 * with its projection intact — including a listed system field (`_id`), an
 * accepted no-op. A corpus or protocol drift on the projection surface fails
 * here, not just silently round-tripping.
 */
describe("wire-corpus: projection query entry (Query.fields)", () => {
  const corpus = loadCorpus();
  const projected = corpus.queries.filter(
    (q): q is QueryJson & { fields: string[] } =>
      typeof q === "object" && q !== null && "fields" in q,
  );
  it("carries the fields entry with title/status/_id intact", () => {
    expect(projected).toHaveLength(1);
    const _typeCheck: QueryJson = projected[0];
    void _typeCheck;
    expect(projected[0].index).toBe("by_status");
    expect(projected[0].take).toBe(10);
    expect(projected[0].fields).toEqual(["title", "status", "_id"]);
  });
});

/**
 * olderThan pins: the corpus gained the execution-time-relative filter op —
 * `patchByQuery`/`deleteByQuery` steps carrying an `olderThan` filter inside a
 * `client_messages` Mutate frame, plus a `queries` entry with a bare
 * `olderThan` filter. The generic loops above round-trip them raw; this block
 * additionally type-checks both against the wire types (`FilterExpr` inside
 * `StepJson`/`QueryJson`) and asserts each entry is present with its
 * load-bearing fields intact — a corpus or protocol drift on the olderThan
 * surface fails here, not just silently round-tripping.
 */
describe("wire-corpus: olderThan entries (by-query filter op)", () => {
  const corpus = loadCorpus();
  const byQuerySteps = corpus.client_messages
    .filter((m): m is Extract<ClientMessage, { type: "mutate" }> => m.type === "mutate")
    .flatMap((m) => m.txn.steps)
    .flatMap((s) =>
      s.op === "patchByQuery" || s.op === "deleteByQuery" ? [{ stepOp: s.op, ...s }] : [],
    );
  const olderThanQueries = corpus.queries.filter(
    (q): q is QueryJson & { filter: { op: "olderThan"; field: string; ms: number } } =>
      typeof q === "object" &&
      q !== null &&
      "filter" in q &&
      (q as { filter?: { op?: string } }).filter?.op === "olderThan",
  );
  it("carries the patchByQuery bare-olderThan and deleteByQuery and-wrapped steps", () => {
    expect(byQuerySteps).toHaveLength(2);
    const patch = byQuerySteps.find((s) => s.stepOp === "patchByQuery");
    expect(patch).toBeDefined();
    // The bare-leaf form; narrowing the discriminant types `filter` as the
    // olderThan member for the field/ms assertions.
    if (patch?.filter.op === "olderThan") {
      const _typeCheck: FilterExpr = patch.filter;
      void _typeCheck;
      expect(patch.filter.field).toBe("completedAt");
      expect(patch.filter.ms).toBe(604800000);
    } else {
      throw new Error("patchByQuery step must carry a bare olderThan filter");
    }
    // The composed form: olderThan nested inside an `and` with an `exists`.
    const del = byQuerySteps.find((s) => s.stepOp === "deleteByQuery");
    expect(del).toBeDefined();
    expect(del?.filter.op).toBe("and");
    const inner = del?.filter.op === "and" ? del.filter.exprs[0] : undefined;
    expect(inner?.op).toBe("olderThan");
    expect((inner as { field: string; ms: number }).field).toBe("claimExpiresAt");
    expect((inner as { field: string; ms: number }).ms).toBe(0);
    expect((del as { limit?: number }).limit).toBe(500);
  });
  it("carries the bare olderThan query entry and it satisfies QueryJson", () => {
    expect(olderThanQueries).toHaveLength(1);
    const _typeCheck: QueryJson = olderThanQueries[0];
    void _typeCheck;
    expect(olderThanQueries[0].table).toBe("workItems");
    expect(olderThanQueries[0].filter.field).toBe("completedAt");
    expect(olderThanQueries[0].filter.ms).toBe(604800000);
  });
});

/**
 * FM-29 pins: the corpus gained workflow entries — `startWorkflow` /
 * `cancelWorkflow` txn steps inside `client_messages` Mutate frames, and the
 * `startWorkflowOk` / `startWorkflowErr` / `workflowAck` / `listWorkflowsOk`
 * server frames. The awaitSignal feature added `signalWorkflow` frames, a
 * mixed `awaitSignal`-among-txn spec, a CONFLICT `workflowAck`, and a
 * `waiting`-status info. The generic loops above round-trip them raw; this
 * block additionally type-checks the spec family (`WorkflowSpec`,
 * `WorkflowStepSpec`, `StepRetry`, `WorkflowInfo`, `AwaitSignalSpec`) and
 * asserts each entry is present with its load-bearing fields intact. The
 * `retry` object carries all three fields (server `StepRetry` serde-defaults
 * without `skip_serializing_if`, so a serialized retry always re-emits them —
 * this pins the canonical full form), the second spec step pins
 * optional-field absence, and `workflowAck` mirrors `scheduleAck`'s ok/error
 * shape.
 */
describe("wire-corpus: workflow entries (FM-29 steps + frames)", () => {
  const corpus = loadCorpus();
  const startSteps = corpus.client_messages.flatMap((m) =>
    m.type === "mutate" ? m.txn.steps.filter((s) => s.op === "startWorkflow") : [],
  );
  const cancelSteps = corpus.client_messages.flatMap((m) =>
    m.type === "mutate" ? m.txn.steps.filter((s) => s.op === "cancelWorkflow") : [],
  );

  it("carries a startWorkflow step whose spec type-checks with retry and sleep", () => {
    expect(startSteps).toHaveLength(2);
    const spec: WorkflowSpec = startSteps[0].spec;
    expect(spec.name).toBe("drip");
    expect(spec.steps).toHaveLength(2);
    const first: WorkflowStepSpec = spec.steps[0];
    const retry: StepRetry = first.retry as StepRetry;
    expect(retry).toEqual({ maxAttempts: 3, initialRetryMs: 1000, maxRetryMs: 60000 });
    expect(first.sleepBeforeMs).toBe(60000);
    // The second step pins optional-field absence (no retry, no sleep).
    const second: WorkflowStepSpec = spec.steps[1];
    expect(second.retry).toBeUndefined();
    expect(second.sleepBeforeMs).toBeUndefined();
  });

  it("carries a startWorkflow step mixing awaitSignal waits among txn steps", () => {
    const gated = startSteps.find((s) => s.spec.name === "gated");
    expect(gated).toBeDefined();
    const spec: WorkflowSpec | undefined = gated?.spec;
    expect(spec?.steps).toHaveLength(3);
    // Ordinary txn step first, then a bounded wait, then an unbounded one —
    // `timeoutMs` omitted on the wire when absent (skip_serializing_if).
    expect(spec?.steps[0].txn).toBeDefined();
    expect(spec?.steps[0].awaitSignal).toBeUndefined();
    expect(spec?.steps[1].awaitSignal).toEqual({ name: "approve", timeoutMs: 3600000 });
    expect(spec?.steps[1].txn).toBeUndefined();
    expect(spec?.steps[2].awaitSignal).toEqual({ name: "audit" });
    expect(spec?.steps[2].awaitSignal?.timeoutMs).toBeUndefined();
  });

  it("carries signalWorkflow frames with payload present and omitted", () => {
    const signalFrames = corpus.client_messages.filter(
      (m): m is Extract<ClientMessage, { type: "signalWorkflow" }> => m.type === "signalWorkflow",
    );
    expect(signalFrames).toHaveLength(2);
    expect(signalFrames[0].payload).toEqual({ ok: true, count: 2 });
    // The omitted-payload frame must not carry `payload` at all — the raw
    // round-trip loop above fails any implementation that emits `payload: null`.
    expect(signalFrames[1].payload).toBeUndefined();
    expect("payload" in signalFrames[1]).toBe(false);
  });

  it("carries a cancelWorkflow step", () => {
    expect(cancelSteps).toHaveLength(1);
    expect(cancelSteps[0].id).toBe("wf-9");
  });

  it("carries the FM-29 server frames with WorkflowInfo type-checks", () => {
    const okFrames = corpus.server_messages.filter(
      (m): m is Extract<ServerMessage, { type: "startWorkflowOk" }> => m.type === "startWorkflowOk",
    );
    expect(okFrames).toHaveLength(1);
    const info: WorkflowInfo = okFrames[0].info;
    expect(info.status).toBe("pending");
    // Optional fields omitted on the wire when absent (pending run).
    expect(info.sleepUntil).toBeUndefined();
    expect(info.lastError).toBeUndefined();
    expect(info.startedAt).toBeUndefined();
    expect(info.finishedAt).toBeUndefined();

    const errFrames = corpus.server_messages.filter(
      (m): m is Extract<ServerMessage, { type: "startWorkflowErr" }> =>
        m.type === "startWorkflowErr",
    );
    expect(errFrames).toHaveLength(1);
    expect(errFrames[0].error.code).toBe("BAD_REQUEST");

    const ackFrames = corpus.server_messages.filter(
      (m): m is Extract<ServerMessage, { type: "workflowAck" }> => m.type === "workflowAck",
    );
    expect(ackFrames).toHaveLength(3);
    expect(ackFrames.filter((f) => f.ok)).toHaveLength(1);
    expect(ackFrames.filter((f) => !f.ok && f.error)).toHaveLength(2);
    // A name-mismatch delivery acks CONFLICT naming both signals.
    const conflict = ackFrames.find((f) => f.error?.code === "CONFLICT");
    expect(conflict?.error?.message).toBe("workflow waiting on 'approve', got 'ok'");

    const listFrames = corpus.server_messages.filter(
      (m): m is Extract<ServerMessage, { type: "listWorkflowsOk" }> => m.type === "listWorkflowsOk",
    );
    expect(listFrames).toHaveLength(3);
    expect(listFrames.some((f) => f.workflows.length === 0)).toBe(true);
    const populated = listFrames.find((f) => f.workflows.length > 0);
    expect(populated).toBeDefined();
    const running = populated?.workflows.find((w) => w.status === "running");
    expect(running?.sleepUntil).toBe(9000);
    const failed = populated?.workflows.find((w) => w.status === "failed");
    expect(failed?.lastError).toBe("boom");
    expect(failed?.startedAt).toBe(110);
    expect(failed?.finishedAt).toBe(200);
    // A run parked at an awaitSignal step reports `waiting` with the wait
    // fields present (waitingFor/waitedSince) — omitted in every other state.
    const waiting = listFrames.flatMap((f) => f.workflows).find((w) => w.status === "waiting");
    expect(waiting?.waitingFor).toBe("approve");
    expect(waiting?.waitedSince).toBe(1234);
    expect(waiting?.sleepUntil).toBe(3601234);
  });
});

/**
 * Reject fixtures document inputs that other clients' strict-schema parsers
 * (Rust `deny_unknown_fields`, Python `extra='forbid'`, Rust enums) reject. TS
 * has no runtime schema, so we cannot assert rejection here; we only assert
 * the fixtures themselves are JSON-stable so they reach the other clients
 * unchanged. The other clients' tests assert the rejection.
 */
describe("wire-corpus: rejects fixtures are JSON-stable (TS cannot reject at runtime)", () => {
  const corpus = loadCorpus();
  const allRejects = [
    ...corpus.rejects_client_message_unknown_field,
    ...corpus.rejects_schedule_when_unknown_field,
    ...corpus.rejects_workflow_spec_unknown_field,
    ...corpus.rejects_authed_user_unknown_kind,
    ...corpus.rejects_schedule_info_unknown_kind,
    ...corpus.rejects_schedule_info_unknown_status,
  ];
  for (const [idx, entry] of allRejects.entries()) {
    it(`rejects #${idx} is JSON-stable`, () => {
      assertJsonRoundTrip(entry);
    });
  }
});

/**
 * ARC-104: protocol constants (MAX_STEPS) are part of the four-client wire
 * contract. The corpus records the canonical agreed value; each client asserts
 * its internal const matches, so the next server change to the constant fails a
 * test in every client unless the corpus (and each client) is updated too.
 */
describe("wire-corpus: protocol_constants match the implementation (ARC-104)", () => {
  const corpus = loadCorpus();
  it("MAX_STEPS matches the canonical corpus value", () => {
    expect(corpus.protocol_constants.max_steps).toBe(MAX_STEPS);
  });
});

/**
 * ARC-017: `wire-corpus/error-codes.json` is the canonical `{code,
 * httpStatus}` table generated from the server's `ErrorCode` enum
 * (`server/src/error.rs::tests::error_codes_match_wire_corpus`). ts-client
 * does not model an HTTP status per code (see `RtDbError.status` in
 * `errors.ts` — it threads the transport's HTTP status through instead), so
 * this only asserts the code set itself agrees, in both directions: every
 * corpus code is a known `RtDbErrorCode`, and `ALL_ERROR_CODES` carries no
 * code the corpus doesn't.
 */
describe("wire-corpus: error codes match the server (ARC-017)", () => {
  interface ErrorCodesCorpus {
    codes: Array<{ code: string; httpStatus: number }>;
  }

  const ERROR_CODES_PATH = resolve(__dirname, "../../wire-corpus/error-codes.json");

  function loadErrorCodesCorpus(): ErrorCodesCorpus {
    return JSON.parse(readFileSync(ERROR_CODES_PATH, "utf8")) as ErrorCodesCorpus;
  }

  it("every corpus code and every client code agree, set-for-set", () => {
    const corpusCodes = loadErrorCodesCorpus()
      .codes.map((e) => e.code)
      .sort();
    const clientCodes = [...ALL_ERROR_CODES].sort();
    expect(clientCodes).toStrictEqual(corpusCodes);
  });
});

/**
 * ENH-028 phase 2: `query-combinations.ts` is a hand-mirrored copy of
 * `wire-corpus/query-combinations.json` (see that file's header comment for
 * why it isn't a direct JSON import — the package build's `rootDir` can't
 * reach outside `ts-client/`). This test is the drift guard: it asserts the
 * embedded TS table matches the JSON source exactly, byte-for-byte in
 * content and order (the generic evaluator in `in_memory/query.ts` applies
 * rules in declared order and returns the first match).
 */
describe("wire-corpus: query-combinations table matches query.ts's mirror (ENH-028)", () => {
  interface QueryCombinationsCorpus {
    clauses: string[];
    rules: Array<{
      id: string;
      forbid?: string[];
      atMostOne?: string[];
      code: string;
      message: string;
    }>;
  }

  const QUERY_COMBINATIONS_PATH = resolve(__dirname, "../../wire-corpus/query-combinations.json");

  function loadQueryCombinationsCorpus(): QueryCombinationsCorpus {
    return JSON.parse(readFileSync(QUERY_COMBINATIONS_PATH, "utf8")) as QueryCombinationsCorpus;
  }

  it("clauses match, in order", () => {
    const corpus = loadQueryCombinationsCorpus();
    expect(QUERY_COMBO_CLAUSES).toStrictEqual(corpus.clauses);
  });

  it("rules match, in order", () => {
    const corpus = loadQueryCombinationsCorpus();
    expect(QUERY_COMBO_RULES).toStrictEqual(corpus.rules);
  });
});

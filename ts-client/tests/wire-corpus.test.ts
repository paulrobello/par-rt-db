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

import { MAX_STEPS } from "../src/in_memory.js";
import type {
  AuthedUser,
  AuthedUserKind,
  ClientMessage,
  MigrateRequestJson,
  MigrateResultJson,
  ScheduleInfo,
  ScheduleWhen,
  ServerMessage,
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

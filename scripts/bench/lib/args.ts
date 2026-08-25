/** CLI/env argument parsing for `load.ts` (ENH-033 phase B). Every setting has
 * an env-var fallback so `make bench` (a later phase) can compose this script
 * with the same `RTDB_*` variables it uses to start the servers under test. */

export type Scenario = "a" | "b" | "c" | "d";

export const ALL_SCENARIOS: Scenario[] = ["a", "b", "c", "d"];

export interface LoadOptions {
  /** Base URL of the server under test (scenario a/b/d, and the "primary"
   * server for scenario c's non-owner-vs-owner comparison). */
  url: string;
  /** Second server's base URL, used only by scenario c (multi-instance). */
  shadowUrl: string;
  /** Server-wide admin bearer (`RTDB_ADMIN_KEY`). Required to mint a per-db
   * machine token — the load script never runs unauthenticated. */
  adminKey: string;
  /** Database name the script creates/reuses for its scenarios. */
  db: string;
  /** Scenarios to run, in order. */
  scenarios: Scenario[];
  /** Duration in seconds for the sustained-write scenario (a). */
  durationSec: number;
  /** Concurrent writers (scenario a) / subscribers (scenario b). */
  concurrency: number;
  /** Concurrent subscribers for scenario d's fan-out. */
  subscribers: number;
  /** Rows deleted in scenario d's bulk `deleteByQuery`. */
  bulkRows: number;
  /** PID of the process currently holding db `db`'s ownership lease, if
   * known. When set, scenario c SIGKILLs it and times recovery; when unset,
   * scenario c only reports forward round-trip latency and skips the
   * takeover-time measurement (there is no HTTP-exposed leadership signal to
   * discover it from — see `scripts/bench/README` notes in the report). */
  ownerPid?: number;
  /** git short sha this run is attributed to (`RTDB_BUILD_COMMIT` or
   * `git rev-parse --short HEAD`, mirroring `server/build.rs`'s convention). */
  sha: string;
  /** Output JSON path. Defaults to `bench/results/<sha>.json`. */
  out: string;
}

function usage(): string {
  return `Usage: bun run scripts/bench/load.ts [options]

Drives a running par-rt-db server (or two, for scenario c) over HTTP + WS and
reports commit throughput/latency, subscription fan-out latency, forward
round-trip latency, and bulk-mutation turn hold time. Assumes the server(s)
are already running (dev DB up, \`cargo run --release\`) — this script does not
start or stop them.

Options:
  --url <url>            Server base URL (default: RTDB_TEST_SERVER_URL or
                          http://127.0.0.1:\${RTDB_PORT:-8300})
  --shadow-url <url>     Second (non-owner) server base URL for scenario c
                          (default: http://127.0.0.1:8301)
  --admin-key <key>      Server admin bearer (default: RTDB_TEST_ADMIN_KEY or
                          RTDB_ADMIN_KEY env)
  --db <name>            Database name to create/reuse (default: bench)
  --scenario <a|b|c|d|all>  Comma-separated scenario list, or "all" (default: all)
  --duration <sec>       Scenario a write duration in seconds (default: 30)
  --concurrency <n>      Writer/subscriber concurrency for a/b (default: 8)
  --subscribers <n>      Subscriber count for scenario d (default: 100)
  --bulk-rows <n>        Row count for scenario d's bulk delete (default: 5000)
  --owner-pid <pid>      PID of the current lease owner, for scenario c's
                          takeover-time measurement (optional)
  --sha <sha>            git sha this run is attributed to (default: git rev-parse --short HEAD)
  --out <path>           Output JSON path (default: bench/results/<sha>.json)
  --help                 Print this message and exit
`;
}

/** Parses argv (excluding the `bun run script.ts` prefix). Exits the process
 * after printing usage on `--help` or a `git rev-parse` failure with no
 * `--sha` override — this is a CLI entry point, not a library function. */
export function parseArgs(argv: string[]): LoadOptions {
  const flags = new Map<string, string>();
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--help" || arg === "-h") {
      console.log(usage());
      process.exit(0);
    }
    if (arg?.startsWith("--")) {
      const eq = arg.indexOf("=");
      if (eq !== -1) {
        flags.set(arg.slice(2, eq), arg.slice(eq + 1));
      } else {
        const next = argv[i + 1];
        flags.set(arg.slice(2), next ?? "");
        if (next !== undefined && !next.startsWith("--")) i++;
      }
    }
  }

  const get = (key: string, fallback?: string): string | undefined => flags.get(key) ?? fallback;

  const port = process.env.RTDB_PORT ?? "8300";
  const url = get("url", process.env.RTDB_TEST_SERVER_URL ?? `http://127.0.0.1:${port}`) as string;
  const shadowUrl = get("shadow-url", "http://127.0.0.1:8301") as string;
  const adminKey = get(
    "admin-key",
    process.env.RTDB_TEST_ADMIN_KEY ?? process.env.RTDB_ADMIN_KEY ?? "",
  ) as string;
  const db = get("db", "bench") as string;

  const scenarioArg = get("scenario", "all") as string;
  const scenarios: Scenario[] =
    scenarioArg === "all"
      ? ALL_SCENARIOS
      : (scenarioArg.split(",").map((s) => s.trim()) as Scenario[]).filter((s) =>
          ALL_SCENARIOS.includes(s),
        );

  const durationSec = Number(get("duration", "30"));
  const concurrency = Number(get("concurrency", "8"));
  const subscribers = Number(get("subscribers", "100"));
  const bulkRows = Number(get("bulk-rows", "5000"));
  const ownerPidRaw = get("owner-pid");
  const ownerPid = ownerPidRaw ? Number(ownerPidRaw) : undefined;

  const sha = (get("sha", process.env.RTDB_BUILD_COMMIT) ?? resolveGitSha()) as string;
  const out = get("out", `bench/results/${sha}.json`) as string;

  if (!adminKey) {
    console.error(
      "error: no admin key. Pass --admin-key, or set RTDB_TEST_ADMIN_KEY / RTDB_ADMIN_KEY.",
    );
    process.exit(1);
  }

  return {
    url,
    shadowUrl,
    adminKey,
    db,
    scenarios,
    durationSec,
    concurrency,
    subscribers,
    bulkRows,
    ...(ownerPid !== undefined && !Number.isNaN(ownerPid) ? { ownerPid } : {}),
    sha,
    out,
  };
}

/** `git rev-parse --short HEAD`, mirroring `server/build.rs`'s resolution
 * order (env override first, then git, else a safe placeholder — never
 * throws, so a non-git checkout still produces a result file). */
function resolveGitSha(): string {
  try {
    const proc = Bun.spawnSync(["git", "rev-parse", "--short", "HEAD"]);
    const out = proc.stdout.toString().trim();
    if (proc.success && out) return out;
  } catch {
    // fall through to placeholder
  }
  return "unknown";
}

import { useCallback, useEffect, useRef, useState } from "react";
import { Button, Placard, Spinner } from "../components/ui";
import { useAdmin } from "../lib/admin";
import type { TokenRow } from "../lib/types";
import s from "./TokensPage.module.css";

function timeLabel(ms: number): string {
  return new Date(ms).toLocaleString(undefined, { hour12: false });
}

/** An expiry is "expired" once its (ms) deadline is in the past. `null` = never. */
function isExpired(row: TokenRow): boolean {
  return row.expiresAt !== null && row.expiresAt <= Date.now();
}

export function TokensPage() {
  const { client, databases } = useAdmin();
  const [db, setDb] = useState<string>("");
  const [tokens, setTokens] = useState<TokenRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [listError, setListError] = useState<string | null>(null);

  // Mint-form state.
  const [name, setName] = useState("");
  const [expiry, setExpiry] = useState(""); // datetime-local string; "" = no expiry
  const [readOnly, setReadOnly] = useState(false);
  const [tablesText, setTablesText] = useState(""); // comma-separated
  const [minting, setMinting] = useState(false);
  const [mintError, setMintError] = useState<string | null>(null);
  // The plaintext token is returned ONLY at mint time (the server stores a hash,
  // so it cannot be recovered). Surface it once for copy.
  const [minted, setMinted] = useState<{ tokenId: string; token: string } | null>(null);

  // Per-row action state.
  const [actionError, setActionError] = useState<string | null>(null);
  const [pendingId, setPendingId] = useState<string | null>(null);
  const [confirmingRevoke, setConfirmingRevoke] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);
  const copiedTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Auto-select the first database once the list arrives.
  useEffect(() => {
    if (!db && databases.length > 0) setDb(databases[0]);
  }, [db, databases]);

  const refresh = useCallback(async () => {
    if (!db) return;
    setLoading(true);
    setListError(null);
    try {
      setTokens(await client.listTokens(db));
    } catch (e) {
      setListError(e instanceof Error ? e.message : String(e));
      setTokens([]);
    } finally {
      setLoading(false);
    }
  }, [client, db]);

  useEffect(() => {
    setTokens([]);
    setActionError(null);
    setConfirmingRevoke(null);
    if (db) void refresh();
  }, [db, refresh]);

  useEffect(() => {
    return () => {
      if (copiedTimer.current) clearTimeout(copiedTimer.current);
    };
  }, []);

  async function mint() {
    if (!db) return;
    const trimmedName = name.trim();
    if (!trimmedName) {
      setMintError("name is required.");
      return;
    }
    const opts: { expiresAt?: number; readOnly?: boolean; tables?: string[] } = {};
    if (readOnly) opts.readOnly = true;
    if (expiry) {
      const ms = new Date(expiry).getTime();
      if (!Number.isFinite(ms)) {
        setMintError("expiry is not a valid date/time.");
        return;
      }
      opts.expiresAt = ms;
    }
    const tables = tablesText
      .split(",")
      .map((t) => t.trim())
      .filter(Boolean);
    if (tables.length > 0) opts.tables = tables;

    setMinting(true);
    setMintError(null);
    setMinted(null);
    try {
      setMinted(await client.mintToken(db, trimmedName, opts));
      setName("");
      setExpiry("");
      setReadOnly(false);
      setTablesText("");
      await refresh();
    } catch (e) {
      setMintError(e instanceof Error ? e.message : String(e));
    } finally {
      setMinting(false);
    }
  }

  async function copyToken() {
    if (!minted) return;
    try {
      await navigator.clipboard.writeText(minted.token);
      setCopied(true);
      if (copiedTimer.current) clearTimeout(copiedTimer.current);
      copiedTimer.current = setTimeout(() => setCopied(false), 1500);
    } catch {
      // clipboard may be unavailable (insecure context) — surface quietly
      setActionError("clipboard unavailable");
    }
  }

  async function revoke(row: TokenRow) {
    setPendingId(row.id);
    setActionError(null);
    try {
      await client.revokeToken(row.id);
      setConfirmingRevoke(null);
      await refresh();
    } catch (e) {
      setActionError(e instanceof Error ? e.message : String(e));
    } finally {
      setPendingId(null);
    }
  }

  return (
    <section className={s.page}>
      <Placard>Tokens</Placard>
      <div className={s.head}>
        <h1 className={s.title}>Machine tokens</h1>
        <span className={s.count}>{tokens.length} token(s)</span>
      </div>

      <div className={s.toolbar}>
        <label className={s.field}>
          <span className={s.fieldLabel}>database</span>
          <select
            className={s.select}
            value={db}
            onChange={(e) => setDb(e.target.value)}
            disabled={databases.length === 0}
          >
            {databases.length === 0 && <option value="">— none —</option>}
            {databases.map((name) => (
              <option key={name} value={name}>
                {name}
              </option>
            ))}
          </select>
        </label>
        <Button variant="primary" onClick={() => void refresh()} disabled={loading || !db}>
          {loading ? "refreshing…" : "refresh"}
        </Button>
        {loading && <Spinner label="loading tokens" />}
      </div>

      {listError && <p className={s.error}>{listError}</p>}
      {actionError && <p className={s.error}>{actionError}</p>}

      {!db ? (
        <p className={s.muted}>select a database.</p>
      ) : loading && tokens.length === 0 ? (
        <p className={s.muted}>loading…</p>
      ) : tokens.length === 0 ? (
        <p className={s.muted}>no machine tokens.</p>
      ) : (
        <div className={s.tableWrap}>
          <table className={s.table}>
            <thead>
              <tr>
                <th>id</th>
                <th>name</th>
                <th>created</th>
                <th>expiry</th>
                <th>access</th>
                <th>tables</th>
                <th>status</th>
                <th>actions</th>
              </tr>
            </thead>
            <tbody>
              {tokens.map((row) => {
                const expired = isExpired(row);
                const confirming = confirmingRevoke === row.id;
                const busy = pendingId === row.id;
                return (
                  <tr key={row.id} className={row.revoked ? s.rowRevoked : ""}>
                    <td className={s.idCell} title={row.id}>
                      {row.id.slice(0, 8)}
                    </td>
                    <td className={s.nameCell} title={row.name}>
                      {row.name}
                    </td>
                    <td>{timeLabel(row.createdAt)}</td>
                    <td>
                      {row.expiresAt === null ? (
                        <span className={s.hint}>never</span>
                      ) : (
                        timeLabel(row.expiresAt)
                      )}
                    </td>
                    <td>
                      <span className={`${s.badge} ${row.readOnly ? s.badgeRo : s.badgeRw}`}>
                        {row.readOnly ? "read-only" : "read-write"}
                      </span>
                    </td>
                    <td className={s.tablesCell} title={row.tables?.join(", ") ?? "all tables"}>
                      {row.tables === null || row.tables.length === 0
                        ? "all"
                        : row.tables.join(", ")}
                    </td>
                    <td>
                      {row.revoked ? (
                        <span className={`${s.badge} ${s.badgeRevoked}`}>revoked</span>
                      ) : expired ? (
                        <span className={`${s.badge} ${s.badgeExpired}`}>expired</span>
                      ) : (
                        <span className={`${s.badge} ${s.badgeActive}`}>active</span>
                      )}
                    </td>
                    <td>
                      <div className={s.rowActions}>
                        {row.revoked ? null : confirming ? (
                          <span className={s.confirmInline}>
                            <span className={s.confirmLabel}>revoke?</span>
                            <Button
                              variant="danger"
                              onClick={() => void revoke(row)}
                              disabled={busy}
                            >
                              {busy ? "…" : "confirm"}
                            </Button>
                            <Button onClick={() => setConfirmingRevoke(null)} disabled={busy}>
                              no
                            </Button>
                          </span>
                        ) : (
                          <Button
                            variant="danger"
                            onClick={() => setConfirmingRevoke(row.id)}
                            disabled={busy}
                          >
                            revoke
                          </Button>
                        )}
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      <section className={s.createBlock}>
        <Placard>Mint a token</Placard>
        <div className={s.toolbar}>
          <label className={s.field}>
            <span className={s.fieldLabel}>name</span>
            <input
              className={s.input}
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="ci / prod-reader / …"
            />
          </label>
          <label className={s.field}>
            <span className={s.fieldLabel}>expires (leave empty = never)</span>
            <input
              className={s.input}
              type="datetime-local"
              value={expiry}
              onChange={(e) => setExpiry(e.target.value)}
            />
          </label>
          <label className={s.field}>
            <span className={s.fieldLabel}>access</span>
            <div className={s.segment}>
              <button
                type="button"
                onClick={() => setReadOnly(false)}
                className={`${s.segBtn} ${!readOnly ? s.segBtnActive : ""}`}
                aria-pressed={!readOnly}
              >
                read-write
              </button>
              <button
                type="button"
                onClick={() => setReadOnly(true)}
                className={`${s.segBtn} ${readOnly ? s.segBtnActive : ""}`}
                aria-pressed={readOnly}
              >
                read-only
              </button>
            </div>
          </label>
          <label className={s.field}>
            <span className={s.fieldLabel}>tables (comma-separated, empty = all)</span>
            <input
              className={s.input}
              value={tablesText}
              onChange={(e) => setTablesText(e.target.value)}
              placeholder="users, audit, …"
            />
          </label>
        </div>

        <div className={s.actions}>
          <Button variant="primary" onClick={() => void mint()} disabled={minting || !db}>
            {minting ? "minting…" : "mint"}
          </Button>
          {minting && <Spinner label="minting" />}
          <span className={s.warn}>writes a credential</span>
        </div>
        {mintError && <p className={s.error}>{mintError}</p>}

        {minted && (
          <div className={s.tokenBlock}>
            <Placard>Token — copy now, it will not be shown again</Placard>
            <div className={s.tokenRow}>
              <code className={s.tokenValue}>{minted.token}</code>
              <Button variant="primary" onClick={() => void copyToken()}>
                {copied ? "copied!" : "copy token"}
              </Button>
              <Button onClick={() => setMinted(null)}>dismiss</Button>
            </div>
            <span className={s.hint}>token id {minted.tokenId.slice(0, 8)}</span>
          </div>
        )}
      </section>
    </section>
  );
}

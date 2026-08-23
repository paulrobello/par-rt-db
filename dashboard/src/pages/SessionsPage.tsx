/** Active session management — list and revoke OAuth and anonymous sessions across the instance. */
import { useState } from "react";
import { Button, Placard, Spinner } from "../components/ui";
import { useAdmin } from "../lib/admin";
import { toErrorMessage } from "../lib/errors";
import type { SessionRow } from "../lib/types";
import { useAsync } from "../lib/useAsync";
import s from "./SessionsPage.module.css";

function timeLabel(ms: number): string {
  return new Date(ms).toLocaleString(undefined, { hour12: false });
}

export function SessionsPage() {
  const { client } = useAdmin();
  const [userFilter, setUserFilter] = useState("");
  const [pendingHash, setPendingHash] = useState<string | null>(null);
  const [confirmingRevoke, setConfirmingRevoke] = useState<string | null>(null);
  const [confirmingRemoveAll, setConfirmingRemoveAll] = useState(false);
  const [bulkBusy, setBulkBusy] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);

  // Load once on mount ([client] only — the filter is applied via the Refresh
  // button or Enter, not on every keystroke). `refresh` re-reads the current
  // filter through the hook's fetcher ref.
  const {
    data: sessions,
    loading,
    error: listError,
    refresh,
  } = useAsync(
    () => client.listSessions(userFilter.trim() ? { user: userFilter.trim() } : undefined),
    [client],
    [] as SessionRow[],
  );

  const expiredCount = sessions.filter((row) => row.expiresAt <= Date.now()).length;

  async function revoke(row: SessionRow) {
    setPendingHash(row.tokenHash);
    setActionError(null);
    try {
      await client.revokeSession(row.tokenHash);
      setConfirmingRevoke(null);
      await refresh();
    } catch (e) {
      setActionError(toErrorMessage(e));
    } finally {
      setPendingHash(null);
    }
  }

  async function removeAllExpired() {
    setBulkBusy(true);
    setActionError(null);
    try {
      await client.revokeExpiredSessions();
      setConfirmingRemoveAll(false);
      await refresh();
    } catch (e) {
      setActionError(toErrorMessage(e));
    } finally {
      setBulkBusy(false);
    }
  }

  return (
    <section className={s.page}>
      <Placard>Sessions</Placard>
      <div className={s.head}>
        <h1 className={s.title}>Active sessions</h1>
        <span className={s.count}>{sessions.length} session(s)</span>
      </div>

      <div className={s.toolbar}>
        <label className={s.field}>
          <span className={s.fieldLabel}>user filter</span>
          <input
            className={s.input}
            value={userFilter}
            onChange={(e) => setUserFilter(e.target.value)}
            placeholder="user id or email"
            onKeyDown={(e) => {
              if (e.key === "Enter") void refresh();
            }}
          />
        </label>
        <Button variant="primary" onClick={() => void refresh()} disabled={loading}>
          {loading ? "refreshing…" : "refresh"}
        </Button>
        {confirmingRemoveAll ? (
          <span className={s.confirmInline}>
            <span className={s.confirmLabel}>remove {expiredCount} expired session(s)?</span>
            <Button variant="danger" onClick={() => void removeAllExpired()} disabled={bulkBusy}>
              {bulkBusy ? "…" : "confirm"}
            </Button>
            <Button onClick={() => setConfirmingRemoveAll(false)} disabled={bulkBusy}>
              no
            </Button>
          </span>
        ) : (
          <Button
            variant="danger"
            onClick={() => setConfirmingRemoveAll(true)}
            disabled={expiredCount === 0 || loading || bulkBusy}
          >
            remove all expired{expiredCount > 0 ? ` (${expiredCount})` : ""}
          </Button>
        )}
        {loading && <Spinner label="loading sessions" />}
      </div>

      {listError && <p className={s.error}>{listError}</p>}
      {actionError && <p className={s.error}>{actionError}</p>}

      {loading && sessions.length === 0 ? (
        <p className={s.muted}>loading…</p>
      ) : sessions.length === 0 ? (
        <p className={s.muted}>no active sessions.</p>
      ) : (
        <div className={s.tableWrap}>
          <table className={s.table}>
            <thead>
              <tr>
                <th>user</th>
                <th>email</th>
                <th>kind</th>
                <th>created</th>
                <th>expires</th>
                <th>actions</th>
              </tr>
            </thead>
            <tbody>
              {sessions.map((row) => {
                const confirming = confirmingRevoke === row.tokenHash;
                const busy = pendingHash === row.tokenHash;
                const expired = row.expiresAt <= Date.now();
                return (
                  <tr key={row.tokenHash}>
                    <td className={s.nameCell} title={row.userId}>
                      {row.login ?? row.email ?? row.userId}
                    </td>
                    <td>{row.email === null ? <span className={s.hint}>—</span> : row.email}</td>
                    <td>
                      {row.anonymous ? (
                        <span className={`${s.badge} ${s.badgeAnon}`}>anonymous</span>
                      ) : (
                        <span className={s.hint}>—</span>
                      )}
                    </td>
                    <td>{timeLabel(row.createdAt)}</td>
                    <td>
                      <span className={s.expires}>
                        {timeLabel(row.expiresAt)}
                        {expired && <span className={`${s.badge} ${s.badgeExpired}`}>expired</span>}
                      </span>
                    </td>
                    <td>
                      <div className={s.rowActions}>
                        {confirming ? (
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
                            onClick={() => setConfirmingRevoke(row.tokenHash)}
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
    </section>
  );
}

/** Admin allowlist management — add and remove OAuth admin members (email / GitHub / GitLab ids). */
import { useCallback, useEffect, useState } from "react";
import { Button, Placard, Spinner } from "../components/ui";
import { useAdmin } from "../lib/admin";
import { toErrorMessage } from "../lib/errors";
import type { AdminMember } from "../lib/types";
import s from "./AdminsPage.module.css";

export function AdminsPage() {
  const { client } = useAdmin();
  const [admins, setAdmins] = useState<AdminMember[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [email, setEmail] = useState("");
  const [githubId, setGithubId] = useState("");
  const [busy, setBusy] = useState(false);
  const [formError, setFormError] = useState<string | null>(null);
  const [confirmEmail, setConfirmEmail] = useState<string | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      setAdmins(await client.adminsList());
    } catch (e) {
      setError(toErrorMessage(e));
    } finally {
      setLoading(false);
    }
  }, [client]);

  useEffect(() => {
    void load();
  }, [load]);

  async function add() {
    setFormError(null);
    const trimmed = email.trim();
    if (!trimmed) {
      setFormError("email is required");
      return;
    }
    const gh = githubId.trim();
    setBusy(true);
    try {
      await client.addAdmin(trimmed, gh ? Number(gh) : undefined);
      setEmail("");
      setGithubId("");
      await load();
    } catch (e) {
      setFormError(toErrorMessage(e));
    } finally {
      setBusy(false);
    }
  }

  async function remove(addr: string) {
    setBusy(true);
    setFormError(null);
    try {
      await client.removeAdmin(addr);
      setConfirmEmail(null);
      await load();
    } catch (e) {
      setFormError(toErrorMessage(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <section className={s.page}>
      <h1 className={s.title}>Admin allowlist</h1>
      <Placard>Who can open this console · server-wide</Placard>

      <section className={s.add}>
        <div className={s.row}>
          <input
            className={s.input}
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            placeholder="email"
            spellCheck={false}
          />
          <input
            className={`${s.input} ${s.short}`}
            value={githubId}
            onChange={(e) => setGithubId(e.target.value)}
            placeholder="github id (optional)"
            spellCheck={false}
          />
          <Button variant="primary" onClick={add} disabled={busy}>
            add
          </Button>
        </div>
        {formError && <p className={s.error}>{formError}</p>}
      </section>

      {loading ? (
        <Spinner label="loading admins" />
      ) : error ? (
        <p className={s.error}>{error}</p>
      ) : admins.length === 0 ? (
        <p className={s.empty}>No allowlisted admins.</p>
      ) : (
        <div className={s.tableWrap}>
          <table className={s.table}>
            <thead>
              <tr>
                <th>email</th>
                <th>github id</th>
                <th aria-label="actions"></th>
              </tr>
            </thead>
            <tbody>
              {admins.map((a) => (
                <tr key={a.email}>
                  <td className={s.email}>{a.email}</td>
                  <td className="tnum">{a.githubId ?? "—"}</td>
                  <td className={s.actions}>
                    {confirmEmail === a.email ? (
                      <>
                        <button
                          type="button"
                          className={s.linkDanger}
                          onClick={() => remove(a.email)}
                          disabled={busy}
                        >
                          confirm remove
                        </button>
                        <button
                          type="button"
                          className={s.link}
                          onClick={() => setConfirmEmail(null)}
                        >
                          cancel
                        </button>
                      </>
                    ) : (
                      <button
                        type="button"
                        className={s.linkDanger}
                        onClick={() => setConfirmEmail(a.email)}
                      >
                        remove
                      </button>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

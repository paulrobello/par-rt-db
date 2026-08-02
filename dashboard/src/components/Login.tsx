import { useState } from "react";
import { useSession } from "../lib/session";
import { Button, Field } from "./ui";
import s from "./Login.module.css";

export function Login() {
  const { signInWithAdminKey, signInWithOAuth, error } = useSession();
  const [key, setKey] = useState("");
  const [busy, setBusy] = useState(false);
  const [localError, setLocalError] = useState<string | null>(null);

  async function submitKey() {
    if (!key.trim()) return;
    setBusy(true);
    setLocalError(null);
    try {
      await signInWithAdminKey(key.trim());
    } catch (err) {
      setLocalError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  }

  async function oauth(provider: "github" | "google" | "gitlab" | "oidc") {
    setBusy(true);
    setLocalError(null);
    try {
      await signInWithOAuth(provider);
    } catch (err) {
      setLocalError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  }

  const shownError = localError ?? error;

  return (
    <div className={s.screen}>
      <form
        className={s.card}
        onSubmit={(e) => {
          e.preventDefault();
          void submitKey();
        }}
      >
        <h1 className={s.title}>par-rt-db</h1>
        <p className={s.sub}>operator console</p>
        <Field
          label="Admin key"
          value={key}
          onChange={setKey}
          secret
          mono
          placeholder="RTDB_ADMIN_KEY"
        />
        <Button variant="primary" type="submit" disabled={busy}>
          Sign in with admin key
        </Button>
        <div className={s.divider}>
          <span>or</span>
        </div>
        <Button onClick={() => oauth("github")} disabled={busy}>
          Sign in with GitHub
        </Button>
        <Button onClick={() => oauth("google")} disabled={busy}>
          Sign in with Google
        </Button>
        <Button onClick={() => oauth("gitlab")} disabled={busy}>
          Sign in with GitLab
        </Button>
        <Button onClick={() => oauth("oidc")} disabled={busy}>
          Sign in with OIDC
        </Button>
        {shownError && <p className={s.error}>{shownError}</p>}
        <p className={s.note}>
          Admin key covers the control plane. An allowlisted OAuth sign-in also enables the live
          data browser.
        </p>
      </form>
    </div>
  );
}

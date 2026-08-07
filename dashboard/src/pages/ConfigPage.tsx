import { useEffect, useState } from "react";
import { Button, Placard, Spinner, StatusLamp } from "../components/ui";
import { useAdmin } from "../lib/admin";
import { formatBytes } from "../lib/format";
import type { ConfigResponse } from "../lib/types";
import s from "./ConfigPage.module.css";

export function ConfigPage() {
  const { client } = useAdmin();
  const [cfg, setCfg] = useState<ConfigResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [origins, setOrigins] = useState("");
  const [ttl, setTtl] = useState("");
  const [maxSize, setMaxSize] = useState("");
  const [dedupTtl, setDedupTtl] = useState("");
  const [maxTables, setMaxTables] = useState("");
  const [maxStorage, setMaxStorage] = useState("");
  const [maxSubs, setMaxSubs] = useState("");
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [savedAt, setSavedAt] = useState(false);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    client
      .getConfig()
      .then((c) => {
        if (cancelled) return;
        setCfg(c);
        setOrigins(c.hot.allowedOrigins.join("\n"));
        setTtl(String(c.hot.sessionTtlDays));
        setMaxSize(String(c.hot.maxFileSize));
        setDedupTtl(String(c.hot.idempotencyTtlMs));
        setMaxTables(String(c.hot.maxTablesPerDb));
        setMaxStorage(String(c.hot.maxStorageBytesPerDb));
        setMaxSubs(String(c.hot.maxSubsPerDb));
      })
      .catch((e) => {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [client]);

  async function save() {
    setSaving(true);
    setSaveError(null);
    setSavedAt(false);
    const sessionTtlDays = Number(ttl);
    const maxFileSize = Number(maxSize);
    const idempotencyTtlMs = Number(dedupTtl);
    const maxTablesPerDb = Number(maxTables);
    const maxStorageBytesPerDb = Number(maxStorage);
    const maxSubsPerDb = Number(maxSubs);
    if (!Number.isFinite(sessionTtlDays) || sessionTtlDays < 0) {
      setSaveError("session TTL must be a non-negative number of days");
      setSaving(false);
      return;
    }
    if (!Number.isFinite(maxFileSize) || maxFileSize < 0) {
      setSaveError("max file size must be a non-negative number of bytes");
      setSaving(false);
      return;
    }
    if (!Number.isFinite(idempotencyTtlMs) || idempotencyTtlMs <= 0) {
      setSaveError("idempotency TTL must be a positive number of milliseconds");
      setSaving(false);
      return;
    }
    if (!Number.isFinite(maxTablesPerDb) || maxTablesPerDb < 0) {
      setSaveError("max tables per db must be a non-negative integer (0 = unlimited)");
      setSaving(false);
      return;
    }
    if (!Number.isFinite(maxStorageBytesPerDb) || maxStorageBytesPerDb < 0) {
      setSaveError("max storage per db must be a non-negative byte count (0 = unlimited)");
      setSaving(false);
      return;
    }
    if (!Number.isFinite(maxSubsPerDb) || maxSubsPerDb < 0) {
      setSaveError("max subs per db must be a non-negative integer (0 = unlimited)");
      setSaving(false);
      return;
    }
    try {
      const c = await client.patchConfig({
        allowedOrigins: origins
          .split("\n")
          .map((o) => o.trim())
          .filter(Boolean),
        sessionTtlDays,
        maxFileSize,
        idempotencyTtlMs,
        maxTablesPerDb,
        maxStorageBytesPerDb,
        maxSubsPerDb,
      });
      setCfg(c);
      setOrigins(c.hot.allowedOrigins.join("\n"));
      setTtl(String(c.hot.sessionTtlDays));
      setMaxSize(String(c.hot.maxFileSize));
      setDedupTtl(String(c.hot.idempotencyTtlMs));
      setMaxTables(String(c.hot.maxTablesPerDb));
      setMaxStorage(String(c.hot.maxStorageBytesPerDb));
      setMaxSubs(String(c.hot.maxSubsPerDb));
      setSavedAt(true);
    } catch (e) {
      setSaveError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  }

  if (loading) return <Spinner label="loading config" />;
  if (error) return <p className={s.error}>{error}</p>;
  if (!cfg) return null;

  return (
    <section className={s.page}>
      <h1 className={s.title}>Configuration</h1>

      <section className={s.block}>
        <Placard>Hot knobs · runtime-mutable, no restart</Placard>
        <div className={s.form}>
          <label className={s.field}>
            <span className={s.fieldLabel}>allowed origins</span>
            <textarea
              className={s.textarea}
              value={origins}
              onChange={(e) => setOrigins(e.target.value)}
              spellCheck={false}
              rows={Math.max(2, origins.split("\n").length)}
              placeholder="https://app.example.com"
            />
            <span className={s.hint}>one origin per line · empty list allows all</span>
          </label>
          <div className={s.row}>
            <label className={s.field}>
              <span className={s.fieldLabel}>session ttl (days)</span>
              <input
                className={s.input}
                value={ttl}
                onChange={(e) => setTtl(e.target.value)}
                spellCheck={false}
              />
            </label>
            <label className={s.field}>
              <span className={s.fieldLabel}>max file size (bytes)</span>
              <input
                className={s.input}
                value={maxSize}
                onChange={(e) => setMaxSize(e.target.value)}
                spellCheck={false}
              />
              <span className={s.hint}>{formatBytes(Number(maxSize) || 0)}</span>
            </label>
          </div>
          <label className={s.field}>
            <span className={s.fieldLabel}>idempotency ttl (ms)</span>
            <input
              className={s.input}
              value={dedupTtl}
              onChange={(e) => setDedupTtl(e.target.value)}
              spellCheck={false}
            />
            <span className={s.hint}>
              mutation dedup window in milliseconds (default 300000 = 5 min)
            </span>
          </label>
          <div className={s.row}>
            <label className={s.field}>
              <span className={s.fieldLabel}>max tables / db</span>
              <input
                className={s.input}
                value={maxTables}
                onChange={(e) => setMaxTables(e.target.value)}
                spellCheck={false}
              />
              <span className={s.hint}>0 = unlimited</span>
            </label>
            <label className={s.field}>
              <span className={s.fieldLabel}>max storage / db (bytes)</span>
              <input
                className={s.input}
                value={maxStorage}
                onChange={(e) => setMaxStorage(e.target.value)}
                spellCheck={false}
              />
              <span className={s.hint}>{formatBytes(Number(maxStorage) || 0)}</span>
            </label>
            <label className={s.field}>
              <span className={s.fieldLabel}>max subs / db</span>
              <input
                className={s.input}
                value={maxSubs}
                onChange={(e) => setMaxSubs(e.target.value)}
                spellCheck={false}
              />
              <span className={s.hint}>0 = unlimited</span>
            </label>
          </div>
          <div className={s.actions}>
            <Button variant="primary" onClick={save} disabled={saving}>
              {saving ? "saving…" : "save"}
            </Button>
            {savedAt && <span className={s.saved}>applied live</span>}
            {saveError && <span className={s.error}>{saveError}</span>}
          </div>
        </div>
      </section>

      <section className={s.block}>
        <Placard>Server</Placard>
        <dl className={s.spec}>
          <Spec k="version" v={cfg.version} />
          <Spec k="commit" v={cfg.gitCommit.slice(0, 12)} />
          <Spec k="public url" v={cfg.publicUrl || "—"} />
          <Spec k="port" v={String(cfg.port)} />
          <Spec k="github api" v={cfg.githubApiUrl} />
        </dl>
      </section>

      <section className={s.block}>
        <Placard>Configured providers</Placard>
        <div className={s.providers}>
          <ProviderRow label="database url" on={cfg.databaseUrlConfigured} />
          <ProviderRow label="admin key" on={cfg.adminKeyConfigured} />
          <ProviderRow label="github oauth" on={cfg.githubConfigured} />
          <ProviderRow label="google oauth" on={cfg.googleConfigured} />
          <ProviderRow label="gitlab oauth" on={cfg.gitlabConfigured} />
        </div>
      </section>
    </section>
  );
}

function Spec({ k, v }: { k: string; v: string }) {
  return (
    <div className={s.specRow}>
      <dt className={s.specKey}>{k}</dt>
      <dd className={s.specVal}>{v}</dd>
    </div>
  );
}

function ProviderRow({ label, on }: { label: string; on: boolean }) {
  return (
    <div className={s.provider}>
      <StatusLamp status={on ? "ok" : "idle"} />
      <span className={s.providerLabel}>{label}</span>
      <span className={s.providerState}>{on ? "set" : "unset"}</span>
    </div>
  );
}

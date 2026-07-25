/**
 * Scaffold shell — verifies the Instrument Manual token world renders and the
 * Vite/bun toolchain is wired. Replaced by the real app shell (routing, auth
 * gate, command/live rails) in the next task.
 */
export function App() {
  return (
    <div className="console">
      <header className="topbar">
        <span className="brand mono">par-rt-db</span>
        <span className="topbar__spacer" />
        <span className="conn" title="connection status">
          <span className="conn__dot" />
          <span className="conn__label mono">offline</span>
        </span>
      </header>
      <div className="console__body">
        <aside className="rail">
          <p className="placard">Databases</p>
          <p className="rail__empty mono">— none loaded —</p>
        </aside>
        <main className="surface">
          <p className="placard">Console</p>
          <h1 className="surface__title">Instrument console</h1>
          <p className="surface__lede">
            Dashboard scaffolding is live. The data browser, schema, metrics, op feed, and config
            surfaces arrive next, in the same language.
          </p>
        </main>
      </div>
    </div>
  );
}

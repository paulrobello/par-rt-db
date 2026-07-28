import { BrowserRouter, Route, Routes } from "react-router-dom";
import { Spinner } from "./components/ui";
import { Login } from "./components/Login";
import { AdminProvider } from "./lib/admin";
import { SessionProvider, useSession } from "./lib/session";
import { DbPage } from "./pages/DbPage";
import { DatabasesPage } from "./pages/DatabasesPage";
import { SchemaPage } from "./pages/SchemaPage";
import { AdminsPage } from "./pages/AdminsPage";
import { ConfigPage } from "./pages/ConfigPage";
import { DataBrowserPage } from "./pages/DataBrowserPage";
import { MetricsPage } from "./pages/MetricsPage";
import { OpsPage } from "./pages/OpsPage";
import { QueryConsolePage } from "./pages/QueryConsolePage";
import { NotFound } from "./routes";
import { AppShell } from "./shell/AppShell";

function Root() {
  const { method, loading } = useSession();
  if (loading) {
    return (
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          height: "100dvh",
        }}
      >
        <Spinner label="restoring session" />
      </div>
    );
  }
  if (!method) return <Login />;
  return (
    <AdminProvider>
      <BrowserRouter>
        <Routes>
          <Route element={<AppShell />}>
            <Route index element={<DatabasesPage />} />
            <Route path="dbs/:db" element={<DbPage />} />
            <Route path="dbs/:db/schema" element={<SchemaPage />} />
            <Route path="dbs/:db/tables/:table" element={<DataBrowserPage />} />
            <Route path="metrics" element={<MetricsPage />} />
            <Route path="ops" element={<OpsPage />} />
            <Route path="console" element={<QueryConsolePage />} />
            <Route path="config" element={<ConfigPage />} />
            <Route path="admins" element={<AdminsPage />} />
            <Route path="*" element={<NotFound />} />
          </Route>
        </Routes>
      </BrowserRouter>
    </AdminProvider>
  );
}

export function App() {
  return (
    <SessionProvider>
      <Root />
    </SessionProvider>
  );
}

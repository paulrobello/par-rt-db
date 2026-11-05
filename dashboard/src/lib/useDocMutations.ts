/**
 * The data browser's write surface: the shared `adminMutate` runner (error and
 * busy state, live-table refresh on success), the patch-by-id composer, and the
 * two-step delete confirm. Extracted from DataBrowserPage (QA-007) so the page
 * renders and this hook owns every mutation flow.
 */

import type { RtDbAdminClient, TransactionJson } from "@par-rt-db/client";
import { useState } from "react";
import { formatWriteError } from "./errors";

export function useDocMutations(
  client: RtDbAdminClient,
  db: string,
  table: string,
  refresh: () => Promise<void> | void,
) {
  const [mutError, setMutError] = useState<string | null>(null);
  const [mutBusy, setMutBusy] = useState(false);
  // Patch-by-id composer (the toolbar "Patch" button).
  const [patchOpen, setPatchOpen] = useState(false);
  const [patchId, setPatchId] = useState("");
  const [patchFields, setPatchFields] = useState("{}");
  // Inline two-step confirm for row deletes: set = awaiting confirmation.
  const [confirmId, setConfirmId] = useState<string | null>(null);

  async function mutate(steps: TransactionJson["steps"]): Promise<boolean> {
    setMutBusy(true);
    setMutError(null);
    try {
      await client.adminMutate(db, { steps });
      await refresh();
      return true;
    } catch (e) {
      setMutError(formatWriteError(e));
      return false;
    } finally {
      setMutBusy(false);
    }
  }

  async function doPatch() {
    let f: Record<string, unknown>;
    try {
      f = JSON.parse(patchFields);
    } catch {
      setMutError("invalid JSON for fields");
      return;
    }
    if (!patchId.trim()) {
      setMutError("document id is required");
      return;
    }
    if (await mutate([{ op: "patch", table, id: patchId.trim(), fields: f }])) setPatchOpen(false);
  }

  async function doUndelete(id: string) {
    await mutate([{ op: "undelete", table, id }]);
  }

  async function doDelete(id: string) {
    if (await mutate([{ op: "delete", table, id }])) setConfirmId(null);
  }

  return {
    mutate,
    doPatch,
    doUndelete,
    doDelete,
    mutError,
    mutBusy,
    patchOpen,
    setPatchOpen,
    patchId,
    setPatchId,
    patchFields,
    setPatchFields,
    confirmId,
    setConfirmId,
  };
}

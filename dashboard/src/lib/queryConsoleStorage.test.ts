import { beforeEach, describe, expect, it } from "vitest";
import {
  loadRecentQueries,
  loadSavedQueries,
  pushRecentQuery,
  saveRecentQueries,
  saveSavedQueries,
} from "./queryConsoleStorage";

describe("queryConsoleStorage", () => {
  beforeEach(() => localStorage.clear());

  it("round-trips saved queries through localStorage (simulated reload)", () => {
    saveSavedQueries([{ name: "active users", db: "prod", text: '{"table":"users"}' }]);
    expect(loadSavedQueries()).toEqual([
      { name: "active users", db: "prod", text: '{"table":"users"}' },
    ]);
  });

  it("keeps the last 20 distinct queries, most recent first", () => {
    let list: import("./queryConsoleStorage").RecentQuery[] = [];
    for (let i = 0; i < 25; i++) list = pushRecentQuery(list, { db: "d", text: `q${i}` });
    expect(list).toHaveLength(20);
    expect(list[0].text).toBe("q24");
    expect(list[19].text).toBe("q5");
    expect(list.map((q) => q.text)).not.toContain("q0");
  });

  it("dedupes an exact repeat instead of duplicating it", () => {
    let list = pushRecentQuery([], { db: "d", text: "A" });
    list = pushRecentQuery(list, { db: "d", text: "B" });
    list = pushRecentQuery(list, { db: "d", text: "A" });
    expect(list.map((q) => q.text)).toEqual(["A", "B"]);
  });

  it("round-trips the recent list and drops malformed entries", () => {
    saveRecentQueries([{ db: "d", text: "A" }]);
    expect(loadRecentQueries()).toEqual([{ db: "d", text: "A" }]);
    localStorage.setItem("rtdb.queryConsole.saved", '{"not":"an array"}');
    expect(loadSavedQueries()).toEqual([]);
  });
});

import assert from "node:assert/strict";
import test from "node:test";
import { ageInDays, buildSyncStatus } from "./sync-status.ts";

test("ageInDays counts calendar days between Berlin dates", () => {
  assert.equal(ageInDays("2026-07-30", "2026-08-05"), 6);
  assert.equal(ageInDays("2026-08-05", "2026-08-05"), 0);
  assert.equal(ageInDays(null, "2026-08-05"), null);
});

test("fresh orders with stale order_items is warning, not critical", () => {
  const snapshot = buildSyncStatus({
    todayISO: "2026-08-05",
    maxOrderDate: "2026-08-04",
    maxOrderItemDate: "2026-07-30",
    maxInventorySnapshot: "2026-08-05",
    lastEtl: {
      status: "success",
      startedAt: "2026-08-05T05:23:00Z",
      finishedAt: "2026-08-05T05:26:00Z",
      marketplace: "DE",
      periodYear: 2026,
      periodMonth: 8,
    },
  });

  assert.equal(snapshot.sources.find((s) => s.key === "orders")?.stale, false);
  assert.equal(snapshot.sources.find((s) => s.key === "inventory")?.stale, false);
  assert.equal(snapshot.sources.find((s) => s.key === "etl")?.stale, false);
  assert.equal(snapshot.sources.find((s) => s.key === "order_items")?.stale, true);
  assert.equal(snapshot.overall, "warning");
  assert.ok(snapshot.warnings.some((w) => /Order-Items/i.test(w)));
});

test("marks critical when order import itself is stale", () => {
  const snapshot = buildSyncStatus({
    todayISO: "2026-08-05",
    maxOrderDate: "2026-07-30",
    maxOrderItemDate: "2026-07-30",
    maxInventorySnapshot: "2026-08-05",
    lastEtl: {
      status: "error",
      startedAt: "2025-12-19T03:50:16.642837+00",
      finishedAt: null,
      marketplace: "DE",
      periodYear: 2025,
      periodMonth: 12,
    },
  });

  assert.equal(snapshot.overall, "critical");
  assert.equal(snapshot.sources.find((s) => s.key === "orders")?.stale, true);
  assert.equal(snapshot.sources.find((s) => s.key === "etl")?.stale, true);
});

test("overall ok when orders, items, inventory and etl are fresh", () => {
  const snapshot = buildSyncStatus({
    todayISO: "2026-08-05",
    maxOrderDate: "2026-08-05",
    maxOrderItemDate: "2026-08-04",
    maxInventorySnapshot: "2026-08-05",
    lastEtl: {
      status: "success",
      startedAt: "2026-08-05T02:00:00Z",
      finishedAt: "2026-08-05T02:10:00Z",
      marketplace: "DE",
      periodYear: 2026,
      periodMonth: 8,
    },
  });
  assert.equal(snapshot.sources.find((s) => s.key === "order_items")?.stale, false);
  assert.equal(snapshot.overall, "ok");
  assert.equal(snapshot.warnings.length, 0);
});

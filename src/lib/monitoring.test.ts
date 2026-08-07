import assert from "node:assert/strict";
import test from "node:test";
import {
  buildPipelineSeries,
  classifyEtlPipeline,
  lastNBerlinDays,
} from "./monitoring.ts";

test("lastNBerlinDays returns inclusive window ending today", () => {
  assert.deepEqual(lastNBerlinDays("2026-08-06", 3), [
    "2026-08-04",
    "2026-08-05",
    "2026-08-06",
  ]);
});

test("classifyEtlPipeline reads order_items and orders tags", () => {
  assert.equal(
    classifyEtlPipeline({
      status: "success",
      started_at: null,
      finished_at: null,
      marketplace: "DE",
      period_year: 2026,
      period_month: 8,
      run_log: "[order_items] upserted 120",
    }),
    "order_items",
  );
  assert.equal(
    classifyEtlPipeline({
      status: "success",
      started_at: null,
      finished_at: null,
      marketplace: "DE",
      period_year: 2026,
      period_month: 8,
      run_log: "OK via GitHub Actions run 1 (which=orders)",
    }),
    "orders",
  );
});

test("buildPipelineSeries marks success/error/missing per day", () => {
  const dayKeys = ["2026-08-05", "2026-08-06"];
  const series = buildPipelineSeries({
    dayKeys,
    runs: [
      {
        status: "ok",
        started_at: "2026-08-06T02:12:00Z",
        finished_at: "2026-08-06T02:20:00Z",
        marketplace: "DE",
        period_year: 2026,
        period_month: 8,
        run_log: "[orders] ok",
      },
      {
        status: "error",
        started_at: "2026-08-05T02:12:00Z",
        finished_at: "2026-08-05T02:13:00Z",
        marketplace: "DE",
        period_year: 2026,
        period_month: 8,
        run_log: "[orders] failed",
      },
    ],
    inventorySnapshotDates: ["2026-08-06"],
  });

  const orders = series.find((s) => s.key === "orders")!;
  assert.equal(orders.days["2026-08-06"].status, "success");
  assert.equal(orders.days["2026-08-05"].status, "error");
  assert.equal(orders.days["2026-08-05"].runCount, 1);

  const inventory = series.find((s) => s.key === "inventory")!;
  assert.equal(inventory.days["2026-08-06"].status, "success");
  assert.equal(inventory.days["2026-08-05"].status, "missing");
});

test("buildPipelineSeries falls back to order/item data presence", () => {
  const dayKeys = ["2026-08-06", "2026-08-07"];
  const series = buildPipelineSeries({
    dayKeys,
    runs: [],
    orderDataDates: ["2026-08-07"],
    orderItemDataDates: ["2026-08-07"],
    inventorySnapshotDates: ["2026-08-07"],
  });
  assert.equal(series.find((s) => s.key === "orders")!.days["2026-08-07"].status, "success");
  assert.equal(series.find((s) => s.key === "order_items")!.days["2026-08-07"].status, "success");
  assert.equal(series.find((s) => s.key === "orders")!.days["2026-08-06"].status, "missing");
});

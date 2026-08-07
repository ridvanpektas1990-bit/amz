import assert from "node:assert/strict";
import test from "node:test";
import { buildInventoryStockForecast } from "./inventory-stock-forecast.ts";

test("burns amazon daily and keeps total >= amazon", () => {
  const result = buildInventoryStockForecast({
    available: 100,
    inbound: 0,
    localQty: 50,
    forecastDailySales: 10,
    todayISO: "2026-08-07",
    horizonDays: 20,
    recommendedShipQty: 0,
  });
  assert.equal(result.points[0].events.some((e) => e.kind === "today"), true);
  assert.ok(result.points.every((p) => p.total >= p.amazon));
  assert.ok(result.points.every((p) => p.amazon >= 0 && p.local >= 0 && p.total >= 0));
});

test("transfer moves local→amazon after lead without changing total", () => {
  const result = buildInventoryStockForecast({
    available: 30,
    inbound: 0,
    localQty: 100,
    forecastDailySales: 10,
    transferLeadDays: 3,
    recommendedShipQty: 60,
    todayISO: "2026-08-07",
    horizonDays: 30,
  });
  const ship = result.points.find((p) => p.events.some((e) => e.kind === "transfer_ship"));
  assert.ok(ship);
  assert.equal(ship!.dayOffset, 0);
  assert.equal(ship!.local, 40);
  assert.equal(ship!.total, 130);

  const emptyMarker = result.points.find((p) =>
    p.events.some((e) => e.kind === "amazon_empty_without_transfer"),
  );
  assert.ok(emptyMarker);
  assert.match(
    emptyMarker!.events.find((e) => e.kind === "amazon_empty_without_transfer")!.label,
    /ohne Transfer/,
  );
});

test("supplier PO joins local on arrival; total-empty marker ignores PO", () => {
  const result = buildInventoryStockForecast({
    available: 20,
    inbound: 0,
    localQty: 10,
    onOrderUnits: 200,
    onOrderOrderedAt: "2026-08-01",
    supplierLeadDays: 20,
    forecastDailySales: 10,
    todayISO: "2026-08-07",
    horizonDays: 40,
    recommendedShipQty: 0,
  });
  const delivery = result.points.find((p) => p.events.some((e) => e.kind === "supplier_delivery"));
  assert.ok(delivery);
  assert.equal(delivery!.dayOffset, 14);

  const totalEmpty = result.points.find((p) =>
    p.events.some((e) => e.kind === "total_empty_without_po"),
  );
  assert.ok(totalEmpty);
  assert.ok(totalEmpty!.dayOffset < delivery!.dayOffset);
});

test("after PO arrival stock keeps declining via auto-transfer to Amazon", () => {
  const result = buildInventoryStockForecast({
    available: 20,
    inbound: 0,
    localQty: 0,
    onOrderUnits: 300,
    onOrderOrderedAt: "2026-08-01",
    supplierLeadDays: 10,
    transferLeadDays: 2,
    forecastDailySales: 10,
    recommendedShipQty: 100,
    todayISO: "2026-08-07",
    horizonDays: 60,
  });

  const delivery = result.points.find((p) => p.events.some((e) => e.kind === "supplier_delivery"));
  assert.ok(delivery);

  // A few days after PO+transfer lead, Gesamt must keep falling (not flat).
  const after = delivery!.dayOffset + 10;
  const later = delivery!.dayOffset + 25;
  assert.ok(result.points[after].total > result.points[later].total);
  assert.ok(result.points[later].total < 300);
});

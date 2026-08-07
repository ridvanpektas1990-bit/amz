import assert from "node:assert/strict";
import test from "node:test";
import {
  classifyStockStatus,
  daysUntilAmazonShip,
  daysUntilSupplierOrderDeadline,
  filterItemsBySelectedSku,
  formatCoverDaysDe,
  formatInDaysDe,
  inventoryActionHint,
  inventoryStatusLabel,
  selectOosRiskItems,
  summarizeInventoryKpis,
  type InventoryOverviewItem,
} from "./inventory-overview.ts";

function item(partial: Partial<InventoryOverviewItem> & Pick<InventoryOverviewItem, "sku" | "status">): InventoryOverviewItem {
  return {
    asin: partial.asin || `ASIN-${partial.sku}`,
    sku: partial.sku,
    imageUrl: null,
    productName: partial.productName || partial.sku,
    marketplace: "DE",
    snapshotDate: "2026-08-05",
    available: partial.available ?? 0,
    total: partial.total ?? partial.available ?? 0,
    reserved: 0,
    pendingCustomerOrders: 0,
    inbound: partial.inbound ?? 0,
    localQty: partial.localQty ?? 0,
    onOrderUnits: partial.onOrderUnits ?? 0,
    transferLeadDays: partial.transferLeadDays ?? 7,
    units30: partial.units30 ?? 0,
    units90: partial.units90 ?? 0,
    dailySales30: partial.dailySales30 ?? 0,
    forecastDailySales: partial.forecastDailySales ?? 0,
    forecastMethod: "recent",
    growthFactor: 1,
    growthPercent: 0,
    comparisonCurrentUnits: partial.comparisonCurrentUnits ?? 0,
    comparisonPreviousUnits: partial.comparisonPreviousUnits ?? 0,
    daysOfCover: partial.daysOfCover ?? null,
    estimatedOosDate: partial.estimatedOosDate ?? null,
    daysOfCoverOnHand: partial.daysOfCoverOnHand ?? partial.daysOfCover ?? null,
    estimatedOosDateOnHand: partial.estimatedOosDateOnHand ?? partial.estimatedOosDate ?? null,
    daysOfCoverAmazonAndLocal:
      partial.daysOfCoverAmazonAndLocal ?? partial.daysOfCoverWithLocal ?? partial.daysOfCover ?? null,
    estimatedOosDateAmazonAndLocal:
      partial.estimatedOosDateAmazonAndLocal ??
      partial.estimatedOosDateWithLocal ??
      partial.estimatedOosDate ??
      null,
    daysOfCoverWithLocal: partial.daysOfCoverWithLocal ?? partial.daysOfCover ?? null,
    estimatedOosDateWithLocal: partial.estimatedOosDateWithLocal ?? partial.estimatedOosDate ?? null,
    supplierLeadDays: partial.supplierLeadDays ?? null,
    status: partial.status,
  };
}

test("KPI summary aggregates sales, stock, risk and growth", () => {
  const kpis = summarizeInventoryKpis([
    item({ sku: "A", status: "out", units30: 10, available: 0, comparisonCurrentUnits: 120, comparisonPreviousUnits: 100 }),
    item({ sku: "B", status: "critical", units30: 20, available: 40, daysOfCover: 12, comparisonCurrentUnits: 80, comparisonPreviousUnits: 100 }),
    item({ sku: "C", status: "healthy", units30: 5, available: 200, daysOfCover: 90 }),
  ]);
  assert.equal(kpis.units30, 35);
  assert.equal(kpis.available, 240);
  assert.equal(kpis.atRisk, 2);
  assert.equal(kpis.out, 1);
  assert.equal(kpis.critical, 1);
  assert.equal(kpis.growthPercent, 0); // 200 vs 200
});

test("OOS risk list includes only out/critical, prioritized by cover then sales", () => {
  const ranked = selectOosRiskItems([
    item({ sku: "warn", status: "warning", daysOfCover: 40, units30: 100 }),
    item({ sku: "crit-slow", status: "critical", daysOfCover: 20, units30: 5 }),
    item({ sku: "crit-fast", status: "critical", daysOfCover: 8, units30: 50 }),
    item({ sku: "out", status: "out", daysOfCover: 0, units30: 10 }),
    item({ sku: "ok", status: "healthy", daysOfCover: 120, units30: 200 }),
  ]);
  assert.deepEqual(
    ranked.map((row) => row.sku),
    ["out", "crit-fast", "crit-slow"],
  );
});

test("filterItemsBySelectedSku scopes KPIs to the ASIN of the chosen SKU", () => {
  const items = [
    item({ sku: "A1", asin: "ASIN-X", status: "healthy", units30: 10, available: 5 }),
    item({ sku: "A2", asin: "ASIN-X", status: "critical", units30: 20, available: 2, daysOfCover: 10 }),
    item({ sku: "B1", asin: "ASIN-Y", status: "out", units30: 50, available: 0 }),
  ];
  const scoped = filterItemsBySelectedSku(items, "A1");
  assert.deepEqual(
    scoped.map((row) => row.sku).sort(),
    ["A1", "A2"],
  );
  assert.equal(summarizeInventoryKpis(scoped).units30, 30);
});

test("cover action KPIs: ship and supplier deadlines from cover minus lead", () => {
  const row = item({
    sku: "cover",
    status: "healthy",
    localQty: 100,
    transferLeadDays: 7,
    supplierLeadDays: 40,
    daysOfCover: 31,
    daysOfCoverAmazonAndLocal: 75,
  });
  assert.equal(daysUntilAmazonShip(row), 24);
  assert.equal(daysUntilSupplierOrderDeadline(row), 35);
  assert.equal(formatCoverDaysDe(24), "24 Tage");
  assert.equal(formatInDaysDe(9), "in 9 Tagen");
  assert.equal(formatInDaysDe(0), "jetzt");
});

test("cover action KPIs skip ship without local and order with open PO", () => {
  assert.equal(
    daysUntilAmazonShip(item({ sku: "nolocal", status: "healthy", daysOfCover: 40, localQty: 0 })),
    null,
  );
  assert.equal(
    daysUntilSupplierOrderDeadline(
      item({
        sku: "po",
        status: "healthy",
        supplierLeadDays: 40,
        daysOfCoverAmazonAndLocal: 80,
        onOrderUnits: 200,
      }),
    ),
    null,
  );
});

test("action hints are actionable for OOS and short cover", () => {
  assert.equal(inventoryActionHint(item({ sku: "x", status: "out" })), "Jetzt bestellen");
  assert.match(
    inventoryActionHint(item({ sku: "y", status: "critical", daysOfCover: 10 })),
    /Jetzt bestellen/,
  );
});

test("inbound extends cover and softens status / hints", () => {
  assert.equal(classifyStockStatus(0, 0, 0), "out");
  assert.equal(classifyStockStatus(0, 100, 45), "warning");
  assert.equal(classifyStockStatus(10, 200, 90), "healthy");

  const coveredByInbound = item({
    sku: "inb",
    status: "healthy",
    available: 5,
    inbound: 200,
    daysOfCover: 80,
    daysOfCoverOnHand: 8,
  });
  assert.match(inventoryActionHint(coveredByInbound), /Kein Nachschub nötig/);
  assert.equal(inventoryStatusLabel(item({ sku: "z", status: "warning", available: 0, inbound: 40 }), "Beobachten"), "Zulauf");
});

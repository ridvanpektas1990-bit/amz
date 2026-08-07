import assert from "node:assert/strict";
import test from "node:test";
import { buildReorderBoardRows, timingLabel } from "./reorder-board.ts";
import type { InventoryOverviewItem } from "./inventory-overview.ts";

function item(
  partial: Partial<InventoryOverviewItem> & Pick<InventoryOverviewItem, "sku" | "status">,
): InventoryOverviewItem {
  return {
    asin: partial.asin || `ASIN-${partial.sku}`,
    sku: partial.sku,
    imageUrl: null,
    productName: partial.productName || partial.sku,
    marketplace: "DE",
    snapshotDate: "2026-08-06",
    available: partial.available ?? 10,
    total: partial.total ?? 10,
    reserved: 0,
    pendingCustomerOrders: 0,
    inbound: partial.inbound ?? 0,
    localQty: partial.localQty ?? 0,
    onOrderUnits: partial.onOrderUnits ?? 0,
    onOrderOrderedAt: partial.onOrderOrderedAt ?? null,
    transferLeadDays: partial.transferLeadDays ?? 7,
    units30: partial.units30 ?? 30,
    units90: partial.units90 ?? 90,
    dailySales30: partial.dailySales30 ?? 1,
    forecastDailySales: partial.forecastDailySales ?? 1,
    forecastMethod: "recent",
    growthFactor: 1,
    growthPercent: 0,
    comparisonCurrentUnits: 0,
    comparisonPreviousUnits: 0,
    daysOfCover: partial.daysOfCover ?? 40,
    estimatedOosDate: null,
    daysOfCoverOnHand: partial.daysOfCoverOnHand ?? partial.daysOfCover ?? 40,
    estimatedOosDateOnHand: null,
    daysOfCoverAmazonAndLocal: partial.daysOfCoverAmazonAndLocal ?? null,
    estimatedOosDateAmazonAndLocal: null,
    daysOfCoverWithLocal: partial.daysOfCoverWithLocal ?? null,
    estimatedOosDateWithLocal: null,
    supplierLeadDays: partial.supplierLeadDays ?? null,
    recommendedShipQty: partial.recommendedShipQty ?? null,
    recommendedOrderQty: partial.recommendedOrderQty ?? null,
    status: partial.status,
  };
}

test("board includes due-soon SKUs with carton rounding and supplier text", () => {
  const specs = new Map([
    [
      "A",
      {
        productionTimeDays: 10,
        shippingTimeDays: 5,
        bufferTimeDays: 0,
        unitsPerCarton: 32,
      },
    ],
  ]);
  // lead 15, cover Amazon+local 10 → too_late; LY qty precomputed 30 → round to 32
  const rows = buildReorderBoardRows(
    [
      item({
        sku: "A",
        status: "critical",
        daysOfCover: 10,
        daysOfCoverAmazonAndLocal: 10,
        forecastDailySales: 2,
        recommendedOrderQty: 30,
      }),
    ],
    specs as never,
  );
  assert.equal(rows.length, 1);
  assert.equal(rows[0].timing?.status, "too_late");
  assert.equal(rows[0].rawQty, 30);
  assert.equal(rows[0].orderQty, 32);
  assert.equal(rows[0].cartons, 1);
  assert.equal(rows[0].action, "order_supplier");
  assert.match(rows[0].supplierMessage || "", /Stückzahl pro Karton: 32 Stück/);
  assert.match(rows[0].supplierMessage || "", /Anzahl Produkte insgesamt: 32 Stück/);
  assert.match(rows[0].supplierMessage || "", /Neue Bestellung/);
  assert.doesNotMatch(rows[0].supplierMessage || "", /Lieferzeit/);
});

test("local stock triggers Amazon replenish instead of supplier order", () => {
  const specs = new Map([
    [
      "L",
      {
        productionTimeDays: 30,
        shippingTimeDays: 30,
        bufferTimeDays: 0,
        unitsPerCarton: 10,
      },
    ],
  ]);
  const rows = buildReorderBoardRows(
    [
      item({
        sku: "L",
        status: "critical",
        daysOfCover: 3,
        daysOfCoverAmazonAndLocal: 400,
        forecastDailySales: 5,
        localQty: 2000,
        transferLeadDays: 7,
      }),
    ],
    specs as never,
  );
  assert.equal(rows.length, 1);
  assert.equal(rows[0].action, "replenish_amazon");
  assert.equal(rows[0].supplierMessage, null);
});

test("critical SKU without lead time appears as missing stammdaten", () => {
  const rows = buildReorderBoardRows(
    [item({ sku: "B", status: "critical", daysOfCover: 5, available: 5 })],
    new Map(),
  );
  assert.equal(rows.length, 1);
  assert.equal(rows[0].missingLeadTime, true);
  assert.equal(timingLabel(rows[0].timing, true), "Stammdaten fehlen");
});

test("healthy long-cover SKU is omitted", () => {
  const specs = new Map([
    ["C", { productionTimeDays: 5, shippingTimeDays: 5, bufferTimeDays: 0, unitsPerCarton: 10 }],
  ]);
  const rows = buildReorderBoardRows(
    [item({ sku: "C", status: "healthy", daysOfCover: 120, forecastDailySales: 1 })],
    specs as never,
  );
  assert.equal(rows.length, 0);
});

test("includeAllActive lists healthy SKUs for early order", () => {
  const specs = new Map([
    ["C", { productionTimeDays: 5, shippingTimeDays: 5, bufferTimeDays: 0, unitsPerCarton: 10 }],
  ]);
  const rows = buildReorderBoardRows(
    [
      item({
        sku: "C",
        status: "healthy",
        daysOfCover: 120,
        daysOfCoverAmazonAndLocal: 120,
        forecastDailySales: 1,
        recommendedOrderQty: 20,
      }),
    ],
    specs as never,
    { includeAllActive: true },
  );
  assert.equal(rows.length, 1);
  assert.equal(rows[0].action, "ok");
  assert.equal(rows[0].orderQty, 20);
  assert.match(rows[0].supplierMessage || "", /Neue Bestellung/);
});

test("open PO zeroing net qty still enables supplier copy text", () => {
  const specs = new Map([
    [
      "UI-JKHV-J3CU",
      {
        productionTimeDays: 60,
        shippingTimeDays: 35,
        bufferTimeDays: 0,
        unitsPerCarton: 48,
      },
    ],
  ]);
  const rows = buildReorderBoardRows(
    [
      item({
        sku: "UI-JKHV-J3CU",
        status: "healthy",
        available: 231,
        inbound: 480,
        localQty: 672,
        onOrderUnits: 3120,
        onOrderOrderedAt: "2026-06-25",
        transferLeadDays: 9,
        supplierLeadDays: 95,
        daysOfCover: 24,
        daysOfCoverAmazonAndLocal: 47,
        daysOfCoverWithLocal: 198,
        units30: 100,
        dailySales30: 3,
        forecastDailySales: 3,
        recommendedOrderQty: null,
      }),
    ],
    specs as never,
  );
  assert.equal(rows.length, 1);
  assert.equal(rows[0].orderQty, 0);
  assert.ok(rows[0].messageOrderQty > 0);
  assert.match(rows[0].supplierMessage || "", /Neue Bestellung/);
  assert.match(
    rows[0].supplierMessage || "",
    new RegExp(`Anzahl Produkte insgesamt: ${rows[0].messageOrderQty} Stück`),
  );
});

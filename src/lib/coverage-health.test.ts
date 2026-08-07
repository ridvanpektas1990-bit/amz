import assert from "node:assert/strict";
import test from "node:test";
import {
  amazonShipActionLabel,
  classifyCoverageHealth,
  coverageHealthFromOverviewItem,
  supplierOrderActionLabel,
} from "./coverage-health.ts";
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
    snapshotDate: "2026-08-05",
    available: partial.available ?? 100,
    total: partial.total ?? partial.available ?? 100,
    reserved: 0,
    pendingCustomerOrders: 0,
    inbound: partial.inbound ?? 0,
    localQty: partial.localQty ?? 0,
    onOrderUnits: partial.onOrderUnits ?? 0,
    transferLeadDays: partial.transferLeadDays ?? 7,
    onOrderOrderedAt: partial.onOrderOrderedAt ?? null,
    supplierLeadDays: partial.supplierLeadDays ?? 40,
    units30: partial.units30 ?? 30,
    units90: partial.units90 ?? 90,
    dailySales30: partial.dailySales30 ?? 1,
    forecastDailySales: partial.forecastDailySales ?? 1,
    forecastMethod: "recent",
    growthFactor: 1,
    growthPercent: 0,
    comparisonCurrentUnits: 0,
    comparisonPreviousUnits: 0,
    daysOfCover: partial.daysOfCover ?? null,
    estimatedOosDate: null,
    daysOfCoverOnHand: partial.daysOfCoverOnHand ?? partial.daysOfCover ?? null,
    estimatedOosDateOnHand: null,
    daysOfCoverAmazonAndLocal:
      partial.daysOfCoverAmazonAndLocal ?? partial.daysOfCoverWithLocal ?? partial.daysOfCover ?? null,
    estimatedOosDateAmazonAndLocal: null,
    daysOfCoverWithLocal: partial.daysOfCoverWithLocal ?? partial.daysOfCover ?? null,
    estimatedOosDateWithLocal: null,
    status: partial.status,
  };
}

test("short Amazon + local stock is Ins Amz Lager senden, not Stockout", () => {
  const result = classifyCoverageHealth({
    amazonAvailable: 24,
    localQty: 672,
    onOrderUnits: 3120,
    amazonDaysOfCover: 24,
    amazonAndLocalDaysOfCover: 700,
    stockAction: "replenish_amazon",
    reorderTiming: {
      daysUntilOos: 700,
      leadDays: 40,
      daysUntilMustOrder: 660,
      status: "ok",
    },
    daysUntilShip: -6,
    daysUntilOrder: null,
    deliveryGapDays: -500,
  });
  assert.equal(result.health, "ship_to_amazon");
  assert.equal(result.shortLabel, "Ins Amz Lager senden");
  assert.match(result.label, /Ins Amz Lager senden/);
});

test("pipeline ok stays Abgedeckt", () => {
  const result = classifyCoverageHealth({
    amazonAvailable: 80,
    localQty: 100,
    onOrderUnits: 0,
    amazonDaysOfCover: 80,
    amazonAndLocalDaysOfCover: 120,
    stockAction: "ok",
    reorderTiming: {
      daysUntilOos: 120,
      leadDays: 40,
      daysUntilMustOrder: 80,
      status: "ok",
    },
    daysUntilShip: 60,
    daysUntilOrder: 80,
  });
  assert.equal(result.health, "covered");
  assert.equal(result.shortLabel, "Abgedeckt");
});

test("sold out when no Amazon, local, or open PO", () => {
  const result = classifyCoverageHealth({
    amazonAvailable: 0,
    amazonInbound: 0,
    localQty: 0,
    onOrderUnits: 0,
    amazonDaysOfCover: 0,
    amazonAndLocalDaysOfCover: 0,
    stockAction: "order_supplier",
    reorderTiming: {
      daysUntilOos: 0,
      leadDays: 40,
      daysUntilMustOrder: -40,
      status: "already_oos",
    },
  });
  assert.equal(result.health, "sold_out");
  assert.equal(result.shortLabel, "Ausverkauft");
});

test("delivery gap marks Stockout-Risiko", () => {
  const result = classifyCoverageHealth({
    amazonAvailable: 10,
    localQty: 0,
    onOrderUnits: 100,
    amazonDaysOfCover: 10,
    amazonAndLocalDaysOfCover: 10,
    stockAction: "order_supplier",
    reorderTiming: {
      daysUntilOos: 10,
      leadDays: 40,
      daysUntilMustOrder: -30,
      status: "too_late",
    },
    deliveryGapDays: 30,
  });
  assert.equal(result.health, "stockout_risk");
  assert.equal(result.shortLabel, "Stockout-Risiko");
});

test("too_late without delivery gap is Produkt nachbestellen", () => {
  const result = classifyCoverageHealth({
    amazonAvailable: 20,
    localQty: 0,
    amazonDaysOfCover: 20,
    amazonAndLocalDaysOfCover: 20,
    stockAction: "order_supplier",
    reorderTiming: {
      daysUntilOos: 20,
      leadDays: 40,
      daysUntilMustOrder: -20,
      status: "too_late",
    },
    deliveryGapDays: null,
    daysUntilOrder: -20,
  });
  assert.equal(result.health, "reorder_product");
  assert.equal(result.shortLabel, "Produkt nachbestellen");
});

test("order_supplier with buffer is Produkt nachbestellen", () => {
  const result = classifyCoverageHealth({
    amazonAvailable: 50,
    localQty: 0,
    amazonDaysOfCover: 50,
    amazonAndLocalDaysOfCover: 50,
    stockAction: "order_supplier",
    reorderTiming: {
      daysUntilOos: 50,
      leadDays: 40,
      daysUntilMustOrder: 10,
      status: "ok",
    },
    daysUntilOrder: 10,
  });
  assert.equal(result.health, "reorder_product");
  assert.equal(result.label, "Produkt nachbestellen · in 10 Tagen");
});

test("missing lead is Daten fehlen", () => {
  const result = classifyCoverageHealth({
    amazonAvailable: 40,
    localQty: 0,
    amazonDaysOfCover: 40,
    amazonAndLocalDaysOfCover: 40,
    stockAction: "missing_lead",
    reorderTiming: null,
  });
  assert.equal(result.health, "missing_data");
  assert.equal(result.shortLabel, "Daten fehlen");
});

test("overview: no sales tempo with stock stays Abgedeckt", () => {
  const result = coverageHealthFromOverviewItem(
    item({
      sku: "IX-3V2G-GKCQ",
      status: "no_sales",
      available: 187,
      localQty: 0,
      onOrderUnits: 0,
      transferLeadDays: 9,
      supplierLeadDays: 95,
      daysOfCover: null,
      daysOfCoverAmazonAndLocal: null,
      daysOfCoverWithLocal: null,
      dailySales30: 0.2,
      forecastDailySales: 0.2,
    }),
  );
  assert.equal(result.health, "covered");
  assert.equal(result.shortLabel, "Abgedeckt");
});

test("overview: Amazon knapp + lokal + offenes PO → Ins Amz Lager senden", () => {
  const result = coverageHealthFromOverviewItem(
    item({
      sku: "DEMO",
      status: "critical",
      available: 24,
      localQty: 672,
      onOrderUnits: 3120,
      onOrderOrderedAt: "2026-07-01",
      transferLeadDays: 30,
      supplierLeadDays: 40,
      daysOfCover: 24,
      daysOfCoverAmazonAndLocal: 696,
      daysOfCoverWithLocal: 3816,
      dailySales30: 1,
      forecastDailySales: 1,
    }),
  );
  assert.equal(result.health, "ship_to_amazon");
  assert.equal(amazonShipActionLabel(
    item({
      sku: "DEMO",
      status: "critical",
      available: 24,
      localQty: 672,
      transferLeadDays: 30,
      daysOfCover: 24,
    }),
  ), "jetzt");
  assert.match(
    supplierOrderActionLabel(
      item({
        sku: "DEMO",
        status: "critical",
        onOrderUnits: 3120,
        localQty: 672,
        daysOfCover: 24,
      }),
    ),
    /bereits bestellt/,
  );
});

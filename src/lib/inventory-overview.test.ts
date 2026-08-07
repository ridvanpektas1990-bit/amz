import assert from "node:assert/strict";
import test from "node:test";
import {
  actionPlanWhenLabel,
  buildInventoryActionPlan,
  classifyStockStatus,
  daysUntilAmazonShip,
  daysUntilSupplierOrderDeadline,
  filterItemsBySelectedSku,
  formatCoverDaysDe,
  formatInDaysDe,
  inventoryActionHint,
  inventoryStatusLabel,
  selectOosRiskItems,
  suggestedAmazonShipQty,
  suggestedSupplierOrderQty,
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
    onOrderOrderedAt: partial.onOrderOrderedAt ?? null,
    transferLeadDays: partial.transferLeadDays ?? 7,
    amazonTargetCoverDays: partial.amazonTargetCoverDays ?? 30,
    bufferTimeDays: partial.bufferTimeDays ?? null,
    unitsPerCarton: partial.unitsPerCarton ?? null,
    units30: partial.units30 ?? 0,
    units90: partial.units90 ?? 0,
    dailySales30: partial.dailySales30 ?? 0,
    recentTempoDays: partial.recentTempoDays ?? 14,
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

test("action plan sorts upcoming ship/order todos by due date", () => {
  const plan = buildInventoryActionPlan(
    [
      item({
        sku: "later-order",
        productName: "Produkt Y",
        status: "healthy",
        available: 80,
        supplierLeadDays: 40,
        bufferTimeDays: 0,
        forecastDailySales: 2,
        dailySales30: 2,
        units30: 28,
        recentTempoDays: 14,
        daysOfCover: 80,
        daysOfCoverAmazonAndLocal: 46,
      }),
      item({
        sku: "soon-ship",
        productName: "Produkt X",
        status: "critical",
        available: 20,
        inbound: 0,
        localQty: 100,
        transferLeadDays: 7,
        amazonTargetCoverDays: 30,
        forecastDailySales: 2,
        dailySales30: 2,
        units30: 28,
        recentTempoDays: 14,
        daysOfCover: 9,
        daysOfCoverAmazonAndLocal: 120,
        supplierLeadDays: 90,
      }),
      item({
        sku: "far",
        status: "healthy",
        available: 500,
        localQty: 50,
        transferLeadDays: 7,
        supplierLeadDays: 40,
        daysOfCover: 100,
        daysOfCoverAmazonAndLocal: 120,
        units30: 50,
        dailySales30: 1,
      }),
      item({
        sku: "empty",
        productName: "Leer",
        status: "out",
        available: 0,
        supplierLeadDays: 90,
        bufferTimeDays: 60,
        forecastDailySales: 1,
        dailySales30: 1,
        units30: 14,
        recentTempoDays: 14,
        unitsPerCarton: 32,
      }),
      // Phantom / placeholder — must not appear even with fake local stock
      item({
        sku: "15-FBFB-FBFB",
        status: "critical",
        available: 0,
        localQty: 50,
        transferLeadDays: 7,
        daysOfCover: 0,
        units30: 10,
        units90: 10,
        dailySales30: 0.5,
        forecastDailySales: 0.5,
      }),
      item({
        sku: "dead-local",
        status: "critical",
        available: 0,
        localQty: 50,
        transferLeadDays: 7,
        daysOfCover: 0,
        units30: 0,
        units90: 0,
        dailySales30: 0,
        forecastDailySales: 0,
      }),
    ],
    { horizonDays: 21 },
  );

  assert.deepEqual(
    plan.map((row) => `${row.daysUntil}:${row.kind}:${row.sku}`),
    ["0:sold_out:empty", "2:ship_amazon:soon-ship", "6:order_supplier:later-order"],
  );
  assert.ok(!plan.some((row) => row.sku === "15-FBFB-FBFB"));
  assert.ok(!plan.some((row) => row.sku === "dead-local"));
  assert.equal(actionPlanWhenLabel(2), "in 2 Tagen");
  assert.equal(plan[1]!.actionLabel, "Amz Lager senden");
  // Volle Ziel-Charge: 5 Wochen × 2/Tag × 7 = 70 (kein Abzug FBA)
  assert.equal(plan[1]!.qtySuggested, 70);
  assert.equal(plan[1]!.qtyBasis, "Ziel 30 T");
  assert.equal(plan[2]!.actionLabel, "Lieferant nachbestellen");
  // Wochen-Charge: ceil(40/7)=6 Wochen × (2*7)=14 → 84
  assert.equal(plan[2]!.qtySuggested, 84);
  // empty: lead 90+60=150 → ceil(150/7)=22 Wochen × 7 = 154 → round 5*32=160
  assert.equal(plan[0]!.qtySuggested, 160);
});

test("supplier order qty matches Nachbestellung weekly charge for JB-YNGC-UTVY", () => {
  const qty = suggestedSupplierOrderQty(
    item({
      sku: "JB-YNGC-UTVY",
      status: "healthy",
      available: 85,
      inbound: 240,
      localQty: 576,
      supplierLeadDays: 95,
      bufferTimeDays: 60,
      unitsPerCarton: 48,
      forecastDailySales: 9.14,
      dailySales30: 9.14,
      recentTempoDays: 14,
      daysOfCover: 36,
      daysOfCoverAmazonAndLocal: 99,
    }),
    { todayISO: "2026-08-07" },
  );
  assert.equal(qty, 1488);
});

test("amazon ship qty is full LY/tempo target window, not FBA gap (M0-style)", () => {
  const qty = suggestedAmazonShipQty(
    item({
      sku: "M0-V6TY-PMMJ",
      status: "healthy",
      available: 276,
      inbound: 36,
      localQty: 3456,
      amazonTargetCoverDays: 30,
      unitsPerCarton: 36,
      forecastDailySales: 11.7,
      dailySales30: 11.21,
      recentTempoDays: 14,
      growthFactor: 1.148,
      daysOfCover: 27,
      transferLeadDays: 9,
      supplierLeadDays: 95,
    }),
    { todayISO: "2026-08-07" },
  );
  // 5 weeks × 11.7/day × 7 ≈ 409.5 → 410 → round to 12×36 = 432
  assert.equal(qty, 432);
  assert.ok(qty > 100, "must not be tiny FBA-gap like 39");
});

test("action plan skips stockout when PO is open; keeps Amz ship if local exists", () => {
  // UI-JKHV-J3CU-style: open supplier PO + Lieferverzug math, but actionable todo = ship only
  const plan = buildInventoryActionPlan(
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
      }),
    ],
    { horizonDays: 21 },
  );
  assert.equal(plan.some((row) => row.kind === "ship_amazon"), true);
  assert.equal(plan.some((row) => row.actionLabel === "Stockout prüfen"), false);
  assert.equal(plan.some((row) => row.kind === "order_supplier"), false);
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

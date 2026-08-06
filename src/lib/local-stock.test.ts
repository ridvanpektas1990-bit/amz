import assert from "node:assert/strict";
import test from "node:test";
import {
  classifyLocalStockAction,
  computeInboundLocalDeduction,
  openOrderCoverDays,
  supplierDeliveryGap,
  supplierOrderQtyAfterPipeline,
} from "./local-stock.ts";

test("baseline sets last inbound without deducting", () => {
  const result = computeInboundLocalDeduction(4000, null, 200);
  assert.equal(result.isBaseline, true);
  assert.equal(result.deducted, 0);
  assert.equal(result.nextLocalQty, 4000);
  assert.equal(result.nextLastInboundSeen, 200);
});

test("inbound increase deducts from local qty", () => {
  const result = computeInboundLocalDeduction(4000, 0, 500);
  assert.equal(result.deducted, 500);
  assert.equal(result.nextLocalQty, 3500);
  assert.equal(result.nextLastInboundSeen, 500);
});

test("inbound decrease does not restore local qty", () => {
  const result = computeInboundLocalDeduction(3500, 500, 120);
  assert.equal(result.deducted, 0);
  assert.equal(result.nextLocalQty, 3500);
  assert.equal(result.nextLastInboundSeen, 120);
});

test("deduct caps at local qty", () => {
  const result = computeInboundLocalDeduction(100, 0, 400);
  assert.equal(result.deducted, 100);
  assert.equal(result.nextLocalQty, 0);
});

test("replenish amazon when FBA short but local has stock", () => {
  assert.equal(
    classifyLocalStockAction({
      amazonDaysOfCover: 5,
      transferLeadDays: 7,
      localQty: 2000,
      onOrderUnits: 0,
      supplierLeadDays: 90,
      dailyRate: 20,
      chargeCoverDays: 150,
    }),
    "replenish_amazon",
  );
});

test("order supplier when extended cover below supplier lead", () => {
  assert.equal(
    classifyLocalStockAction({
      amazonDaysOfCover: 10,
      transferLeadDays: 7,
      localQty: 0,
      onOrderUnits: 0,
      supplierLeadDays: 90,
      dailyRate: 20,
      chargeCoverDays: 150,
    }),
    "order_supplier",
  );
});

test("awaiting supplier when open PO arrives before Amazon+local OOS", () => {
  assert.equal(
    classifyLocalStockAction({
      amazonDaysOfCover: 10,
      transferLeadDays: 7,
      localQty: 0,
      onOrderUnits: 3000,
      supplierLeadDays: 90,
      dailyRate: 20,
      amazonAndLocalDaysOfCover: 50, // below lead → would need order
      onOrderArrivesInDays: 40, // but open PO lands before OOS
    }),
    "awaiting_supplier",
  );
});

test("order supplier when Amazon+local below lead even if late PO inflates pipeline cover", () => {
  // M0-V6TY-PMMJ-style: local extends cover ~37d, open PO without date must not say "ok"
  assert.equal(
    classifyLocalStockAction({
      amazonDaysOfCover: 25,
      transferLeadDays: 7,
      localQty: 200,
      onOrderUnits: 100,
      supplierLeadDays: 105,
      dailyRate: 8,
      amazonAndLocalDaysOfCover: 37,
      pipelineDaysOfCover: 120, // gap-jumping pipeline — ignore for supplier decision
      onOrderArrivesInDays: null,
    }),
    "order_supplier",
  );
});

test("pipeline cover drives supplier order even when rate estimate looks fine", () => {
  assert.equal(
    classifyLocalStockAction({
      amazonDaysOfCover: 68,
      transferLeadDays: 7,
      localQty: 816,
      onOrderUnits: 0,
      supplierLeadDays: 90,
      dailyRate: 2,
      amazonAndLocalDaysOfCover: 48,
    }),
    "order_supplier",
  );
});

test("awaiting when open PO arrival matches Lieferverzug (no extra transfer)", () => {
  // Ordered such that remaining lead == amazon+local cover → kein Verzug → awaiting
  assert.equal(
    classifyLocalStockAction({
      amazonDaysOfCover: 24,
      transferLeadDays: 7,
      localQty: 672,
      onOrderUnits: 3120,
      supplierLeadDays: 90,
      dailyRate: 30,
      amazonAndLocalDaysOfCover: 48,
      onOrderArrivesInDays: 48,
    }),
    "awaiting_supplier",
  );
});

test("open order cover days from rate", () => {
  assert.equal(openOrderCoverDays(3120, 30), 104);
  assert.equal(openOrderCoverDays(0, 30), null);
});

test("supplier order qty subtracts on-order only", () => {
  assert.equal(
    supplierOrderQtyAfterPipeline({ rawChargeQty: 3000, onOrderUnits: 1000 }),
    2000,
  );
  assert.equal(
    supplierOrderQtyAfterPipeline({
      rawChargeQty: 3000,
      onOrderUnits: 1000,
      localQty: 500,
      subtractLocal: true,
    }),
    2000,
  );
});

test("supplier delivery gap compares Amazon+local OOS to order arrival", () => {
  // OOS in 30 days from 2026-06-01 → 2026-07-01
  // Ordered 2026-06-01 + 90 lead → arrival 2026-08-30; gap ≈ 60 days
  const gap = supplierDeliveryGap({
    oosDaysAmazonAndLocal: 30,
    orderedAtISO: "2026-06-01",
    supplierLeadDays: 90,
    todayISO: "2026-06-01",
  });
  assert.equal(gap.oosDateISO, "2026-07-01");
  assert.equal(gap.arrivalDateISO, "2026-08-30");
  assert.equal(gap.gapDays, 60);
  assert.equal(gap.hasOpenOrder, true);
});

test("without order date, gap uses today + lead as hypothetical arrival", () => {
  const gap = supplierDeliveryGap({
    oosDaysAmazonAndLocal: 120,
    orderedAtISO: null,
    supplierLeadDays: 90,
    todayISO: "2026-06-01",
  });
  assert.equal(gap.hasOpenOrder, false);
  assert.equal(gap.arrivalDateISO, "2026-08-30");
  assert.equal(gap.gapDays, -30); // 30 days buffer / Bestellfrist
});

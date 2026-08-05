import assert from "node:assert/strict";
import test from "node:test";
import {
  classifyReorderTiming,
  daysToWeeks,
  leadTimeDaysFromSpec,
  leadTimeWeeksFromSpec,
  roundUpToCartons,
} from "./carton-specs.ts";

test("daysToWeeks ceils partial weeks", () => {
  assert.equal(daysToWeeks(0), 0);
  assert.equal(daysToWeeks(7), 1);
  assert.equal(daysToWeeks(8), 2);
  assert.equal(daysToWeeks(45), 7);
});

test("leadTimeDaysFromSpec sums production and shipping in days", () => {
  assert.equal(leadTimeDaysFromSpec(null), null);
  assert.equal(
    leadTimeDaysFromSpec({ productionTimeDays: 30, shippingTimeDays: 15 }),
    45,
  );
  assert.equal(
    leadTimeDaysFromSpec({ productionTimeDays: 0, shippingTimeDays: 0 }),
    null,
  );
  assert.equal(
    leadTimeWeeksFromSpec({ productionTimeDays: 30, shippingTimeDays: 15 }),
    7,
  );
});

test("roundUpToCartons fills complete cartons", () => {
  assert.deepEqual(roundUpToCartons(52, 32), {
    rawQty: 52,
    orderQty: 64,
    unitsPerCarton: 32,
    cartons: 2,
    rounded: true,
  });
  assert.deepEqual(roundUpToCartons(64, 32), {
    rawQty: 64,
    orderQty: 64,
    unitsPerCarton: 32,
    cartons: 2,
    rounded: false,
  });
  assert.equal(roundUpToCartons(52, null).orderQty, 52);
});

test("classifyReorderTiming uses lead time as order deadline", () => {
  assert.equal(classifyReorderTiming(120, 90).status, "ok");
  assert.equal(classifyReorderTiming(120, 90).daysUntilMustOrder, 30);
  assert.equal(classifyReorderTiming(90, 90).status, "order_now");
  assert.equal(classifyReorderTiming(60, 90).status, "too_late");
  assert.equal(classifyReorderTiming(0, 90).status, "already_oos");
  assert.equal(classifyReorderTiming(null, 90).status, "no_demand");
});

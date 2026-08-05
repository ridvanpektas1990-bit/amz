import assert from "node:assert/strict";
import test from "node:test";
import { isActiveListing, sortByDailySalesDesc } from "./listing-activity.ts";

test("active when stock or inbound exists even without sales", () => {
  assert.equal(isActiveListing({ available: 5, inbound: 0, units90: 0 }), true);
  assert.equal(isActiveListing({ available: 0, inbound: 12, units90: 0 }), true);
});

test("active when OOS but recent sales exist", () => {
  assert.equal(isActiveListing({ available: 0, inbound: 0, units90: 3 }), true);
  assert.equal(isActiveListing({ available: 0, inbound: 0, units30: 1 }), true);
});

test("inactive when OOS for a long time with no sales and no inbound", () => {
  assert.equal(isActiveListing({ available: 0, inbound: 0, units90: 0, units30: 0 }), false);
});

test("sorts by daily sales descending", () => {
  const sorted = sortByDailySalesDesc([
    { label: "slow", units30: 30 },
    { label: "fast", units30: 300 },
    { label: "mid", dailySales30: 4 },
  ]);
  assert.deepEqual(
    sorted.map((row) => row.label),
    ["fast", "mid", "slow"],
  );
});

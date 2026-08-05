import assert from "node:assert/strict";
import test from "node:test";
import {
  calculatePositiveGrowthFactor,
  chooseForecastDemand,
  periodsUntilOos,
} from "./inventory-forecast.ts";

test("stabilizes positive growth and never applies negative growth", () => {
  assert.equal(calculatePositiveGrowthFactor(80, 100), 1);
  assert.equal(calculatePositiveGrowthFactor(150, 100), 1.25);
  assert.equal(calculatePositiveGrowthFactor(150, 0), 1);
});

test("uses recent demand when the seasonal reference is zero", () => {
  const forecast = chooseForecastDemand({
    seasonalDemand: 0,
    recentDemand: 110 / 30 * 7,
    growthFactor: 1.8,
  });
  assert.equal(forecast.source, "recent");
  assert.ok(Math.abs(forecast.demand - 25.6666666667) < 0.0001);
});

test("applies growth only to a real seasonal reference", () => {
  const forecast = chooseForecastDemand({
    seasonalDemand: 20,
    recentDemand: 30,
    growthFactor: 1.25,
  });
  assert.deepEqual(forecast, { demand: 25, source: "seasonal" });
});

test("8Y-MKK8-1CHG regression: 179 units at 110 units per 30 days cover about seven weeks", () => {
  const weekly = chooseForecastDemand({ seasonalDemand: 0, recentDemand: 110 / 30 * 7 }).demand;
  const weeks = periodsUntilOos(179, Array.from({ length: 52 }, () => weekly));
  assert.equal(weeks, 7);
});

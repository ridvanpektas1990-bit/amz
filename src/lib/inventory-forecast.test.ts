import assert from "node:assert/strict";
import test from "node:test";
import {
  addWeeksToISO,
  calculatePositiveGrowthFactor,
  chooseForecastDemand,
  explainForecastDemand,
  periodsUntilOos,
  planArrivalShipmentReorder,
  planSixMonthReorder,
  planTargetCoverReorder,
  projectDailyOos,
  projectWeeklyOos,
  weeklyGrowthFactorFromMaps,
} from "./inventory-forecast.ts";
import { daysToWeeks } from "./carton-specs.ts";

test("stabilizes positive growth and never applies negative growth", () => {
  assert.equal(calculatePositiveGrowthFactor(80, 100), 1);
  assert.equal(calculatePositiveGrowthFactor(150, 100), 1.25);
  assert.equal(calculatePositiveGrowthFactor(150, 0), 1);
});

test("uses recent demand when the seasonal reference is zero", () => {
  const forecast = chooseForecastDemand({
    seasonalDemand: 0,
    recentDemand: (110 / 30) * 7,
    growthFactor: 1.8,
  });
  assert.equal(forecast.source, "recent");
  assert.ok(Math.abs(forecast.demand - 25.6666666667) < 0.0001);
  assert.match(explainForecastDemand(forecast.source), /Verkaufstempo/);
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
  const weekly = chooseForecastDemand({
    seasonalDemand: 0,
    recentDemand: (110 / 30) * 7,
  }).demand;
  const weeks = periodsUntilOos(
    179,
    Array.from({ length: 52 }, () => weekly),
  );
  assert.equal(weeks, 7);
});

test("8Y-MKK8-1CHG regression: null weeks in reference year fall back to 30-day tempo", () => {
  const previous = new Map<number, number>();
  const current = new Map<number, number>([
    [1, 20],
    [2, 22],
  ]);
  // No seasonal signal for upcoming weeks → must use recent 110/30d.
  for (let week = 3; week <= 53; week++) previous.set(week, 0);

  const projection = projectWeeklyOos({
    inventory: 179,
    currentWeek: 2,
    previousYearWeekTotals: previous,
    currentYearWeekTotals: current,
    recent30Units: 110,
    recentTempoDays: 30,
  });

  assert.equal(projection.weeks, 7);
  assert.ok(projection.weekKw !== null);
});

test("weekly growth factor ignores future weeks and clamps negative growth", () => {
  const current = new Map([
    [1, 50],
    [2, 50],
    [3, 999],
  ]);
  const previous = new Map([
    [1, 40],
    [2, 40],
    [3, 10],
  ]);
  // Only weeks < 3 count → 100 vs 80, stabilized: (100+100)/(80+100)
  assert.equal(weeklyGrowthFactorFromMaps(current, previous, 3), 200 / 180);
  assert.equal(
    weeklyGrowthFactorFromMaps(
      new Map([
        [1, 10],
        [2, 10],
      ]),
      new Map([
        [1, 50],
        [2, 50],
      ]),
      3,
    ),
    1,
  );
});

test("daily OOS projection marks hybrid when seasonal and recent weeks mix", () => {
  const projection = projectDailyOos({
    inventory: 10,
    todayDemand: { demand: 2, seasonal: true },
    demandForDayOffset: (day) =>
      day < 3
        ? { demand: 2, seasonal: true }
        : { demand: 2, seasonal: false },
  });
  assert.equal(projection.daysOfCover, 5);
  assert.equal(projection.forecastMethod, "hybrid");
});

test("reorder plan covers six months after projected OOS with recent fallback", () => {
  const previous = new Map<number, number>();
  const current = new Map<number, number>([[10, 5]]);
  for (let week = 1; week <= 53; week++) previous.set(week, 0);

  const weekly = (110 / 30) * 7;
  const plan = planSixMonthReorder({
    inventory: weekly * 2 - 0.01, // clearly under 3 weeks, over 1 week
    currentIsoYear: 2026,
    currentIsoWeek: 10,
    previousYearWeekTotals: previous,
    currentYearWeekTotals: current,
    recent30Units: 110,
    recentTempoDays: 30,
    coverWeeks: 26,
  });

  assert.ok(plan);
  assert.equal(plan!.weeksUntilOos, 2);
  assert.ok(Math.abs(plan!.reorderQty - weekly * 26) < 0.01);
});

test("target-cover reorder orders only the gap for lead time + safety from today", () => {
  const previous = new Map<number, number>();
  const current = new Map<number, number>();
  for (let week = 1; week <= 53; week++) previous.set(week, 0);

  const weekly = (110 / 30) * 7; // ~25.67
  const leadTimeWeeks = 10;
  const safetyWeeks = 6;
  const target = leadTimeWeeks + safetyWeeks;
  const inventory = weekly * 4; // 4 weeks on hand (+ inbound already folded in by caller)

  const plan = planTargetCoverReorder({
    inventory,
    currentIsoYear: 2026,
    currentIsoWeek: 10,
    previousYearWeekTotals: previous,
    currentYearWeekTotals: current,
    recent30Units: 110,
    recentTempoDays: 30,
    leadTimeWeeks,
    safetyWeeks,
  });

  assert.ok(plan);
  assert.equal(plan!.targetCoverWeeks, target);
  assert.ok(Math.abs(plan!.demandUntilTarget - weekly * target) < 0.2);
  assert.equal(plan!.reorderQty, Math.ceil(plan!.demandUntilTarget - inventory));
  assert.equal(plan!.needsReorder, true);
  assert.ok(plan!.weeksUntilOos >= 4 && plan!.weeksUntilOos <= 5);

  const covered = planTargetCoverReorder({
    inventory: weekly * target + 5,
    currentIsoYear: 2026,
    currentIsoWeek: 10,
    previousYearWeekTotals: previous,
    currentYearWeekTotals: current,
    recent30Units: 110,
    recentTempoDays: 30,
    leadTimeWeeks,
    safetyWeeks,
  });
  assert.ok(covered);
  assert.equal(covered!.reorderQty, 0);
  assert.equal(covered!.needsReorder, false);
});

test("arrival shipment qty is LY/tempo demand over lead+buffer from arrival, not stock gap", () => {
  const previous = new Map<number, number>();
  const current = new Map<number, number>();
  for (let week = 1; week <= 53; week++) previous.set(week, 70); // flat seasonal

  const leadTimeDays = 90; // → 13 weeks
  const bufferDays = 60; // → cover 150 days → 22 weeks
  const coverWeeks = daysToWeeks(leadTimeDays + bufferDays);
  assert.equal(coverWeeks, 22);

  const plan = planArrivalShipmentReorder({
    inventory: 200, // would wrongly shrink a “demand − stock” model
    currentIsoYear: 2026,
    currentIsoWeek: 10,
    previousYearWeekTotals: previous,
    currentYearWeekTotals: current,
    recent30Units: 10,
    leadTimeDays,
    bufferDays,
  });

  assert.ok(plan);
  assert.equal(plan!.coverDays, 150);
  assert.equal(plan!.coverWeeks, 22);
  assert.equal(plan!.reorderQty, 70 * 22); // stock not subtracted
  const arrival = addWeeksToISO(2026, 10, 13);
  assert.equal(plan!.arrivalWeek, arrival.week);
  assert.equal(plan!.arrivalYear, arrival.year);
});

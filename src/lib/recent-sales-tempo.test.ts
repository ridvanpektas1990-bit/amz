import assert from "node:assert/strict";
import test from "node:test";
import {
  dailyUnitsSeriesFromMap,
  recentSalesTempoFromDaily,
  recentSalesWindow,
  recentWeeklyDemand,
  salesAsOfYesterdayISO,
  RECENT_TEMPO_LOOKBACK_DAYS,
} from "./recent-sales-tempo.ts";

test("uses last 14 calendar days by default", () => {
  assert.equal(RECENT_TEMPO_LOOKBACK_DAYS, 14);
  // 30 days: first 16 are 1/day, last 14 are 10/day → only last 14 count
  const daily = [
    ...Array.from({ length: 16 }, () => 1),
    ...Array.from({ length: 14 }, () => 10),
  ];
  const tempo = recentSalesTempoFromDaily(daily);
  assert.equal(tempo.windowDays, 14);
  assert.equal(tempo.activeDays, 14);
  assert.equal(tempo.units, 140);
  assert.equal(tempo.dailyRate, 10);
  assert.equal(tempo.truncated, false);
});

test("leading zeros inside 14-day window truncate for new listings", () => {
  // 14-day window: 5 zero days then 9 × 10
  const daily = [...Array.from({ length: 5 }, () => 0), ...Array.from({ length: 9 }, () => 10)];
  const tempo = recentSalesTempoFromDaily(daily, 14);
  assert.equal(tempo.truncated, true);
  assert.equal(tempo.activeDays, 9);
  assert.equal(tempo.units, 90);
  assert.equal(tempo.dailyRate, 10);
});

test("all zeros keep full lookback and zero rate", () => {
  const tempo = recentSalesTempoFromDaily(Array.from({ length: 14 }, () => 0));
  assert.equal(tempo.truncated, false);
  assert.equal(tempo.activeDays, 14);
  assert.equal(tempo.dailyRate, 0);
});

test("dailyUnitsSeriesFromMap fills missing days with zero", () => {
  const map = new Map([
    ["2026-07-20", 4],
    ["2026-07-22", 6],
  ]);
  const series = dailyUnitsSeriesFromMap("2026-07-20", "2026-07-22", map);
  assert.deepEqual(series, [4, 0, 6]);
});

test("sales windows end yesterday (today excluded)", () => {
  const asOf = salesAsOfYesterdayISO("2026-08-07");
  assert.equal(asOf, "2026-08-06");
  const w30 = recentSalesWindow(30, asOf);
  assert.equal(w30.endISO, "2026-08-06");
  assert.equal(w30.startISO, "2026-07-08");
  assert.equal(w30.days, 30);
  const w14 = recentSalesWindow(14, asOf);
  assert.equal(w14.endISO, "2026-08-06");
  assert.equal(w14.startISO, "2026-07-24");
});

test("recentWeeklyDemand uses active days as divisor", () => {
  assert.ok(Math.abs(recentWeeklyDemand(140, 14) - 70) < 1e-9);
  assert.ok(Math.abs(recentWeeklyDemand(150, 30) - 35) < 1e-9);
});

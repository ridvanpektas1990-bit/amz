export const FORECAST_GROWTH_PRIOR_UNITS = 100;

function recentWeeklyDemand(units: number, activeDays = 30): number {
  const days = Math.max(1, Math.round(Number(activeDays) || 30));
  return (Math.max(0, Number(units) || 0) / days) * 7;
}


export type ForecastDemandSource = "seasonal" | "recent" | "none";
export type ForecastMethod = "seasonal" | "hybrid" | "recent" | "none";

export function calculatePositiveGrowthFactor(
  currentUnits: number,
  previousUnits: number,
  stabilizationUnits = FORECAST_GROWTH_PRIOR_UNITS,
): number {
  const current = Math.max(0, Number(currentUnits) || 0);
  const previous = Math.max(0, Number(previousUnits) || 0);
  if (previous <= 0 || current <= previous) return 1;
  return (current + stabilizationUnits) / (previous + stabilizationUnits);
}

export function chooseForecastDemand({
  seasonalDemand,
  recentDemand,
  growthFactor = 1,
}: {
  seasonalDemand: number;
  recentDemand: number;
  growthFactor?: number;
}): { demand: number; source: ForecastDemandSource } {
  const seasonal = Math.max(0, Number(seasonalDemand) || 0);
  const recent = Math.max(0, Number(recentDemand) || 0);
  const growth = Math.max(1, Number(growthFactor) || 1);

  if (seasonal > 0) return { demand: seasonal * growth, source: "seasonal" };
  if (recent > 0) return { demand: recent, source: "recent" };
  return { demand: 0, source: "none" };
}

export function explainForecastDemand(source: ForecastDemandSource): string {
  switch (source) {
    case "seasonal":
      return "Saisonaler Vorjahreswert (mit positivem Wachstum, falls vorhanden)";
    case "recent":
      return "Aktuelles Verkaufstempo (letzte 14 Tage) als Fallback";
    default:
      return "Keine belastbare Nachfrage";
  }
}

export function periodsUntilOos(
  inventory: number,
  demands: Iterable<number>,
): number | null {
  let remaining = Math.max(0, Number(inventory) || 0);
  if (remaining <= 0) return 0;

  let period = 0;
  for (const rawDemand of demands) {
    period += 1;
    remaining -= Math.max(0, Number(rawDemand) || 0);
    if (remaining <= 0) return period;
  }
  return null;
}

export function ytdUnitsBeforeWeek(
  weekTotals: Map<number, number> | null | undefined,
  currentWeek: number,
): number {
  if (!weekTotals) return 0;
  let total = 0;
  for (let week = 1; week < currentWeek; week++) {
    total += Math.max(0, weekTotals.get(week) || 0);
  }
  return total;
}

export function weeklyGrowthFactorFromMaps(
  currentYearWeekTotals: Map<number, number>,
  previousYearWeekTotals: Map<number, number>,
  currentWeek: number,
): number {
  return calculatePositiveGrowthFactor(
    ytdUnitsBeforeWeek(currentYearWeekTotals, currentWeek),
    ytdUnitsBeforeWeek(previousYearWeekTotals, currentWeek),
  );
}

export type WeeklyOosProjection = {
  /** 0 = already OOS, -1 = more than ~1 year of cover, else weeks until OOS */
  weeks: number;
  weekKw: number | null;
};

/**
 * Shared weekly OOS walk used by the dashboard chart and reorder block.
 * Null/zero seasonal weeks fall back to the recent 30-day weekly tempo.
 */
export function projectWeeklyOos({
  inventory,
  currentWeek,
  previousYearWeekTotals,
  currentYearWeekTotals,
  recent30Units,
  recentTempoDays = 14,
  delayedAdditions = [],
}: {
  inventory: number;
  currentWeek: number;
  previousYearWeekTotals?: Map<number, number> | null;
  currentYearWeekTotals?: Map<number, number> | null;
  recent30Units?: number;
  /** Days used as divisor for recent tempo (truncated for new listings). */
  recentTempoDays?: number;
  /** Inject units after N weeks (e.g. local warehouse → Amazon transfer). */
  delayedAdditions?: Array<{ weekOffset: number; units: number }>;
}): WeeklyOosProjection {
  const additions = delayedAdditions
    .map((entry) => ({
      weekOffset: Math.max(0, Math.round(Number(entry.weekOffset) || 0)),
      units: Math.max(0, Math.floor(Number(entry.units) || 0)),
    }))
    .filter((entry) => entry.units > 0);
  const lastAdditionWeek = additions.reduce((max, entry) => Math.max(max, entry.weekOffset), 0);

  let remaining = Math.max(0, Number(inventory) || 0);
  for (const entry of additions) {
    if (entry.weekOffset === 0) remaining += entry.units;
  }

  if (remaining <= 0 && additions.every((entry) => entry.weekOffset === 0)) {
    return { weeks: 0, weekKw: currentWeek };
  }

  let weeks = 0;
  let hitWeekKw: number | null = null;
  const fallbackWeeklyDemand = recentWeeklyDemand(Number(recent30Units) || 0, recentTempoDays);
  const growthFactor =
    previousYearWeekTotals && currentYearWeekTotals
      ? weeklyGrowthFactorFromMaps(currentYearWeekTotals, previousYearWeekTotals, currentWeek)
      : 1;

  const demandFor = (seasonal: number, applyGrowth: boolean) =>
    chooseForecastDemand({
      seasonalDemand: Math.max(0, seasonal),
      recentDemand: fallbackWeeklyDemand,
      growthFactor: applyGrowth ? growthFactor : 1,
    }).demand;

  const applyWeek = (week: number, seasonal: number, applyGrowth: boolean) => {
    weeks += 1;
    for (const entry of additions) {
      if (entry.weekOffset === weeks) remaining += entry.units;
    }
    remaining -= demandFor(seasonal, applyGrowth);
    if (remaining <= 0) {
      remaining = 0;
      const moreComing = additions.some((entry) => entry.weekOffset > weeks);
      if (!moreComing) {
        hitWeekKw = week;
        return true;
      }
    }
    return false;
  };

  if (previousYearWeekTotals) {
    for (let week = currentWeek + 1; week <= 53; week++) {
      if (applyWeek(week, previousYearWeekTotals.get(week) ?? 0, true)) break;
    }
  }

  if (hitWeekKw == null && (remaining > 0 || weeks < lastAdditionWeek) && currentYearWeekTotals) {
    for (let week = 1; week <= 53; week++) {
      if (applyWeek(week, currentYearWeekTotals.get(week) ?? 0, false)) break;
    }
  }

  if (hitWeekKw == null && remaining > 0) return { weeks: -1, weekKw: null };
  if (hitWeekKw == null) return { weeks: -1, weekKw: null };
  return { weeks, weekKw: hitWeekKw };
}

export type ReorderPlan = {
  weeksUntilOos: number;
  oosWeek: number;
  oosYear: number;
  /** @deprecated use TargetCoverReorderPlan.reorderQty */
  reorderQty: number;
  newOosWeek: number;
  newOosYear: number;
};

export function isoWeekFromDateISO(dateISO: string): { isoYear: number; isoWeek: number } {
  const day = String(dateISO || "").slice(0, 10);
  const d = new Date(`${day}T00:00:00Z`);
  const target = new Date(d.valueOf());
  const dayNr = (d.getUTCDay() + 6) % 7;
  target.setUTCDate(target.getUTCDate() - dayNr + 3);
  const firstThursday = new Date(Date.UTC(target.getUTCFullYear(), 0, 4));
  const firstDayNr = (firstThursday.getUTCDay() + 6) % 7;
  firstThursday.setUTCDate(firstThursday.getUTCDate() - firstDayNr + 3);
  const week = 1 + Math.round((target.getTime() - firstThursday.getTime()) / 604800000);
  return { isoYear: target.getUTCFullYear(), isoWeek: week };
}

/** Map an OOS calendar date to the chart KW in `chartYear`, or null if outside that year. */
export function chartWeekFromOosDate(
  oosDateISO: string | null | undefined,
  chartYear: number,
): number | null {
  const day = String(oosDateISO || "").slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(day)) return null;
  const { isoYear, isoWeek } = isoWeekFromDateISO(day);
  if (isoYear !== chartYear || isoWeek < 1 || isoWeek > 53) return null;
  return isoWeek;
}

function isoMondayOfWeek(isoYear: number, isoWeek: number): Date {
  const jan4 = new Date(Date.UTC(isoYear, 0, 4));
  const dayNr = (jan4.getUTCDay() + 6) % 7;
  const mondayW1 = new Date(jan4);
  mondayW1.setUTCDate(jan4.getUTCDate() - dayNr);
  const mondayTarget = new Date(mondayW1);
  mondayTarget.setUTCDate(mondayW1.getUTCDate() + (isoWeek - 1) * 7);
  return mondayTarget;
}

function dateToISOUTC(d: Date): string {
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

export function addWeeksToISO(isoYear: number, isoWeek: number, add: number) {
  const monday = isoMondayOfWeek(isoYear, isoWeek);
  monday.setUTCDate(monday.getUTCDate() + add * 7);
  const iso = isoWeekFromDateISO(dateToISOUTC(monday));
  return { year: monday.getUTCFullYear(), week: iso.isoWeek };
}

export const DEFAULT_LEAD_TIME_WEEKS = 10;
export const DEFAULT_SAFETY_WEEKS = 6;

export type TargetCoverReorderPlan = {
  /** 0 = already OOS / empty soon, -1 = no OOS within walk horizon */
  weeksUntilOos: number;
  oosWeek: number | null;
  oosYear: number | null;
  leadTimeWeeks: number;
  safetyWeeks: number;
  targetCoverWeeks: number;
  /** Forecast demand from now through target cover weeks */
  demandUntilTarget: number;
  inventory: number;
  /** max(0, demand − inventory), rounded up */
  reorderQty: number;
  needsReorder: boolean;
  /** OOS week if reorderQty arrives and is added to stock now */
  coveredUntilWeek: number | null;
  coveredUntilYear: number | null;
};

/**
 * Shipment sized for (lead + buffer) days of forecast demand starting at expected arrival.
 * Uses Vorjahr/seasonal weeks (with growth) for the arrival calendar window — not “demand from today − stock”.
 */
export type ArrivalShipmentPlan = {
  weeksUntilOos: number;
  oosWeek: number | null;
  oosYear: number | null;
  leadTimeDays: number;
  bufferDays: number;
  coverDays: number;
  leadTimeWeeks: number;
  coverWeeks: number;
  arrivalWeek: number;
  arrivalYear: number;
  /** Forecast demand over cover window starting at arrival */
  shipmentDemand: number;
  inventory: number;
  /** ceil(shipmentDemand) — charge size, stock not subtracted */
  reorderQty: number;
  needsReorder: boolean;
};

type YearTag = "previous" | "current";

function createWeeklyDemandOf({
  previousYearWeekTotals,
  currentYearWeekTotals,
  recent30Units,
  recentTempoDays = 14,
  currentIsoWeek,
}: {
  previousYearWeekTotals: Map<number, number>;
  currentYearWeekTotals: Map<number, number>;
  recent30Units: number;
  recentTempoDays?: number;
  currentIsoWeek: number;
}) {
  const fallbackWeeklyDemand = recentWeeklyDemand(recent30Units, recentTempoDays);
  const growthFactor = weeklyGrowthFactorFromMaps(
    currentYearWeekTotals,
    previousYearWeekTotals,
    currentIsoWeek,
  );

  return (tag: YearTag, week: number) =>
    chooseForecastDemand({
      seasonalDemand: Math.max(
        0,
        (tag === "previous" ? previousYearWeekTotals : currentYearWeekTotals).get(week) ?? 0,
      ),
      recentDemand: fallbackWeeklyDemand,
      growthFactor: tag === "previous" ? growthFactor : 1,
    }).demand;
}

function advanceWeek(week: number, tag: YearTag): { week: number; tag: YearTag } {
  let nextWeek = week + 1;
  let nextTag = tag;
  if (nextWeek > 53) {
    nextWeek = 1;
    nextTag = tag === "previous" ? "current" : "previous";
  }
  return { week: nextWeek, tag: nextTag };
}

/**
 * Target-cover reorder: order the gap to (lead time + safety) weeks of demand from today.
 * inventory should already include inbound if you want Zulauf counted.
 */
export function planTargetCoverReorder({
  inventory,
  currentIsoYear,
  currentIsoWeek,
  previousYearWeekTotals,
  currentYearWeekTotals,
  recent30Units,
  recentTempoDays = 14,
  leadTimeWeeks = DEFAULT_LEAD_TIME_WEEKS,
  safetyWeeks = DEFAULT_SAFETY_WEEKS,
}: {
  inventory: number;
  currentIsoYear: number;
  currentIsoWeek: number;
  previousYearWeekTotals: Map<number, number>;
  currentYearWeekTotals: Map<number, number>;
  recent30Units: number;
  recentTempoDays?: number;
  leadTimeWeeks?: number;
  safetyWeeks?: number;
}): TargetCoverReorderPlan | null {
  if (!Number.isFinite(inventory)) return null;

  const lead = Math.max(0, Math.round(Number(leadTimeWeeks) || 0));
  const safety = Math.max(0, Math.round(Number(safetyWeeks) || 0));
  const targetCoverWeeks = Math.max(1, lead + safety);
  const demandOf = createWeeklyDemandOf({
    previousYearWeekTotals,
    currentYearWeekTotals,
    recent30Units,
    recentTempoDays,
    currentIsoWeek,
  });

  const stock = Math.max(0, Number(inventory) || 0);

  // Demand from now for target cover horizon
  let week = currentIsoWeek;
  let tag: YearTag = "previous";
  let demandUntilTarget = 0;
  for (let i = 0; i < targetCoverWeeks; i++) {
    ({ week, tag } = advanceWeek(week, tag));
    demandUntilTarget += demandOf(tag, week);
  }

  const reorderQty = Math.max(0, Math.ceil(demandUntilTarget - stock));

  // OOS walk on current stock
  let remaining = stock;
  let walkWeek = currentIsoWeek;
  let walkTag: YearTag = "previous";
  let elapsed = 0;
  let oosWeek: number | null = null;
  let oosYear: number | null = null;

  if (remaining <= 0) {
    elapsed = 0;
    const now = addWeeksToISO(currentIsoYear, currentIsoWeek, 0);
    oosWeek = now.week;
    oosYear = now.year;
  } else {
    for (let guard = 0; guard < 500 && remaining > 0; guard++) {
      ({ week: walkWeek, tag: walkTag } = advanceWeek(walkWeek, walkTag));
      remaining -= demandOf(walkTag, walkWeek);
      elapsed += 1;
    }
    if (remaining <= 0) {
      const oosIso = addWeeksToISO(currentIsoYear, currentIsoWeek, elapsed);
      oosWeek = oosIso.week;
      oosYear = oosIso.year;
    } else {
      elapsed = -1;
    }
  }

  // Cover after ordering (assume qty added to sellable pool)
  let coveredUntilWeek: number | null = null;
  let coveredUntilYear: number | null = null;
  const afterOrder = stock + reorderQty;
  if (afterOrder > 0) {
    let rem = afterOrder;
    let w = currentIsoWeek;
    let t: YearTag = "previous";
    let steps = 0;
    for (let guard = 0; guard < 500 && rem > 0; guard++) {
      ({ week: w, tag: t } = advanceWeek(w, t));
      rem -= demandOf(t, w);
      steps += 1;
    }
    if (rem <= 0) {
      const covered = addWeeksToISO(currentIsoYear, currentIsoWeek, steps);
      coveredUntilWeek = covered.week;
      coveredUntilYear = covered.year;
    }
  } else {
    const now = addWeeksToISO(currentIsoYear, currentIsoWeek, 0);
    coveredUntilWeek = now.week;
    coveredUntilYear = now.year;
  }

  return {
    weeksUntilOos: elapsed,
    oosWeek,
    oosYear,
    leadTimeWeeks: lead,
    safetyWeeks: safety,
    targetCoverWeeks,
    demandUntilTarget: Math.round(demandUntilTarget * 10) / 10,
    inventory: stock,
    reorderQty,
    needsReorder: reorderQty > 0,
    coveredUntilWeek,
    coveredUntilYear,
  };
}

function weeksCeilFromDays(days: number): number {
  const value = Math.max(0, Number(days) || 0);
  if (value <= 0) return 0;
  return Math.ceil(value / 7);
}

/**
 * Order qty = forecast demand for (leadDays + bufferDays) starting when the PO is expected to arrive.
 * Calendar weeks follow the same Vorjahr/seasonal path as other forecasts (growth on seasonal, else 30d tempo).
 * Timing (OOS) is still based on current stock; stock is not subtracted from the charge size.
 */
export function planArrivalShipmentReorder({
  inventory,
  currentIsoYear,
  currentIsoWeek,
  previousYearWeekTotals,
  currentYearWeekTotals,
  recent30Units,
  recentTempoDays = 14,
  leadTimeDays,
  bufferDays = 0,
}: {
  inventory: number;
  currentIsoYear: number;
  currentIsoWeek: number;
  previousYearWeekTotals: Map<number, number>;
  currentYearWeekTotals: Map<number, number>;
  recent30Units: number;
  recentTempoDays?: number;
  leadTimeDays: number;
  bufferDays?: number;
}): ArrivalShipmentPlan | null {
  if (!Number.isFinite(inventory)) return null;

  const leadDays = Math.max(0, Math.round(Number(leadTimeDays) || 0));
  const buffer = Math.max(0, Math.round(Number(bufferDays) || 0));
  const coverDays = Math.max(1, leadDays + buffer);
  const leadWeeks = weeksCeilFromDays(leadDays);
  const coverWeeks = Math.max(1, weeksCeilFromDays(coverDays));

  const demandOf = createWeeklyDemandOf({
    previousYearWeekTotals,
    currentYearWeekTotals,
    recent30Units,
    recentTempoDays,
    currentIsoWeek,
  });

  const stock = Math.max(0, Number(inventory) || 0);

  // Walk to expected arrival week, then sum coverWeeks of demand (arrival window / LY-aligned).
  let week = currentIsoWeek;
  let tag: YearTag = "previous";
  for (let i = 0; i < leadWeeks; i++) {
    ({ week, tag } = advanceWeek(week, tag));
  }
  const arrival = addWeeksToISO(currentIsoYear, currentIsoWeek, leadWeeks);

  let shipmentDemand = 0;
  for (let i = 0; i < coverWeeks; i++) {
    if (leadWeeks === 0 || i > 0) {
      ({ week, tag } = advanceWeek(week, tag));
    }
    shipmentDemand += demandOf(tag, week);
  }

  const reorderQty = Math.max(0, Math.ceil(shipmentDemand));

  // OOS walk on current stock (timing only)
  let remaining = stock;
  let walkWeek = currentIsoWeek;
  let walkTag: YearTag = "previous";
  let elapsed = 0;
  let oosWeek: number | null = null;
  let oosYear: number | null = null;

  if (remaining <= 0) {
    elapsed = 0;
    const now = addWeeksToISO(currentIsoYear, currentIsoWeek, 0);
    oosWeek = now.week;
    oosYear = now.year;
  } else {
    for (let guard = 0; guard < 500 && remaining > 0; guard++) {
      ({ week: walkWeek, tag: walkTag } = advanceWeek(walkWeek, walkTag));
      remaining -= demandOf(walkTag, walkWeek);
      elapsed += 1;
    }
    if (remaining <= 0) {
      const oosIso = addWeeksToISO(currentIsoYear, currentIsoWeek, elapsed);
      oosWeek = oosIso.week;
      oosYear = oosIso.year;
    } else {
      elapsed = -1;
    }
  }

  return {
    weeksUntilOos: elapsed,
    oosWeek,
    oosYear,
    leadTimeDays: leadDays,
    bufferDays: buffer,
    coverDays,
    leadTimeWeeks: leadWeeks,
    coverWeeks,
    arrivalWeek: arrival.week,
    arrivalYear: arrival.year,
    shipmentDemand: Math.round(shipmentDemand * 10) / 10,
    inventory: stock,
    reorderQty,
    needsReorder: reorderQty > 0,
  };
}

/**
 * @deprecated Prefer planTargetCoverReorder (lead time + safety from today).
 * Kept for compatibility: orders 6 months of demand starting at projected OOS.
 */
export function planSixMonthReorder({
  inventory,
  currentIsoYear,
  currentIsoWeek,
  previousYearWeekTotals,
  currentYearWeekTotals,
  recent30Units,
  recentTempoDays = 14,
  coverWeeks = 26,
}: {
  inventory: number;
  currentIsoYear: number;
  currentIsoWeek: number;
  previousYearWeekTotals: Map<number, number>;
  currentYearWeekTotals: Map<number, number>;
  recent30Units: number;
  recentTempoDays?: number;
  coverWeeks?: number;
}): ReorderPlan | null {
  if (!Number.isFinite(inventory)) return null;

  const demandOf = createWeeklyDemandOf({
    previousYearWeekTotals,
    currentYearWeekTotals,
    recent30Units,
    recentTempoDays,
    currentIsoWeek,
  });

  let remaining = inventory;
  let week = currentIsoWeek;
  let tag: YearTag = "previous";
  let elapsed = 0;

  for (let guard = 0; guard < 500 && remaining > 0; guard++) {
    ({ week, tag } = advanceWeek(week, tag));
    remaining -= demandOf(tag, week);
    elapsed += 1;
  }

  const oosIso = addWeeksToISO(currentIsoYear, currentIsoWeek, Math.max(elapsed, 0));
  let need = 0;
  let tw = week;
  let ttag: YearTag = tag;
  for (let i = 0; i < coverWeeks; i++) {
    ({ week: tw, tag: ttag } = advanceWeek(tw, ttag));
    need += demandOf(ttag, tw);
  }

  const newOos = addWeeksToISO(oosIso.year, oosIso.week, coverWeeks);
  return {
    weeksUntilOos: elapsed,
    oosWeek: oosIso.week,
    oosYear: oosIso.year,
    reorderQty: need,
    newOosWeek: newOos.week,
    newOosYear: newOos.year,
  };
}

export type DailyDemandPoint = {
  demand: number;
  seasonal: boolean;
};

export type DailyOosProjection = {
  daysOfCover: number | null;
  forecastMethod: ForecastMethod;
  forecastDailySales: number;
};

/**
 * Day-by-day cover used by /api/inventory/overview.
 * Optional delayedAdditions inject units on a future day offset (e.g. local warehouse → Amazon).
 */
export function projectDailyOos({
  inventory,
  todayDemand,
  demandForDayOffset,
  maxDays = 730,
  delayedAdditions = [],
}: {
  inventory: number;
  todayDemand: DailyDemandPoint;
  demandForDayOffset: (dayOffset: number) => DailyDemandPoint;
  maxDays?: number;
  delayedAdditions?: Array<{ dayOffset: number; units: number }>;
}): DailyOosProjection {
  const available = Math.max(0, Number(inventory) || 0);
  const additions = delayedAdditions
    .map((entry) => ({
      dayOffset: Math.max(0, Math.round(Number(entry.dayOffset) || 0)),
      units: Math.max(0, Math.floor(Number(entry.units) || 0)),
    }))
    .filter((entry) => entry.units > 0);
  const lastAdditionDay = additions.reduce((max, entry) => Math.max(max, entry.dayOffset), 0);

  let remaining = available;
  let daysOfCover: number | null = null;
  let seasonalDays = 0;
  let fallbackDays = 0;
  let hasDemand = false;

  if (available <= 0 && additions.length === 0) {
    if (todayDemand.demand > 0) {
      hasDemand = true;
      if (todayDemand.seasonal) seasonalDays = 1;
      else fallbackDays = 1;
    }
    return {
      daysOfCover: 0,
      forecastMethod: hasDemand ? (todayDemand.seasonal ? "seasonal" : "recent") : "none",
      forecastDailySales: todayDemand.demand,
    };
  }

  for (let day = 0; day < maxDays; day++) {
    for (const entry of additions) {
      if (entry.dayOffset === day) remaining += entry.units;
    }

    const forecast = demandForDayOffset(day);
    if (forecast.demand <= 0) {
      if (remaining <= 0 && day >= lastAdditionDay) break;
      continue;
    }

    hasDemand = true;
    if (forecast.seasonal) seasonalDays += 1;
    else fallbackDays += 1;

    if (remaining <= 0) {
      // Stockout gap while waiting for a later delayed addition.
      const moreComing = additions.some((entry) => entry.dayOffset > day);
      if (!moreComing) {
        daysOfCover = daysOfCover ?? day;
        break;
      }
      continue;
    }

    remaining -= forecast.demand;
    if (remaining <= 0) {
      remaining = 0;
      daysOfCover = day + 1;
      const moreComing = additions.some((entry) => entry.dayOffset > day);
      if (!moreComing) break;
    }
  }

  let forecastMethod: ForecastMethod = "none";
  if (seasonalDays > 0 && fallbackDays > 0) forecastMethod = "hybrid";
  else if (seasonalDays > 0) forecastMethod = "seasonal";
  else if (fallbackDays > 0) forecastMethod = "recent";
  if (!hasDemand && (available > 0 || additions.length > 0)) daysOfCover = null;

  return {
    daysOfCover,
    forecastMethod,
    forecastDailySales: todayDemand.demand,
  };
}

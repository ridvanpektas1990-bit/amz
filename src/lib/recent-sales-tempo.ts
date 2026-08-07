/**
 * Recent-sales tempo for forecasts.
 * Uses the last N calendar days ending yesterday (sales are complete only
 * through yesterday). Leading zero days inside that window are dropped for
 * brand-new listings.
 */

export const RECENT_TEMPO_LOOKBACK_DAYS = 14;
/** Rolling units30 / activity window length (ending yesterday). */
export const RECENT_SALES_UNITS30_DAYS = 30;

export type RecentSalesTempo = {
  /** Units in the effective window. */
  units: number;
  /** Calendar lookback requested (e.g. 14). */
  windowDays: number;
  /** Days used as divisor after dropping leading zeros in the lookback. */
  activeDays: number;
  /** units / activeDays */
  dailyRate: number;
  /** True when leading zero days were dropped inside the lookback. */
  truncated: boolean;
};

function berlinTodayISO(): string {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: "Europe/Berlin",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(new Date());
}

function addDaysISO(dateISO: string, days: number): string {
  const date = new Date(`${dateISO.slice(0, 10)}T12:00:00Z`);
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
}

/**
 * Last complete sales day (Berlin). Today is excluded — order data is incomplete.
 */
export function salesAsOfYesterdayISO(todayISO?: string): string {
  const today = (todayISO || berlinTodayISO()).slice(0, 10);
  return addDaysISO(today, -1);
}

/**
 * Inclusive calendar window of `days` ending on `asOfISO`
 * (default: yesterday). E.g. 30 → yesterday−29 … yesterday.
 */
export function recentSalesWindow(
  days: number,
  asOfISO?: string,
): { startISO: string; endISO: string; days: number } {
  const count = Math.max(1, Math.round(Number(days) || 1));
  const endISO = (asOfISO || salesAsOfYesterdayISO()).slice(0, 10);
  const startISO = addDaysISO(endISO, -(count - 1));
  return { startISO, endISO, days: count };
}

/**
 * @param dailyUnitsOldestFirst - one entry per calendar day, index 0 = oldest, last = as-of day (yesterday)
 * @param lookbackDays - use only the newest N days (default 14)
 */
export function recentSalesTempoFromDaily(
  dailyUnitsOldestFirst: number[],
  lookbackDays: number = RECENT_TEMPO_LOOKBACK_DAYS,
): RecentSalesTempo {
  const lookback = Math.max(1, Math.round(Number(lookbackDays) || RECENT_TEMPO_LOOKBACK_DAYS));
  const slice =
    dailyUnitsOldestFirst.length > lookback
      ? dailyUnitsOldestFirst.slice(-lookback)
      : dailyUnitsOldestFirst.slice();
  const windowDays = slice.length;

  if (windowDays <= 0) {
    return { units: 0, windowDays: 0, activeDays: 0, dailyRate: 0, truncated: false };
  }

  let firstSaleIdx = -1;
  for (let i = 0; i < windowDays; i++) {
    if (Math.max(0, Number(slice[i]) || 0) > 0) {
      firstSaleIdx = i;
      break;
    }
  }

  if (firstSaleIdx < 0) {
    return { units: 0, windowDays: windowDays, activeDays: windowDays, dailyRate: 0, truncated: false };
  }

  const truncated = firstSaleIdx > 0;
  let units = 0;
  for (let i = firstSaleIdx; i < windowDays; i++) {
    units += Math.max(0, Number(slice[i]) || 0);
  }
  const activeDays = windowDays - firstSaleIdx;
  return {
    units,
    windowDays,
    activeDays,
    dailyRate: activeDays > 0 ? units / activeDays : 0,
    truncated,
  };
}

/** Build oldest→newest daily series for [startISO, endISO] inclusive. */
export function dailyUnitsSeriesFromMap(
  startISO: string,
  endISO: string,
  unitsByDate: Map<string, number>,
): number[] {
  const out: number[] = [];
  const cursor = new Date(`${startISO}T12:00:00Z`);
  const end = new Date(`${endISO}T12:00:00Z`);
  if (!Number.isFinite(cursor.getTime()) || !Number.isFinite(end.getTime()) || cursor > end) {
    return out;
  }
  while (cursor <= end) {
    const key = cursor.toISOString().slice(0, 10);
    out.push(Math.max(0, Number(unitsByDate.get(key)) || 0));
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }
  return out;
}

/** Weekly tempo from recent sales (for weekly forecast fallbacks). */
export function recentWeeklyDemandFromTempo(tempo: Pick<RecentSalesTempo, "dailyRate">): number {
  return Math.max(0, Number(tempo.dailyRate) || 0) * 7;
}

/** Units / days → weekly demand. */
export function recentWeeklyDemand(units: number, activeDays = RECENT_TEMPO_LOOKBACK_DAYS): number {
  const days = Math.max(1, Math.round(Number(activeDays) || RECENT_TEMPO_LOOKBACK_DAYS));
  return (Math.max(0, Number(units) || 0) / days) * 7;
}

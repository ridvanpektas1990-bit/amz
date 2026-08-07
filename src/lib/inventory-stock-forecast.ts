import {
  chooseForecastDemand,
  isoWeekFromDateISO,
  weeklyGrowthFactorFromMaps,
} from "./inventory-forecast.ts";
import {
  DEFAULT_AMAZON_TARGET_COVER_DAYS,
  DEFAULT_TRANSFER_LEAD_DAYS,
  onOrderArrivalDelayDays,
} from "./local-stock.ts";
import { berlinTodayISO } from "./sync-status.ts";

export type StockForecastHorizonDays = 90 | 180 | 365;

export type StockForecastEventKind =
  | "today"
  | "transfer_ship"
  | "amazon_empty_without_transfer"
  | "total_empty_without_po"
  | "supplier_delivery";

export type StockForecastEvent = {
  kind: StockForecastEventKind;
  label: string;
  /** Short label for chart badges / crowded UI. */
  shortLabel: string;
  units?: number;
  dateISO: string;
  dayOffset: number;
};

export type StockForecastPoint = {
  dateISO: string;
  dayOffset: number;
  amazon: number;
  local: number;
  total: number;
  events: StockForecastEvent[];
};

export type StockForecastInput = {
  available: number;
  inbound: number;
  localQty: number;
  onOrderUnits?: number | null;
  onOrderOrderedAt?: string | null;
  supplierLeadDays?: number | null;
  transferLeadDays?: number | null;
  recommendedShipQty?: number | null;
  amazonTargetCoverDays?: number | null;
  forecastDailySales?: number | null;
  dailySales30?: number | null;
  previousYearWeekTotals?: Map<number, number> | null;
  currentYearWeekTotals?: Map<number, number> | null;
  todayISO?: string;
  horizonDays?: StockForecastHorizonDays | number;
};

export type StockForecastResult = {
  points: StockForecastPoint[];
  events: StockForecastEvent[];
  dailyDemandApprox: number;
  transferShipQty: number;
  supplierArrivalUnits: number;
};

type PendingTransfer = {
  shipDay: number;
  arriveDay: number;
  units: number;
  announce: boolean;
};

function addDaysISO(dateISO: string, days: number): string {
  const date = new Date(`${dateISO.slice(0, 10)}T12:00:00Z`);
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
}

function formatDeDate(dateISO: string): string {
  const [y, m, d] = dateISO.slice(0, 10).split("-");
  if (!y || !m || !d) return dateISO;
  return `${d}.${m}.${y}`;
}

function formatUnits(units: number): string {
  return new Intl.NumberFormat("de-DE").format(Math.max(0, Math.round(units)));
}

function recentDailyRate(input: StockForecastInput): number {
  const forecast = Math.max(0, Number(input.forecastDailySales) || 0);
  if (forecast > 0) return forecast;
  return Math.max(0, Number(input.dailySales30) || 0);
}

function demandForDayOffset(
  dayOffset: number,
  todayISO: string,
  recentDaily: number,
  previousYearWeekTotals: Map<number, number> | null | undefined,
  currentYearWeekTotals: Map<number, number> | null | undefined,
): number {
  if (recentDaily <= 0 && !previousYearWeekTotals?.size) return 0;

  const dateISO = addDaysISO(todayISO, dayOffset);
  const { isoWeek } = isoWeekFromDateISO(dateISO);
  const growth =
    previousYearWeekTotals && currentYearWeekTotals
      ? weeklyGrowthFactorFromMaps(
          currentYearWeekTotals,
          previousYearWeekTotals,
          isoWeekFromDateISO(todayISO).isoWeek,
        )
      : 1;
  const seasonalWeekly = previousYearWeekTotals?.get(isoWeek) ?? 0;
  const seasonalDaily = seasonalWeekly > 0 ? seasonalWeekly / 7 : 0;
  return chooseForecastDemand({
    seasonalDemand: seasonalDaily,
    recentDemand: recentDaily,
    growthFactor: growth,
  }).demand;
}

function firstEmptyDay(
  startUnits: number,
  horizon: number,
  demandAt: (day: number) => number,
): number | null {
  let remaining = Math.max(0, startUnits);
  if (remaining <= 0) return 0;
  for (let day = 0; day < horizon; day++) {
    remaining -= demandAt(day);
    if (remaining <= 0) return day;
  }
  return null;
}

function coverShipQty(recentDaily: number, targetCoverDays: number, localQty: number): number {
  const rate = Math.max(0, recentDaily);
  const cover = Math.max(1, Math.round(targetCoverDays) || DEFAULT_AMAZON_TARGET_COVER_DAYS);
  if (rate <= 0 || localQty <= 0) return 0;
  const qty = Math.max(1, Math.ceil((cover / 7) * rate * 7));
  return Math.min(localQty, qty);
}

/**
 * Day-by-day Amazon / local / total stock forecast for the dashboard Bestandsprognose chart.
 * Reuses chooseForecastDemand (Vorjahr+Wachstum, sonst Tempo). Open POs join local on arrival only.
 * Local stock is auto-transferred to Amazon so sell-through continues after PO arrivals.
 */
export function buildInventoryStockForecast(input: StockForecastInput): StockForecastResult {
  const todayISO = (input.todayISO || berlinTodayISO()).slice(0, 10);
  const horizon = Math.max(1, Math.min(730, Math.round(Number(input.horizonDays) || 180)));
  const recentDaily = recentDailyRate(input);
  const prevMap = input.previousYearWeekTotals;
  const currMap = input.currentYearWeekTotals;
  const targetCover = Math.max(
    1,
    Math.round(Number(input.amazonTargetCoverDays) || DEFAULT_AMAZON_TARGET_COVER_DAYS),
  );

  const demandAt = (day: number) =>
    demandForDayOffset(day, todayISO, recentDaily, prevMap, currMap);

  const amazonStart =
    Math.max(0, Math.floor(Number(input.available) || 0)) +
    Math.max(0, Math.floor(Number(input.inbound) || 0));
  const localStart = Math.max(0, Math.floor(Number(input.localQty) || 0));
  const onOrder = Math.max(0, Math.floor(Number(input.onOrderUnits) || 0));
  const transferLead = Math.max(
    0,
    Math.round(Number(input.transferLeadDays) || DEFAULT_TRANSFER_LEAD_DAYS) ||
      DEFAULT_TRANSFER_LEAD_DAYS,
  );

  const recommendedShip = Math.max(0, Math.floor(Number(input.recommendedShipQty) || 0));
  const initialTransferQty = Math.min(
    localStart,
    recommendedShip > 0 ? recommendedShip : coverShipQty(recentDaily, targetCover, localStart),
  );

  const amazonEmptyWithoutTransfer = firstEmptyDay(amazonStart, horizon, demandAt);
  const totalEmptyWithoutPo = firstEmptyDay(amazonStart + localStart, horizon, demandAt);

  let plannedShipDay: number | null = null;
  if (initialTransferQty > 0) {
    plannedShipDay =
      amazonEmptyWithoutTransfer != null
        ? Math.max(0, amazonEmptyWithoutTransfer - transferLead)
        : 0;
  }

  const supplierArriveInDays =
    onOrder > 0
      ? onOrderArrivalDelayDays({
          orderedAtISO: input.onOrderOrderedAt,
          supplierLeadDays: input.supplierLeadDays,
          todayISO,
        })
      : null;
  const supplierArriveDay =
    supplierArriveInDays != null && onOrder > 0
      ? Math.max(0, Math.min(horizon, supplierArriveInDays))
      : null;

  let amazon = amazonStart;
  let local = localStart;
  const pending: PendingTransfer[] = [];
  const points: StockForecastPoint[] = [];
  const timelineEvents: StockForecastEvent[] = [];
  let announcedInitialTransfer = false;

  const pushEvent = (event: StockForecastEvent, dayEvents: StockForecastEvent[]) => {
    dayEvents.push(event);
    timelineEvents.push(event);
  };

  for (let day = 0; day <= horizon; day++) {
    const dateISO = addDaysISO(todayISO, day);
    const dayEvents: StockForecastEvent[] = [];

    if (day === 0) {
      pushEvent(
        {
          kind: "today",
          label: "Heute",
          shortLabel: "Heute",
          dateISO,
          dayOffset: day,
        },
        dayEvents,
      );
    }

    const applyArrivalsDueToday = () => {
      for (let i = pending.length - 1; i >= 0; i--) {
        const transfer = pending[i];
        if (transfer.arriveDay !== day) continue;
        amazon += transfer.units;
        pending.splice(i, 1);
      }
    };

    // 1) Transfer arrivals from earlier ship days → Amazon
    applyArrivalsDueToday();

    // 2) Supplier PO → local
    if (supplierArriveDay != null && day === supplierArriveDay && onOrder > 0) {
      local += onOrder;
      pushEvent(
        {
          kind: "supplier_delivery",
          label: `${formatUnits(onOrder)} Stk. Lieferung`,
          shortLabel: `${formatUnits(onOrder)} Lieferung`,
          units: onOrder,
          dateISO,
          dayOffset: day,
        },
        dayEvents,
      );
    }

    // 3) Schedule transfers from local → Amazon
    const inFlightUnits = () =>
      pending
        .filter((row) => row.shipDay <= day && row.arriveDay > day)
        .reduce((sum, row) => sum + row.units, 0);

    const scheduleTransfer = (units: number, announce: boolean) => {
      const qty = Math.min(Math.max(0, Math.floor(units)), local);
      if (qty <= 0) return;
      local -= qty;
      pending.push({
        shipDay: day,
        arriveDay: day + transferLead,
        units: qty,
        announce,
      });
      if (announce) {
        pushEvent(
          {
            kind: "transfer_ship",
            label: `${formatUnits(qty)} Stk. an Amazon senden`,
            shortLabel: `${formatUnits(qty)} → Amazon`,
            units: qty,
            dateISO,
            dayOffset: day,
          },
          dayEvents,
        );
      }
      // Transfer lead 0: available at Amazon the same day.
      if (transferLead === 0) applyArrivalsDueToday();
    };

    // Planned first transfer (recommended / cover charge).
    if (
      plannedShipDay != null &&
      day === plannedShipDay &&
      !announcedInitialTransfer &&
      initialTransferQty > 0 &&
      local >= initialTransferQty
    ) {
      scheduleTransfer(initialTransferQty, true);
      announcedInitialTransfer = true;
    }

    // Keep sell-through going: if Amazon is empty and local sits idle, send the next batch.
    // (Also covers stock that arrives later via supplier PO.)
    if (amazon <= 0 && local > 0 && inFlightUnits() <= 0) {
      const nextQty =
        recommendedShip > 0
          ? Math.min(local, recommendedShip)
          : coverShipQty(recentDaily, targetCover, local);
      if (nextQty > 0) {
        const announce = !announcedInitialTransfer;
        scheduleTransfer(nextQty, announce);
        if (announce) announcedInitialTransfer = true;
      }
    }

    // Scenario markers (planning only).
    if (amazonEmptyWithoutTransfer != null && day === amazonEmptyWithoutTransfer) {
      pushEvent(
        {
          kind: "amazon_empty_without_transfer",
          label: "Amazon leer ohne Transfer",
          shortLabel: "Amazon leer o. Transfer",
          dateISO,
          dayOffset: day,
        },
        dayEvents,
      );
    }
    if (totalEmptyWithoutPo != null && day === totalEmptyWithoutPo) {
      pushEvent(
        {
          kind: "total_empty_without_po",
          label: "Gesamt leer ohne Nachbestellung",
          shortLabel: "Gesamt leer o. Bestellung",
          dateISO,
          dayOffset: day,
        },
        dayEvents,
      );
    }

    const inTransit = pending
      .filter((row) => row.shipDay <= day && row.arriveDay > day)
      .reduce((sum, row) => sum + row.units, 0);

    points.push({
      dateISO,
      dayOffset: day,
      amazon: Math.max(0, Math.round(amazon)),
      local: Math.max(0, Math.round(local)),
      total: Math.max(0, Math.round(amazon + local + inTransit)),
      events: dayEvents,
    });

    // 4) Sales from Amazon/FBA only (after auto-transfer path keeps Amazon fed from local).
    const demand = demandAt(day);
    if (demand > 0 && amazon > 0) {
      amazon = Math.max(0, amazon - demand);
    }
  }

  return {
    points,
    events: timelineEvents,
    dailyDemandApprox: Math.round(recentDaily * 10) / 10,
    transferShipQty: initialTransferQty,
    supplierArrivalUnits: supplierArriveDay != null ? onOrder : 0,
  };
}

export function stockForecastEventTone(kind: StockForecastEventKind): string {
  switch (kind) {
    case "today":
      return "#16a34a";
    case "transfer_ship":
      return "#0284c7";
    case "amazon_empty_without_transfer":
      return "#dc2626";
    case "total_empty_without_po":
      return "#2563eb";
    case "supplier_delivery":
      return "#059669";
    default:
      return "#64748b";
  }
}

export function stockForecastEventBadgeClass(kind: StockForecastEventKind): string {
  switch (kind) {
    case "today":
      return "bg-emerald-50 text-emerald-800 ring-emerald-200";
    case "transfer_ship":
      return "bg-sky-50 text-sky-800 ring-sky-200";
    case "amazon_empty_without_transfer":
      return "bg-rose-50 text-rose-800 ring-rose-200";
    case "total_empty_without_po":
      return "bg-blue-50 text-blue-800 ring-blue-200";
    case "supplier_delivery":
      return "bg-teal-50 text-teal-800 ring-teal-200";
    default:
      return "bg-slate-50 text-slate-700 ring-slate-200";
  }
}

export { formatDeDate as formatStockForecastDate };

import {
  DEFAULT_AMAZON_TARGET_COVER_DAYS,
  DEFAULT_TRANSFER_LEAD_DAYS,
  amazonShipQtyForTargetCover,
  supplierOrderQtyAfterPipeline,
} from "./local-stock.ts";
import { roundUpToCartons } from "./carton-specs.ts";
import { isoWeekFromDateISO, planArrivalShipmentReorder } from "./inventory-forecast.ts";
import { isActiveListing } from "./listing-activity.ts";

export type StockStatus = "out" | "critical" | "warning" | "healthy" | "no_sales";

export type InventoryOverviewItem = {
  asin: string;
  sku: string;
  imageUrl: string | null;
  productName: string | null;
  marketplace: string;
  snapshotDate: string | null;
  available: number;
  total: number;
  reserved: number;
  pendingCustomerOrders: number;
  inbound: number;
  /** Local / 3PL warehouse units. */
  localQty?: number;
  /** Open supplier PO units. */
  onOrderUnits?: number;
  /** Days local warehouse → Amazon. */
  transferLeadDays?: number;
  /** Desired Amazon FBA cover days for ship-qty recommendations. */
  amazonTargetCoverDays?: number;
  /** Extra supplier charge cover days (Stammdaten Puffer). */
  bufferTimeDays?: number | null;
  /** Units per carton for rounding supplier orders. */
  unitsPerCarton?: number | null;
  /** Date the open supplier PO was placed (YYYY-MM-DD). */
  onOrderOrderedAt?: string | null;
  units30: number;
  units90: number;
  dailySales30: number;
  /** Active days used for recent tempo (for charge qty parity with Nachbestellung). */
  recentTempoDays?: number;
  forecastDailySales: number;
  forecastMethod: "seasonal" | "hybrid" | "recent" | "none";
  growthFactor: number;
  growthPercent: number;
  comparisonCurrentUnits: number;
  comparisonPreviousUnits: number;
  /** Cover with available + inbound (Amazon FBA). Weekly LY+growth (tempo fallback). */
  daysOfCover: number | null;
  estimatedOosDate: string | null;
  /** Cover from sellable stock only (without inbound). */
  daysOfCoverOnHand: number | null;
  estimatedOosDateOnHand: string | null;
  /**
   * Cover including local warehouse (after transfer), excluding open supplier POs.
   * Used for Lieferverzug vs Bestell-Ankunft.
   */
  daysOfCoverAmazonAndLocal: number | null;
  estimatedOosDateAmazonAndLocal: string | null;
  /**
   * Cover including local + open supplier POs (full pipeline).
   */
  daysOfCoverWithLocal: number | null;
  estimatedOosDateWithLocal: string | null;
  /** Supplier lead time in days (production + shipping), if configured. */
  supplierLeadDays?: number | null;
  /**
   * Recommended local→Amazon ship qty (same engine as Nachbestellung / LY weeks).
   * Set by overview API when week maps are available.
   */
  recommendedShipQty?: number | null;
  /**
   * Recommended supplier order qty (same engine as Nachbestellung / LY weeks).
   * Set by overview API when week maps are available.
   */
  recommendedOrderQty?: number | null;
  status: StockStatus;
};

/** Sellable now + inbound already on the way to Amazon. */
export function effectiveInventoryUnits(available: number, inbound: number): number {
  return Math.max(0, Number(available) || 0) + Math.max(0, Number(inbound) || 0);
}

export function classifyStockStatus(
  available: number,
  inbound: number,
  daysOfCover: number | null,
  localQty = 0,
): StockStatus {
  const amazon = effectiveInventoryUnits(available, inbound);
  const local = Math.max(0, Math.floor(Number(localQty) || 0));
  if (amazon <= 0 && local <= 0) return "out";
  if (daysOfCover === null) return "no_sales";
  if (daysOfCover <= 30) return "critical";
  if (daysOfCover <= 60) return "warning";
  return "healthy";
}

export type InventoryOverviewResponse = {
  ok: boolean;
  error?: string;
  snapshotDate: string | null;
  generatedAt: string;
  items: InventoryOverviewItem[];
};

export type InventoryKpis = {
  units30: number;
  available: number;
  atRisk: number;
  out: number;
  critical: number;
  warning: number;
  growthPercent: number | null;
  comparisonCurrent: number;
  comparisonPrevious: number;
};

const riskRank: Record<StockStatus, number> = {
  out: 0,
  critical: 1,
  warning: 2,
  healthy: 3,
  no_sales: 4,
};

export function summarizeInventoryKpis(items: InventoryOverviewItem[]): InventoryKpis {
  let units30 = 0;
  let available = 0;
  let out = 0;
  let critical = 0;
  let warning = 0;
  let comparisonCurrent = 0;
  let comparisonPrevious = 0;

  for (const item of items) {
    units30 += Math.max(0, item.units30 || 0);
    available += Math.max(0, item.available || 0);
    comparisonCurrent += Math.max(0, item.comparisonCurrentUnits || 0);
    comparisonPrevious += Math.max(0, item.comparisonPreviousUnits || 0);
    if (item.status === "out") out += 1;
    else if (item.status === "critical") critical += 1;
    else if (item.status === "warning") warning += 1;
  }

  let growthPercent: number | null = null;
  if (comparisonPrevious > 0) {
    growthPercent = ((comparisonCurrent - comparisonPrevious) / comparisonPrevious) * 100;
  } else if (comparisonCurrent > 0) {
    growthPercent = 100;
  } else if (items.length > 0) {
    growthPercent = 0;
  }

  return {
    units30,
    available,
    atRisk: out + critical,
    out,
    critical,
    warning,
    growthPercent,
    comparisonCurrent,
    comparisonPrevious,
  };
}

export function rankInventoryRisk(items: InventoryOverviewItem[]): InventoryOverviewItem[] {
  return [...items].sort((a, b) => {
    const rankDiff = riskRank[a.status] - riskRank[b.status];
    if (rankDiff !== 0) return rankDiff;
    const coverA = a.daysOfCover ?? Number.MAX_SAFE_INTEGER;
    const coverB = b.daysOfCover ?? Number.MAX_SAFE_INTEGER;
    if (coverA !== coverB) return coverA - coverB;
    return (b.units30 || 0) - (a.units30 || 0);
  });
}

export function selectOosRiskItems(
  items: InventoryOverviewItem[],
  limit = 8,
): InventoryOverviewItem[] {
  return rankInventoryRisk(items)
    .filter((item) => item.status === "out" || item.status === "critical")
    .slice(0, limit);
}

/** Scope overview rows to the ASIN of a selected SKU (all SKUs under that ASIN). */
export function filterItemsBySelectedSku(
  items: InventoryOverviewItem[],
  selectedSku: string | null | undefined,
): InventoryOverviewItem[] {
  const sku = selectedSku?.trim();
  if (!sku) return items;
  const match = items.find((item) => item.sku === sku);
  if (!match?.asin) return items.filter((item) => item.sku === sku);
  return items.filter((item) => item.asin === match.asin);
}

export type CoverActionKpis = {
  amazonCoverDays: number | null;
  gesamtCoverDays: number | null;
  /** Days until local→Amazon ship deadline; null if no local stock / no tempo. */
  daysUntilShip: number | null;
  shipUnavailableReason: "no_local" | "no_tempo" | null;
  /** Days until supplier order deadline; null if open PO / no lead / no tempo. */
  daysUntilOrder: number | null;
  orderUnavailableReason: "already_ordered" | "no_lead" | "no_tempo" | null;
};

/** Portfolio action plan (Alle Produkte): concrete next todos, soonest first. */
export type InventoryActionKind =
  | "sold_out"
  | "ship_amazon"
  | "order_supplier"
  | "missing_lead";

export type InventoryActionPlanRow = {
  id: string;
  sku: string;
  asin: string;
  productName: string | null;
  imageUrl: string | null;
  kind: InventoryActionKind;
  /** Days until action is due; ≤0 means now / overdue. */
  daysUntil: number;
  actionLabel: string;
  /** Short supporting context for the table. */
  context: string;
  /** Recommended units for this action (ship or order). */
  qtySuggested: number | null;
  /** e.g. "Ziel 30 T" / "Charge 150 T". */
  qtyBasis: string | null;
};

/** Show upcoming logistics actions within this window (plus overdue). */
export const ACTION_PLAN_HORIZON_DAYS = 21;

/**
 * Action plan only for listings with real recent tempo (last ~30 days).
 * Also drops placeholder-style SKUs (e.g. 15-FBFB-FBFB) that only have
 * residual local stock / stale catalog rows.
 */
export function isActionableForPlan(item: InventoryOverviewItem): boolean {
  const sku = String(item.sku || "").trim().toUpperCase();
  if (looksLikePlaceholderSku(sku)) return false;

  if (
    !isActiveListing({
      available: item.available,
      inbound: item.inbound,
      units30: item.units30,
      units90: item.units90,
      localQty: item.localQty,
      onOrderUnits: item.onOrderUnits,
    })
  ) {
    return false;
  }
  if (item.status === "no_sales") return false;
  const units30 = Math.max(0, Number(item.units30) || 0);
  const daily = Math.max(0, Number(item.dailySales30) || 0);
  // Recent velocity required — 90d residue alone is not enough.
  return units30 > 0 && daily > 0;
}

/** e.g. "15-FBFB-FBFB" / "XX-TEST-TEST" — repeated token, not a real listing. */
function looksLikePlaceholderSku(sku: string): boolean {
  const parts = sku.split("-").filter(Boolean);
  if (parts.length < 3) return false;
  const tokens = parts.slice(1);
  if (tokens.length < 2) return false;
  return tokens.every((part) => part === tokens[0]);
}

const actionKindRank: Record<InventoryActionKind, number> = {
  sold_out: 0,
  ship_amazon: 1,
  order_supplier: 2,
  missing_lead: 3,
};

export function actionPlanWhenLabel(daysUntil: number): string {
  const days = Math.round(daysUntil);
  if (days <= 0) return "jetzt";
  if (days === 1) return "in 1 Tag";
  return `in ${days} Tagen`;
}

function dailyRateForQty(item: InventoryOverviewItem): number {
  const forecast = Math.max(0, Number(item.forecastDailySales) || 0);
  if (forecast > 0) return forecast;
  return Math.max(0, Number(item.dailySales30) || 0);
}

function berlinTodayISO(): string {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: "Europe/Berlin",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(new Date());
}

/** Units to send local→Amazon: full LY/tempo charge for Amazon-Zielreichweite (not FBA gap). */
export function suggestedAmazonShipQty(
  item: InventoryOverviewItem,
  options?: {
    previousYearWeekTotals?: Map<number, number>;
    currentYearWeekTotals?: Map<number, number>;
    todayISO?: string;
  },
): number {
  const precomputed = Number(item.recommendedShipQty);
  if (
    (!options?.previousYearWeekTotals || !options?.currentYearWeekTotals) &&
    Number.isFinite(precomputed) &&
    precomputed > 0
  ) {
    return Math.floor(precomputed);
  }

  const localQty = Math.max(0, Math.floor(Number(item.localQty) || 0));
  if (localQty <= 0) return 0;

  const target = Math.max(
    1,
    Math.round(Number(item.amazonTargetCoverDays) || DEFAULT_AMAZON_TARGET_COVER_DAYS),
  );
  const rate = dailyRateForQty(item);
  if (rate <= 0) return 0;

  const tempoDays = Math.max(1, Math.round(Number(item.recentTempoDays) || 14));
  const recent30Units = Math.max(0, Math.round(rate * tempoDays));
  const todayISO = options?.todayISO || berlinTodayISO();
  const { isoYear, isoWeek } = isoWeekFromDateISO(todayISO);
  const inventory =
    Math.max(0, Math.floor(Number(item.available) || 0)) +
    Math.max(0, Math.floor(Number(item.inbound) || 0));

  // Same weekly LY engine as Lieferanten-Nachbestellung, but cover window = Amazon-Ziel
  // starting now (leadDays=0). Growth applies when previous-year week maps are provided.
  const plan = planArrivalShipmentReorder({
    inventory,
    currentIsoYear: isoYear,
    currentIsoWeek: isoWeek,
    previousYearWeekTotals: options?.previousYearWeekTotals ?? new Map(),
    currentYearWeekTotals: options?.currentYearWeekTotals ?? new Map(),
    recent30Units,
    recentTempoDays: tempoDays,
    leadTimeDays: 0,
    bufferDays: target,
  });

  const raw =
    plan && plan.reorderQty > 0
      ? plan.reorderQty
      : amazonShipQtyForTargetCover({ dailyRate: rate, targetCoverDays: target });
  const rounded = roundUpToCartons(raw, item.unitsPerCarton ?? null).orderQty;
  return Math.min(rounded, localQty);
}

/**
 * Supplier order qty — same engine as Dashboard „Nachbestellung“:
 * planArrivalShipmentReorder (Wochen-Charge Lead+Puffer ab Ankunft) → minus open PO → Karton-Aufrundung.
 * Without LY week maps, falls back to recent tempo (same as a new listing on the dashboard).
 */
export function suggestedSupplierOrderQty(
  item: InventoryOverviewItem,
  options?: {
    previousYearWeekTotals?: Map<number, number>;
    currentYearWeekTotals?: Map<number, number>;
    todayISO?: string;
    /** When false, return the full charge before subtracting open supplier POs. */
    deductOpenPurchaseOrders?: boolean;
  },
): number {
  const deductOpenPo = options?.deductOpenPurchaseOrders !== false;
  const precomputed = Number(item.recommendedOrderQty);
  if (
    deductOpenPo &&
    (!options?.previousYearWeekTotals || !options?.currentYearWeekTotals) &&
    Number.isFinite(precomputed) &&
    precomputed > 0
  ) {
    return Math.floor(precomputed);
  }

  const lead = Number(item.supplierLeadDays);
  if (!Number.isFinite(lead) || lead <= 0) return 0;
  const rate = dailyRateForQty(item);
  if (rate <= 0) return 0;

  const buffer = Math.max(0, Math.round(Number(item.bufferTimeDays) || 0));
  const tempoDays = Math.max(1, Math.round(Number(item.recentTempoDays) || 14));
  // Prefer units that match tempo×days (dashboard recent30), not calendar units30.
  const recent30Units = Math.max(0, Math.round(rate * tempoDays));
  const todayISO = options?.todayISO || berlinTodayISO();
  const { isoYear, isoWeek } = isoWeekFromDateISO(todayISO);
  const inventory =
    Math.max(0, Math.floor(Number(item.available) || 0)) +
    Math.max(0, Math.floor(Number(item.inbound) || 0));

  const plan = planArrivalShipmentReorder({
    inventory,
    currentIsoYear: isoYear,
    currentIsoWeek: isoWeek,
    previousYearWeekTotals: options?.previousYearWeekTotals ?? new Map(),
    currentYearWeekTotals: options?.currentYearWeekTotals ?? new Map(),
    recent30Units,
    recentTempoDays: tempoDays,
    leadTimeDays: lead,
    bufferDays: buffer,
  });
  if (!plan || plan.reorderQty <= 0) return 0;

  const chargeQty = deductOpenPo
    ? supplierOrderQtyAfterPipeline({
        rawChargeQty: plan.reorderQty,
        onOrderUnits: item.onOrderUnits ?? 0,
      })
    : Math.max(0, Math.ceil(plan.reorderQty));
  return roundUpToCartons(chargeQty, item.unitsPerCarton ?? null).orderQty;
}

/**
 * Build a chronological todo list for the portfolio view.
 * One SKU can contribute multiple rows (e.g. ship + later order).
 */
export function buildInventoryActionPlan(
  items: InventoryOverviewItem[],
  options?: { horizonDays?: number },
): InventoryActionPlanRow[] {
  const horizon = options?.horizonDays ?? ACTION_PLAN_HORIZON_DAYS;
  const rows: InventoryActionPlanRow[] = [];

  for (const item of items) {
    if (!isActionableForPlan(item)) continue;

    const available = Math.max(0, Math.floor(Number(item.available) || 0));
    const inbound = Math.max(0, Math.floor(Number(item.inbound) || 0));
    const localQty = Math.max(0, Math.floor(Number(item.localQty) || 0));
    const onOrder = Math.max(0, Math.floor(Number(item.onOrderUnits) || 0));
    const lead = Number(item.supplierLeadDays);
    const hasLead = Number.isFinite(lead) && lead > 0;
    const targetCover = Math.max(
      1,
      Math.round(Number(item.amazonTargetCoverDays) || DEFAULT_AMAZON_TARGET_COVER_DAYS),
    );
    const buffer = Math.max(0, Math.round(Number(item.bufferTimeDays) || 0));
    const base = {
      sku: item.sku,
      asin: item.asin,
      productName: item.productName,
      imageUrl: item.imageUrl,
    };

    if (available + inbound <= 0 && localQty <= 0 && onOrder <= 0) {
      const orderQty = hasLead ? suggestedSupplierOrderQty(item) : null;
      // Empty + no order qty → not an actionable todo
      if (!(orderQty != null && orderQty > 0) && hasLead) {
        continue;
      }
      if (!hasLead) {
        // Still prompt for Stammdaten only when there is recent tempo (already gated)
        rows.push({
          ...base,
          id: `${item.sku}:missing_lead`,
          kind: "missing_lead",
          daysUntil: 0,
          actionLabel: "Leadzeit hinterlegen",
          context: "Stammdaten unvollständig",
          qtySuggested: null,
          qtyBasis: null,
        });
        continue;
      }
      rows.push({
        ...base,
        id: `${item.sku}:sold_out`,
        kind: "sold_out",
        daysUntil: 0,
        actionLabel: "Produkt nachbestellen",
        context: "kein Bestand / kein Zulauf",
        qtySuggested: orderQty,
        qtyBasis: `Charge ${Math.round(lead) + buffer} T`,
      });
      continue;
    }

    const daysUntilShip = daysUntilAmazonShip(item);
    if (daysUntilShip != null && daysUntilShip <= horizon && localQty > 0) {
      const shipQty = suggestedAmazonShipQty(item);
      if (shipQty > 0) {
        rows.push({
          ...base,
          id: `${item.sku}:ship_amazon`,
          kind: "ship_amazon",
          daysUntil: daysUntilShip,
          actionLabel: "Amz Lager senden",
          context: `${localQty} Stk lokal`,
          qtySuggested: shipQty,
          qtyBasis: `Ziel ${targetCover} T`,
        });
      }
    }

    const daysUntilOrder = daysUntilSupplierOrderDeadline(item);
    if (daysUntilOrder != null && daysUntilOrder <= horizon) {
      const orderQty = suggestedSupplierOrderQty(item);
      if (orderQty > 0) {
        const cover = item.daysOfCoverAmazonAndLocal ?? item.daysOfCover;
        const chargeDays = Math.round(lead) + buffer;
        rows.push({
          ...base,
          id: `${item.sku}:order_supplier`,
          kind: "order_supplier",
          daysUntil: daysUntilOrder,
          actionLabel: "Lieferant nachbestellen",
          context:
            cover != null
              ? `Gesamt-Reichweite ${Math.round(cover)} T · Lead ${Math.round(lead)} T`
              : `Lead ${Math.round(lead)} T`,
          qtySuggested: orderQty,
          qtyBasis: `Charge ${chargeDays} T`,
        });
      }
    } else if (
      !hasLead &&
      (item.status === "out" || item.status === "critical") &&
      onOrder <= 0
    ) {
      rows.push({
        ...base,
        id: `${item.sku}:missing_lead`,
        kind: "missing_lead",
        daysUntil: 0,
        actionLabel: "Leadzeit hinterlegen",
        context: "Stammdaten unvollständig",
        qtySuggested: null,
        qtyBasis: null,
      });
    }
  }

  return rows.sort((a, b) => {
    const aDue = a.daysUntil <= 0;
    const bDue = b.daysUntil <= 0;
    if (aDue !== bDue) return aDue ? -1 : 1;
    if (aDue && bDue) {
      return (
        actionKindRank[a.kind] - actionKindRank[b.kind] || a.sku.localeCompare(b.sku)
      );
    }
    return (
      a.daysUntil - b.daysUntil ||
      actionKindRank[a.kind] - actionKindRank[b.kind] ||
      a.sku.localeCompare(b.sku)
    );
  });
}

function minFinite(values: Array<number | null | undefined>): number | null {
  let min: number | null = null;
  for (const value of values) {
    if (value == null || !Number.isFinite(value)) continue;
    if (min == null || value < min) min = value;
  }
  return min;
}

/** Days left before Amazon cover drops below transfer lead (ship from local). */
export function daysUntilAmazonShip(item: InventoryOverviewItem): number | null {
  const localQty = Math.max(0, Number(item.localQty) || 0);
  if (localQty <= 0) return null;
  if (item.daysOfCover == null) return null;
  const transfer = Math.max(0, Number(item.transferLeadDays) || DEFAULT_TRANSFER_LEAD_DAYS);
  return Math.round(item.daysOfCover) - Math.round(transfer);
}

/** Days left before Amazon+local cover drops below supplier lead (place PO). */
export function daysUntilSupplierOrderDeadline(item: InventoryOverviewItem): number | null {
  if (Math.max(0, Number(item.onOrderUnits) || 0) > 0) return null;
  const lead = Number(item.supplierLeadDays);
  if (!Number.isFinite(lead) || lead <= 0) return null;
  const cover = item.daysOfCoverAmazonAndLocal ?? item.daysOfCover;
  if (cover == null) return null;
  return Math.round(cover) - Math.round(lead);
}

export function coverActionKpisForItem(item: InventoryOverviewItem): CoverActionKpis {
  const localQty = Math.max(0, Number(item.localQty) || 0);
  const onOrder = Math.max(0, Number(item.onOrderUnits) || 0);
  const lead = Number(item.supplierLeadDays);
  const hasLead = Number.isFinite(lead) && lead > 0;

  let shipUnavailableReason: CoverActionKpis["shipUnavailableReason"] = null;
  const daysUntilShip = daysUntilAmazonShip(item);
  if (daysUntilShip == null) {
    shipUnavailableReason = localQty <= 0 ? "no_local" : "no_tempo";
  }

  let orderUnavailableReason: CoverActionKpis["orderUnavailableReason"] = null;
  const daysUntilOrder = daysUntilSupplierOrderDeadline(item);
  if (daysUntilOrder == null) {
    if (onOrder > 0) orderUnavailableReason = "already_ordered";
    else if (!hasLead) orderUnavailableReason = "no_lead";
    else orderUnavailableReason = "no_tempo";
  }

  return {
    amazonCoverDays: item.daysOfCover,
    gesamtCoverDays: item.daysOfCoverAmazonAndLocal ?? item.daysOfCover,
    daysUntilShip,
    shipUnavailableReason,
    daysUntilOrder,
    orderUnavailableReason,
  };
}

/** Portfolio view: shortest covers / soonest actions across items. */
export function coverActionKpisForItems(items: InventoryOverviewItem[]): CoverActionKpis {
  if (items.length === 1) return coverActionKpisForItem(items[0]!);

  const amazonCoverDays = minFinite(items.map((item) => item.daysOfCover));
  const gesamtCoverDays = minFinite(
    items.map((item) => item.daysOfCoverAmazonAndLocal ?? item.daysOfCover),
  );

  const shipCandidates = items
    .map((item) => daysUntilAmazonShip(item))
    .filter((days): days is number => days != null);
  const daysUntilShip = shipCandidates.length ? Math.min(...shipCandidates) : null;

  const orderCandidates = items
    .map((item) => daysUntilSupplierOrderDeadline(item))
    .filter((days): days is number => days != null);
  const daysUntilOrder = orderCandidates.length ? Math.min(...orderCandidates) : null;

  const anyLocal = items.some((item) => Math.max(0, Number(item.localQty) || 0) > 0);
  const anyOpenOrder = items.some((item) => Math.max(0, Number(item.onOrderUnits) || 0) > 0);
  const anyLead = items.some((item) => {
    const lead = Number(item.supplierLeadDays);
    return Number.isFinite(lead) && lead > 0;
  });

  return {
    amazonCoverDays,
    gesamtCoverDays,
    daysUntilShip,
    shipUnavailableReason:
      daysUntilShip != null ? null : anyLocal ? "no_tempo" : "no_local",
    daysUntilOrder,
    orderUnavailableReason:
      daysUntilOrder != null
        ? null
        : anyOpenOrder
          ? "already_ordered"
          : anyLead
            ? "no_tempo"
            : "no_lead",
  };
}

export function formatCoverDaysDe(days: number | null | undefined): string {
  if (days == null || !Number.isFinite(days)) return "–";
  const rounded = Math.max(0, Math.round(days));
  return rounded === 1 ? "1 Tag" : `${rounded} Tage`;
}

export function formatInDaysDe(days: number | null | undefined): string {
  if (days == null || !Number.isFinite(days)) return "–";
  const rounded = Math.round(days);
  if (rounded <= 0) return "jetzt";
  return rounded === 1 ? "in 1 Tag" : `in ${rounded} Tagen`;
}

export function inventoryActionHint(item: InventoryOverviewItem): string {
  const localQty = Math.max(0, Number(item.localQty) || 0);
  const onOrder = Math.max(0, Number(item.onOrderUnits) || 0);
  const transfer = Math.max(0, Number(item.transferLeadDays) || 7);
  const decisionCover = item.daysOfCoverWithLocal ?? item.daysOfCover;

  if (item.status === "out" && localQty <= 0 && onOrder <= 0) return "Jetzt bestellen";
  if (item.daysOfCover === null && decisionCover === null) return "Kein Absatztempo";

  if (
    localQty > 0 &&
    item.daysOfCover !== null &&
    item.daysOfCover <= transfer &&
    (decisionCover == null || decisionCover > transfer)
  ) {
    return `Amazon nachfüllen · lokal ${localQty} Stk`;
  }

  if (onOrder > 0 && decisionCover != null && decisionCover > 30) {
    return `Bestellung unterwegs · Pipeline ${decisionCover} Tage`;
  }

  const inboundHelps =
    item.inbound > 0 &&
    item.daysOfCoverOnHand !== null &&
    item.daysOfCover !== null &&
    item.daysOfCoverOnHand < item.daysOfCover;

  if (inboundHelps && item.daysOfCoverOnHand! <= 30 && item.daysOfCover! > 30) {
    return `Kein Nachschub nötig · Zulauf deckt ${item.daysOfCover} Tage`;
  }
  if (item.available <= 0 && item.inbound > 0) {
    return item.daysOfCover! <= 30
      ? `Nur Zulauf · reicht ${item.daysOfCover} Tage`
      : `Zulauf unterwegs · reicht ${item.daysOfCover} Tage`;
  }

  const cover = decisionCover ?? item.daysOfCover;
  if (cover == null) return "Kein Absatztempo";
  if (cover <= 14) {
    return localQty > 0
      ? `Amazon nachfüllen · Pipeline ${cover} Tage`
      : `Jetzt bestellen · reicht ${cover} Tage`;
  }
  if (cover <= 30) {
    return localQty > 0
      ? `Bald nachfüllen · Pipeline ${cover} Tage`
      : `Bald nachbestellen · reicht ${cover} Tage`;
  }
  if (cover <= 60) {
    return inboundHelps || localQty > 0
      ? `Beobachten · ${cover} Tage inkl. Pipeline`
      : `Beobachten · reicht ${cover} Tage`;
  }
  return inboundHelps || localQty > 0
    ? `Bestand reicht ${cover} Tage inkl. Pipeline`
    : `Bestand reicht ${cover} Tage`;
}

/** Short status chip when FBA is empty but inbound/local covers demand. */
export function inventoryStatusLabel(item: InventoryOverviewItem, fallback: string): string {
  if (item.available <= 0 && item.inbound > 0 && item.status !== "out") return "Zulauf";
  if (
    item.available <= 0 &&
    (item.localQty || 0) > 0 &&
    item.status !== "out"
  ) {
    return "Lokal";
  }
  return fallback;
}

import { classifyReorderTiming, type ReorderTiming } from "./carton-specs.ts";
import {
  daysUntilAmazonShip,
  daysUntilSupplierOrderDeadline,
  formatInDaysDe,
  type InventoryOverviewItem,
} from "./inventory-overview.ts";
import {
  classifyLocalStockAction,
  DEFAULT_TRANSFER_LEAD_DAYS,
  onOrderArrivalDelayDays,
  supplierDeliveryGap,
  type LocalStockAction,
} from "./local-stock.ts";

/**
 * User-facing logistics status (multi-echelon).
 *
 * | Situation                         | Status                  |
 * | Amazon + Gesamt leer              | Ausverkauft             |
 * | Amazon knapp, lokal ok            | Ins Amz Lager senden    |
 * | Gesamtbestand knapp               | Produkt nachbestellen   |
 * | Lieferung zu spät → echter OOS    | Stockout-Risiko         |
 * | Alles ausreichend                 | Abgedeckt               |
 * | Lead Time o. Ä. fehlt             | Daten fehlen            |
 */
export type CoverageHealth =
  | "covered"
  | "ship_to_amazon"
  | "reorder_product"
  | "stockout_risk"
  | "sold_out"
  | "missing_data";

export type CoverageHealthTone = "emerald" | "sky" | "amber" | "red" | "slate" | "violet";

export type CoverageHealthResult = {
  health: CoverageHealth;
  nextActionInDays: number | null;
  nextActionKind: "ship" | "order" | null;
  /** Full status line for dashboard. */
  label: string;
  /** Short chip for tables. */
  shortLabel: string;
  tone: CoverageHealthTone;
};

const SHORT: Record<CoverageHealth, string> = {
  covered: "Abgedeckt",
  ship_to_amazon: "Ins Amz Lager senden",
  reorder_product: "Produkt nachbestellen",
  stockout_risk: "Stockout-Risiko",
  sold_out: "Ausverkauft",
  missing_data: "Daten fehlen",
};

function toneFor(health: CoverageHealth): CoverageHealthTone {
  switch (health) {
    case "covered":
      return "emerald";
    case "ship_to_amazon":
      return "sky";
    case "reorder_product":
      return "amber";
    case "stockout_risk":
      return "red";
    case "sold_out":
      return "slate";
    case "missing_data":
      return "violet";
  }
}

function pickNextAction(
  daysUntilShip: number | null,
  daysUntilOrder: number | null,
): { days: number | null; kind: "ship" | "order" | null } {
  if (daysUntilShip != null && daysUntilOrder != null) {
    if (daysUntilShip <= daysUntilOrder) return { days: daysUntilShip, kind: "ship" };
    return { days: daysUntilOrder, kind: "order" };
  }
  if (daysUntilShip != null) return { days: daysUntilShip, kind: "ship" };
  if (daysUntilOrder != null) return { days: daysUntilOrder, kind: "order" };
  return { days: null, kind: null };
}

function resolveNextActionDays(
  next: { days: number | null; kind: "ship" | "order" | null },
  daysUntilShip: number | null,
  reorderTiming: ReorderTiming | null,
  stockAction: LocalStockAction | null,
): { days: number | null; kind: "ship" | "order" | null } {
  let days = next.days;
  let kind = next.kind;
  if (
    days == null &&
    reorderTiming?.daysUntilMustOrder != null &&
    stockAction !== "awaiting_supplier"
  ) {
    days = reorderTiming.daysUntilMustOrder;
    kind = "order";
  }
  if (days == null && daysUntilShip != null) {
    days = daysUntilShip;
    kind = "ship";
  }
  return { days, kind };
}

function buildLabel(
  health: CoverageHealth,
  opts: {
    nextActionInDays: number | null;
    stockAction: LocalStockAction | null;
  },
): string {
  const short = SHORT[health];
  const { nextActionInDays, stockAction } = opts;

  if (health === "covered") {
    if (stockAction === "awaiting_supplier") return `${short} · Bestellung unterwegs`;
    if (nextActionInDays != null) {
      return `${short} · nächste Aktion ${formatInDaysDe(nextActionInDays)}`;
    }
    return short;
  }

  if (
    (health === "ship_to_amazon" || health === "reorder_product") &&
    nextActionInDays != null
  ) {
    return `${short} · ${formatInDaysDe(nextActionInDays)}`;
  }

  return short;
}

/**
 * Multi-echelon coverage status for UI.
 * Transferbedarf ≠ Stockout-Risiko — letzteres nur bei echtem Lieferverzug / OOS-Lücke.
 */
export function classifyCoverageHealth({
  amazonAvailable,
  amazonInbound = 0,
  localQty = 0,
  onOrderUnits = 0,
  amazonDaysOfCover,
  amazonAndLocalDaysOfCover,
  stockAction,
  reorderTiming,
  deliveryGapDays = null,
  daysUntilShip = null,
  daysUntilOrder = null,
}: {
  amazonAvailable: number;
  amazonInbound?: number;
  localQty?: number;
  onOrderUnits?: number;
  amazonDaysOfCover: number | null;
  amazonAndLocalDaysOfCover: number | null;
  stockAction: LocalStockAction | null;
  reorderTiming: ReorderTiming | null;
  /** Positive = Lieferverzug (arrival after Amazon+local OOS). */
  deliveryGapDays?: number | null;
  daysUntilShip?: number | null;
  daysUntilOrder?: number | null;
}): CoverageHealthResult {
  const amazonUnits =
    Math.max(0, Math.floor(Number(amazonAvailable) || 0)) +
    Math.max(0, Math.floor(Number(amazonInbound) || 0));
  const local = Math.max(0, Math.floor(Number(localQty) || 0));
  const onOrder = Math.max(0, Math.floor(Number(onOrderUnits) || 0));

  const next = resolveNextActionDays(
    pickNextAction(daysUntilShip, daysUntilOrder),
    daysUntilShip,
    reorderTiming,
    stockAction,
  );

  let health: CoverageHealth;

  if (amazonUnits <= 0 && local <= 0 && onOrder <= 0) {
    health = "sold_out";
  } else if (stockAction === "missing_lead") {
    health = "missing_data";
  } else if (deliveryGapDays != null && deliveryGapDays > 0) {
    // Nur echter Lieferverzug: Ankunft nach Amazon+Lokal-OOS
    health = "stockout_risk";
  } else if (stockAction === "replenish_amazon") {
    health = "ship_to_amazon";
  } else if (stockAction === "order_supplier") {
    health = "reorder_product";
  } else {
    // ok / awaiting_supplier — Pipeline trägt (auch bei kurzer Amazon-Reichweite)
    health = "covered";
  }

  const labelDays =
    health === "ship_to_amazon"
      ? daysUntilShip ?? next.days
      : health === "reorder_product"
        ? daysUntilOrder ?? next.days
        : next.days;

  return {
    health,
    nextActionInDays: next.days,
    nextActionKind: next.kind,
    label: buildLabel(health, {
      nextActionInDays: labelDays,
      stockAction,
    }),
    shortLabel: SHORT[health],
    tone: toneFor(health),
  };
}

/** Derive coverage health from an overview row (Bestandssteuerung / KPI context). */
export function coverageHealthFromOverviewItem(item: InventoryOverviewItem): CoverageHealthResult {
  const transfer = Math.max(0, Number(item.transferLeadDays) || DEFAULT_TRANSFER_LEAD_DAYS);
  const lead = Number(item.supplierLeadDays);
  const hasLead = Number.isFinite(lead) && lead > 0;
  const amazonAndLocal = item.daysOfCoverAmazonAndLocal ?? item.daysOfCover;
  const localQty = Math.max(0, Number(item.localQty) || 0);
  const onOrderUnits = Math.max(0, Number(item.onOrderUnits) || 0);
  const rate = Math.max(0, Number(item.forecastDailySales) || Number(item.dailySales30) || 0);

  const onOrderArrivesInDays =
    onOrderUnits > 0
      ? onOrderArrivalDelayDays({
          orderedAtISO: item.onOrderOrderedAt,
          supplierLeadDays: hasLead ? lead : null,
        })
      : null;

  const stockAction = hasLead
    ? classifyLocalStockAction({
        amazonDaysOfCover: item.daysOfCover,
        transferLeadDays: transfer,
        localQty,
        onOrderUnits,
        supplierLeadDays: lead,
        dailyRate: rate,
        amazonAndLocalDaysOfCover: amazonAndLocal,
        pipelineDaysOfCover: item.daysOfCoverWithLocal,
        onOrderArrivesInDays,
      })
    : "missing_lead";

  const reorderTiming = hasLead ? classifyReorderTiming(amazonAndLocal, lead) : null;
  const daysUntilShip = daysUntilAmazonShip(item);
  const daysUntilOrder = daysUntilSupplierOrderDeadline(item);

  const gap = hasLead
    ? supplierDeliveryGap({
        oosDaysAmazonAndLocal: amazonAndLocal,
        orderedAtISO: onOrderUnits > 0 ? item.onOrderOrderedAt : null,
        supplierLeadDays: lead,
      })
    : null;

  return classifyCoverageHealth({
    amazonAvailable: item.available,
    amazonInbound: item.inbound,
    localQty,
    onOrderUnits,
    amazonDaysOfCover: item.daysOfCover,
    amazonAndLocalDaysOfCover: amazonAndLocal,
    stockAction,
    reorderTiming,
    deliveryGapDays: gap?.gapDays ?? null,
    daysUntilShip,
    daysUntilOrder,
  });
}

/** Table / KPI: next local→Amazon ship countdown. */
export function amazonShipActionLabel(item: InventoryOverviewItem): string {
  const localQty = Math.max(0, Number(item.localQty) || 0);
  if (localQty <= 0) return "–";
  const days = daysUntilAmazonShip(item);
  if (days == null) return "–";
  return formatInDaysDe(days);
}

/** Table / KPI: next supplier PO countdown or open-order state. */
export function supplierOrderActionLabel(item: InventoryOverviewItem, nf?: Intl.NumberFormat): string {
  const onOrder = Math.max(0, Number(item.onOrderUnits) || 0);
  if (onOrder > 0) {
    const qty = nf ? nf.format(onOrder) : String(onOrder);
    return `bereits bestellt · ${qty} Stk`;
  }
  const days = daysUntilSupplierOrderDeadline(item);
  if (days == null) {
    const lead = Number(item.supplierLeadDays);
    if (!Number.isFinite(lead) || lead <= 0) return "Leadzeit fehlt";
    return "–";
  }
  return formatInDaysDe(days);
}

export const coverageHealthBadgeClass: Record<CoverageHealthTone, string> = {
  emerald: "bg-emerald-50 text-emerald-800 ring-emerald-200",
  sky: "bg-sky-50 text-sky-800 ring-sky-200",
  amber: "bg-amber-50 text-amber-900 ring-amber-200",
  red: "bg-red-50 text-red-800 ring-red-200",
  slate: "bg-slate-100 text-slate-700 ring-slate-200",
  violet: "bg-violet-50 text-violet-800 ring-violet-200",
};

export const coverageHealthTextClass: Record<CoverageHealthTone, string> = {
  emerald: "text-emerald-800",
  sky: "text-sky-800",
  amber: "text-amber-900",
  red: "text-red-800",
  slate: "text-slate-800",
  violet: "text-violet-800",
};

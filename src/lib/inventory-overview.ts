import { DEFAULT_TRANSFER_LEAD_DAYS } from "./local-stock.ts";

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
  /** Date the open supplier PO was placed (YYYY-MM-DD). */
  onOrderOrderedAt?: string | null;
  units30: number;
  units90: number;
  dailySales30: number;
  forecastDailySales: number;
  forecastMethod: "seasonal" | "hybrid" | "recent" | "none";
  growthFactor: number;
  growthPercent: number;
  comparisonCurrentUnits: number;
  comparisonPreviousUnits: number;
  /** Cover with available + inbound (Amazon FBA). */
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

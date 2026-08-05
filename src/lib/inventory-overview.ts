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
  units30: number;
  units90: number;
  dailySales30: number;
  forecastDailySales: number;
  forecastMethod: "seasonal" | "hybrid" | "recent" | "none";
  growthFactor: number;
  growthPercent: number;
  comparisonCurrentUnits: number;
  comparisonPreviousUnits: number;
  /** Cover with available + inbound (decision metric for Nachschub). */
  daysOfCover: number | null;
  estimatedOosDate: string | null;
  /** Cover from sellable stock only (without inbound). */
  daysOfCoverOnHand: number | null;
  estimatedOosDateOnHand: string | null;
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
): StockStatus {
  if (effectiveInventoryUnits(available, inbound) <= 0) return "out";
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
    .filter((item) => item.status === "out" || item.status === "critical" || item.status === "warning")
    .slice(0, limit);
}

export function inventoryActionHint(item: InventoryOverviewItem): string {
  if (item.status === "out") return "Jetzt bestellen";
  if (item.daysOfCover === null) return "Kein Absatztempo";

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
  if (item.daysOfCover <= 14) return `Jetzt bestellen · reicht ${item.daysOfCover} Tage`;
  if (item.daysOfCover <= 30) return `Bald nachbestellen · reicht ${item.daysOfCover} Tage`;
  if (item.daysOfCover <= 60) {
    return inboundHelps
      ? `Beobachten · ${item.daysOfCover} Tage inkl. Zulauf`
      : `Beobachten · reicht ${item.daysOfCover} Tage`;
  }
  return inboundHelps
    ? `Bestand reicht ${item.daysOfCover} Tage inkl. Zulauf`
    : `Bestand reicht ${item.daysOfCover} Tage`;
}

/** Short status chip when FBA is empty but inbound covers demand. */
export function inventoryStatusLabel(item: InventoryOverviewItem, fallback: string): string {
  if (item.available <= 0 && item.inbound > 0 && item.status !== "out") return "Zulauf";
  return fallback;
}

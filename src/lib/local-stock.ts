export const DEFAULT_TRANSFER_LEAD_DAYS = 7;

/** Default desired Amazon FBA cover used for ship-qty recommendations. */
export const DEFAULT_AMAZON_TARGET_COVER_DAYS = 30;

export type LocalStockRecord = {
  sellerSku: string;
  localQty: number;
  onOrderUnits: number;
  transferLeadDays: number;
  amazonTargetCoverDays: number;
  lastInboundSeen: number | null;
  updatedAt: string | null;
};

/**
 * Full Amazon replenishment charge for the target cover window (default 30 days).
 * Does NOT subtract current FBA stock — each send is a full demand window.
 * Uses week-ceil parity with the Nachbestellung planner. Capped at local qty.
 */
export function amazonShipQtyForTargetCover({
  dailyRate,
  targetCoverDays,
  localQty,
}: {
  amazonAvailable?: number;
  amazonInbound?: number;
  dailyRate: number;
  targetCoverDays: number;
  localQty?: number;
  recentTempoDays?: number;
}): number {
  const rate = Math.max(0, Number(dailyRate) || 0);
  const target = Math.max(1, Math.round(Number(targetCoverDays) || DEFAULT_AMAZON_TARGET_COVER_DAYS));
  if (rate <= 0) return 0;
  const coverWeeks = Math.max(1, Math.ceil(target / 7));
  const qty = Math.max(0, Math.ceil(coverWeeks * rate * 7));
  if (localQty == null) return qty;
  return Math.min(qty, Math.max(0, Math.floor(Number(localQty) || 0)));
}

export type InboundDeductionResult = {
  nextLocalQty: number;
  nextLastInboundSeen: number;
  deducted: number;
  /** True when first seeing inbound — no units deducted. */
  isBaseline: boolean;
};

/**
 * When Amazon inbound_total rises vs the last seen value, assume stock left the
 * local warehouse and deduct the delta (capped at local qty).
 * Inbound decreases (arrived at FBA) never restore local qty.
 */
export function computeInboundLocalDeduction(
  localQty: number,
  lastInboundSeen: number | null,
  currentInbound: number,
): InboundDeductionResult {
  const local = Math.max(0, Math.floor(Number(localQty) || 0));
  const inbound = Math.max(0, Math.floor(Number(currentInbound) || 0));

  if (lastInboundSeen == null) {
    return {
      nextLocalQty: local,
      nextLastInboundSeen: inbound,
      deducted: 0,
      isBaseline: true,
    };
  }

  const previous = Math.max(0, Math.floor(Number(lastInboundSeen) || 0));
  const delta = inbound - previous;
  if (delta <= 0) {
    return {
      nextLocalQty: local,
      nextLastInboundSeen: inbound,
      deducted: 0,
      isBaseline: false,
    };
  }

  const deducted = Math.min(delta, local);
  return {
    nextLocalQty: local - deducted,
    nextLastInboundSeen: inbound,
    deducted,
    isBaseline: false,
  };
}

export type LocalStockAction =
  | "replenish_amazon"
  | "order_supplier"
  | "awaiting_supplier"
  | "ok"
  | "missing_lead";

/**
 * Decide primary logistics action from Amazon cover, local stock, and open POs.
 *
 * Supplier decision uses Amazon+local cover (first continuous OOS) vs lead time —
 * same horizon as Lieferverzug. Full pipeline cover can jump over a sales gap when
 * an open PO arrives late, so it must not mark the SKU as "ok".
 */
export function classifyLocalStockAction({
  amazonDaysOfCover,
  transferLeadDays,
  localQty,
  onOrderUnits,
  supplierLeadDays,
  dailyRate,
  pipelineDaysOfCover,
  amazonAndLocalDaysOfCover,
  onOrderArrivesInDays,
}: {
  amazonDaysOfCover: number | null;
  transferLeadDays: number;
  localQty: number;
  onOrderUnits: number;
  supplierLeadDays: number | null;
  dailyRate: number;
  chargeCoverDays?: number | null;
  /** @deprecated Prefer amazonAndLocalDaysOfCover for supplier timing. */
  pipelineDaysOfCover?: number | null;
  /** Cover until first OOS from Amazon + local (excl. open supplier POs). */
  amazonAndLocalDaysOfCover?: number | null;
  /**
   * Days until open PO arrives (Lieferverzug Ankunft: Bestelldatum + Lieferzeit).
   * Null/undefined = unknown (no Bestelldatum) → do not treat PO as covering the gap.
   */
  onOrderArrivesInDays?: number | null;
}): LocalStockAction {
  const local = Math.max(0, Math.floor(Number(localQty) || 0));
  const onOrder = Math.max(0, Math.floor(Number(onOrderUnits) || 0));
  const transfer = Math.max(0, Math.round(Number(transferLeadDays) || DEFAULT_TRANSFER_LEAD_DAYS));
  const rate = Math.max(0, Number(dailyRate) || 0);

  if (supplierLeadDays == null || supplierLeadDays <= 0) return "missing_lead";

  const amazonCover = amazonDaysOfCover;
  const fbaNeedsReplenish =
    amazonCover !== null && amazonCover <= transfer && local > 0;

  const coverDaysFromUnits = (units: number) => {
    if (units <= 0) return 0;
    if (rate <= 0) return 0;
    return Math.floor(units / rate);
  };

  // null cover = no OOS inside the forecast horizon (often slow movers with stock),
  // not "0 Tage". Treating null as 0 falsely marks them as order_supplier.
  let amazonLocalCover: number | null = null;
  if (amazonAndLocalDaysOfCover != null && Number.isFinite(amazonAndLocalDaysOfCover)) {
    amazonLocalCover = Math.max(0, Math.round(amazonAndLocalDaysOfCover));
  } else if (
    pipelineDaysOfCover != null &&
    Number.isFinite(pipelineDaysOfCover) &&
    onOrder <= 0
  ) {
    amazonLocalCover = Math.max(0, Math.round(pipelineDaysOfCover));
  } else if (amazonCover != null) {
    amazonLocalCover = amazonCover + coverDaysFromUnits(local);
  } else if (rate > 0 && local > 0) {
    // Amazon cover unknown/null, but local units can still be timed against lead.
    amazonLocalCover = coverDaysFromUnits(local);
  }

  const shortVsLead =
    amazonLocalCover != null && amazonLocalCover < supplierLeadDays;

  const poArrival =
    onOrderArrivesInDays != null && Number.isFinite(onOrderArrivesInDays)
      ? Math.max(0, Math.round(onOrderArrivesInDays))
      : null;
  const openPoArrivesBeforeOos =
    onOrder > 0 &&
    poArrival != null &&
    amazonLocalCover != null &&
    poArrival <= amazonLocalCover;

  if (fbaNeedsReplenish && !shortVsLead) return "replenish_amazon";
  if (shortVsLead) {
    if (openPoArrivesBeforeOos) return "awaiting_supplier";
    return "order_supplier";
  }
  if (fbaNeedsReplenish) return "replenish_amazon";

  return "ok";
}

/** Days until open PO arrives (same horizon as Lieferverzug Ankunft = Bestelldatum + Lieferzeit). */
export function onOrderArrivalDelayDays({
  orderedAtISO,
  supplierLeadDays,
  todayISO,
  /** If true, add transfer to Amazon after local arrival (FBA-usable). Default false = Lieferverzug. */
  includeTransfer = false,
  transferLeadDays = DEFAULT_TRANSFER_LEAD_DAYS,
}: {
  orderedAtISO: string | null | undefined;
  supplierLeadDays: number | null | undefined;
  todayISO?: string;
  includeTransfer?: boolean;
  transferLeadDays?: number | null;
}): number | null {
  if (supplierLeadDays == null || supplierLeadDays < 0) return null;
  const day = String(orderedAtISO || "").slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(day)) return null;

  const today =
    todayISO ||
    new Intl.DateTimeFormat("en-CA", {
      timeZone: "Europe/Berlin",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    }).format(new Date());

  const orderedMs = Date.parse(`${day}T12:00:00Z`);
  const todayMs = Date.parse(`${today}T12:00:00Z`);
  if (!Number.isFinite(orderedMs) || !Number.isFinite(todayMs)) return null;

  const daysSinceOrder = Math.max(0, Math.round((todayMs - orderedMs) / 86_400_000));
  const remainingToLocal = Math.max(0, Math.round(supplierLeadDays) - daysSinceOrder);
  if (!includeTransfer) return remainingToLocal;
  const transfer = Math.max(0, Math.round(Number(transferLeadDays) || DEFAULT_TRANSFER_LEAD_DAYS));
  return remainingToLocal + transfer;
}

/** How many days an open supplier qty covers at the given daily rate. */
export function openOrderCoverDays(onOrderUnits: number, dailyRate: number): number | null {
  const units = Math.max(0, Math.floor(Number(onOrderUnits) || 0));
  const rate = Math.max(0, Number(dailyRate) || 0);
  if (units <= 0 || rate <= 0) return null;
  return Math.max(1, Math.round(units / rate));
}

/** Suggested supplier order qty after subtracting open POs only (local already in pipeline timing). */
export function supplierOrderQtyAfterPipeline({
  rawChargeQty,
  onOrderUnits,
}: {
  rawChargeQty: number;
  onOrderUnits: number;
  localQty?: number;
  subtractLocal?: boolean;
}): number {
  const raw = Math.max(0, Math.ceil(Number(rawChargeQty) || 0));
  const onOrder = Math.max(0, Math.floor(Number(onOrderUnits) || 0));
  return Math.max(0, raw - onOrder);
}

/** Expected arrival at local warehouse from order date + supplier lead. */
export function expectedLocalArrivalISO(
  orderedAtISO: string | null | undefined,
  supplierLeadDays: number | null | undefined,
): string | null {
  if (!orderedAtISO || supplierLeadDays == null || supplierLeadDays < 0) return null;
  const day = orderedAtISO.slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(day)) return null;
  const date = new Date(`${day}T12:00:00Z`);
  if (Number.isNaN(date.getTime())) return null;
  date.setUTCDate(date.getUTCDate() + Math.round(supplierLeadDays));
  return date.toISOString().slice(0, 10);
}

/** Compare Amazon+local OOS date with supplier arrival (order date + lead). */
export function supplierDeliveryGap({
  oosDaysAmazonAndLocal,
  orderedAtISO,
  supplierLeadDays,
  todayISO,
}: {
  oosDaysAmazonAndLocal: number | null;
  orderedAtISO: string | null | undefined;
  supplierLeadDays: number | null | undefined;
  todayISO?: string;
}): {
  oosDateISO: string | null;
  arrivalDateISO: string | null;
  /** Positive = days after OOS before arrival (Sales-Lücke). Negative = days buffer before OOS. */
  gapDays: number | null;
  hasOpenOrder: boolean;
} {
  const today =
    todayISO ||
    new Intl.DateTimeFormat("en-CA", {
      timeZone: "Europe/Berlin",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    }).format(new Date());

  if (oosDaysAmazonAndLocal == null || supplierLeadDays == null || supplierLeadDays < 0) {
    return { oosDateISO: null, arrivalDateISO: null, gapDays: null, hasOpenOrder: false };
  }

  const oosDate = new Date(`${today}T12:00:00Z`);
  oosDate.setUTCDate(oosDate.getUTCDate() + Math.max(0, Math.round(oosDaysAmazonAndLocal)));
  const oosDateISO = oosDate.toISOString().slice(0, 10);

  const hasOpenOrder = Boolean(orderedAtISO && /^\d{4}-\d{2}-\d{2}/.test(String(orderedAtISO)));
  const arrivalDateISO = hasOpenOrder
    ? expectedLocalArrivalISO(orderedAtISO, supplierLeadDays)
    : expectedLocalArrivalISO(today, supplierLeadDays);

  if (!arrivalDateISO) {
    return { oosDateISO, arrivalDateISO: null, gapDays: null, hasOpenOrder };
  }

  const oosMs = Date.parse(`${oosDateISO}T12:00:00Z`);
  const arrivalMs = Date.parse(`${arrivalDateISO}T12:00:00Z`);
  if (!Number.isFinite(oosMs) || !Number.isFinite(arrivalMs)) {
    return { oosDateISO, arrivalDateISO, gapDays: null, hasOpenOrder };
  }

  return {
    oosDateISO,
    arrivalDateISO,
    gapDays: Math.round((arrivalMs - oosMs) / 86_400_000),
    hasOpenOrder,
  };
}

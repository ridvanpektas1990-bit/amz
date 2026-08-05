export type CartonSpec = {
  sellerSku: string;
  unitsPerCarton: number | null;
  cartonLenCm: number | null;
  cartonWCm: number | null;
  cartonHCm: number | null;
  cartonWeightKg: number | null;
  productionTimeDays: number | null;
  shippingTimeDays: number | null;
  /** Extra cover days after lead time for shipment sizing. */
  bufferTimeDays: number | null;
  updatedAt: string | null;
};

/** Days the ordered charge should cover = lead + buffer. */
export function chargeCoverDaysFromSpec(
  spec: Pick<CartonSpec, "productionTimeDays" | "shippingTimeDays" | "bufferTimeDays"> | null,
): number | null {
  const lead = leadTimeDaysFromSpec(spec);
  if (lead == null) return null;
  const buffer = Math.max(0, Math.round(Number(spec?.bufferTimeDays) || 0));
  return lead + buffer;
}

export type CartonSpecRow = CartonSpec & {
  asin: string | null;
  productName: string | null;
  imageUrl: string | null;
  available: number;
  inbound: number;
  hasSpec: boolean;
};

/** Convert calendar days to whole weeks (ceil). Used when weekly forecast needs weeks. */
export function daysToWeeks(days: number | null | undefined): number {
  const value = Math.max(0, Number(days) || 0);
  if (value <= 0) return 0;
  return Math.ceil(value / 7);
}

/** Lead time in days = production + shipping. */
export function leadTimeDaysFromSpec(
  spec: Pick<CartonSpec, "productionTimeDays" | "shippingTimeDays"> | null,
): number | null {
  if (!spec) return null;
  const days =
    Math.max(0, Number(spec.productionTimeDays) || 0) +
    Math.max(0, Number(spec.shippingTimeDays) || 0);
  if (days <= 0) return null;
  return days;
}

/** @deprecated Prefer leadTimeDaysFromSpec – kept for weekly forecast glue. */
export function leadTimeWeeksFromSpec(
  spec: Pick<CartonSpec, "productionTimeDays" | "shippingTimeDays"> | null,
): number | null {
  const days = leadTimeDaysFromSpec(spec);
  return days == null ? null : daysToWeeks(days);
}

/** When to place the next PO relative to OOS and lead time. */
export type ReorderTimingStatus = "ok" | "order_now" | "too_late" | "already_oos" | "no_demand";

export type ReorderTiming = {
  daysUntilOos: number | null;
  leadDays: number;
  /** daysUntilOos − leadDays; null if unknown */
  daysUntilMustOrder: number | null;
  status: ReorderTimingStatus;
};

export function classifyReorderTiming(
  daysUntilOos: number | null,
  leadDays: number,
): ReorderTiming {
  const lead = Math.max(0, Math.round(Number(leadDays) || 0));
  if (daysUntilOos === null) {
    return { daysUntilOos: null, leadDays: lead, daysUntilMustOrder: null, status: "no_demand" };
  }
  const untilOos = Math.max(0, Math.round(daysUntilOos));
  if (untilOos === 0) {
    return { daysUntilOos: 0, leadDays: lead, daysUntilMustOrder: -lead, status: "already_oos" };
  }
  const daysUntilMustOrder = untilOos - lead;
  if (daysUntilMustOrder < 0) {
    return { daysUntilOos: untilOos, leadDays: lead, daysUntilMustOrder, status: "too_late" };
  }
  if (daysUntilMustOrder === 0) {
    return { daysUntilOos: untilOos, leadDays: lead, daysUntilMustOrder: 0, status: "order_now" };
  }
  return { daysUntilOos: untilOos, leadDays: lead, daysUntilMustOrder, status: "ok" };
}

export type CartonRoundResult = {
  rawQty: number;
  orderQty: number;
  unitsPerCarton: number | null;
  cartons: number | null;
  rounded: boolean;
};

/** Round order quantity up to full cartons. */
export function roundUpToCartons(rawQty: number, unitsPerCarton: number | null | undefined): CartonRoundResult {
  const raw = Math.max(0, Math.ceil(Number(rawQty) || 0));
  const upc = Math.max(0, Math.floor(Number(unitsPerCarton) || 0));
  if (upc <= 0 || raw <= 0) {
    return { rawQty: raw, orderQty: raw, unitsPerCarton: upc > 0 ? upc : null, cartons: null, rounded: false };
  }
  const cartons = Math.ceil(raw / upc);
  const orderQty = cartons * upc;
  return {
    rawQty: raw,
    orderQty,
    unitsPerCarton: upc,
    cartons,
    rounded: orderQty !== raw,
  };
}

export function buildSupplierOrderMessage({
  productName,
  sku,
  asin,
  orderQty,
  cartons,
  unitsPerCarton,
  productionDays,
  shippingDays,
}: {
  productName?: string | null;
  sku: string;
  asin?: string | null;
  orderQty: number;
  cartons?: number | null;
  unitsPerCarton?: number | null;
  productionDays?: number | null;
  shippingDays?: number | null;
}): string {
  const title = (productName || sku).trim();
  const lines = [
    `Bestellung / Purchase Order`,
    ``,
    `Produkt: ${title}`,
    `SKU: ${sku}`,
  ];
  if (asin) lines.push(`ASIN: ${asin}`);
  lines.push(`Menge: ${orderQty} Stück`);
  if (cartons && unitsPerCarton) {
    lines.push(`Kartons: ${cartons} × ${unitsPerCarton} Stück`);
  }
  const leadDays =
    Math.max(0, Number(productionDays) || 0) + Math.max(0, Number(shippingDays) || 0);
  if (leadDays > 0) {
    lines.push(`Erwartete Lead Time: ca. ${leadDays} Tage (Produktion + Versand)`);
  }
  lines.push(``, `Bitte um Auftragsbestätigung und voraussichtliches Versanddatum.`);
  return lines.join("\n");
}

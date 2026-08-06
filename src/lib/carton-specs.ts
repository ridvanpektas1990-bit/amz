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
  /** Local / 3PL warehouse units (not Amazon). */
  localQty: number;
  /** Units already ordered at supplier, not yet in local warehouse. */
  onOrderUnits: number;
  /** Days local warehouse → Amazon. */
  transferLeadDays: number;
  /** Date open supplier PO was placed (YYYY-MM-DD). */
  onOrderOrderedAt: string | null;
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

export type SupplierMessageLang = "de" | "en";

function formatCartonDimsCm(
  lenCm?: number | null,
  wCm?: number | null,
  hCm?: number | null,
): string | null {
  const len = Number(lenCm);
  const w = Number(wCm);
  const h = Number(hCm);
  if (!(len > 0) || !(w > 0) || !(h > 0)) return null;
  const fmt = (n: number) => (Number.isInteger(n) ? String(n) : String(n));
  return `${fmt(len)}x${fmt(w)}x${fmt(h)}cm`;
}

export function buildSupplierOrderMessage({
  productName,
  sku,
  asin,
  orderQty,
  cartons,
  unitsPerCarton,
  cartonLenCm,
  cartonWCm,
  cartonHCm,
  lang = "de",
}: {
  productName?: string | null;
  sku: string;
  asin?: string | null;
  orderQty: number;
  cartons?: number | null;
  unitsPerCarton?: number | null;
  cartonLenCm?: number | null;
  cartonWCm?: number | null;
  cartonHCm?: number | null;
  lang?: SupplierMessageLang;
}): string {
  const title = (productName || sku).trim();
  const en = lang === "en";
  const dims = formatCartonDimsCm(cartonLenCm, cartonWCm, cartonHCm);
  const upc = Math.max(0, Math.floor(Number(unitsPerCarton) || 0));
  const cartonCount = Math.max(0, Math.floor(Number(cartons) || 0));
  // Prefer carton count for the "Kartons:" line; fall back to order qty.
  const cartonLineQty = cartonCount > 0 ? cartonCount : orderQty;

  const lines = [
    en ? "New Purchase Order" : "Neue Bestellung",
    "",
    en ? `Product: ${title}` : `Produkt: ${title}`,
    `SKU: ${sku}`,
  ];
  if (asin) lines.push(`ASIN: ${asin}`);

  if (dims) {
    lines.push(
      en
        ? `Cartons: ${cartonLineQty} (${dims})`
        : `Kartons: ${cartonLineQty} Stück (${dims})`,
    );
  } else {
    lines.push(en ? `Cartons: ${cartonLineQty}` : `Kartons: ${cartonLineQty} Stück`);
  }

  if (upc > 0) {
    lines.push(
      en
        ? `Units per carton: ${upc} units`
        : `Stückzahl pro Karton: ${upc} Stück`,
    );
  }

  lines.push(
    en
      ? `Total products: ${orderQty} units`
      : `Anzahl Produkte insgesamt: ${orderQty} Stück`,
  );

  lines.push(
    "",
    en
      ? "Please confirm the order and the expected ship date."
      : "Bitte um Auftragsbestätigung und voraussichtliches Versanddatum.",
  );
  return lines.join("\n");
}

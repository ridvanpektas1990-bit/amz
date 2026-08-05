export type SyncSourceKey = "orders" | "order_items" | "inventory" | "etl";

export type SyncSourceStatus = {
  key: SyncSourceKey;
  label: string;
  lastDataAt: string | null;
  lastRunAt: string | null;
  status: string | null;
  ageDays: number | null;
  thresholdDays: number;
  stale: boolean;
  detail: string | null;
};

export type SyncStatusSnapshot = {
  sources: SyncSourceStatus[];
  warnings: string[];
  overall: "ok" | "warning" | "critical";
};

export function berlinTodayISO(now = new Date()): string {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: "Europe/Berlin",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(now);
}

export function ageInDays(dateISO: string | null, todayISO: string): number | null {
  if (!dateISO) return null;
  const start = Date.parse(`${dateISO.slice(0, 10)}T12:00:00Z`);
  const end = Date.parse(`${todayISO.slice(0, 10)}T12:00:00Z`);
  if (!Number.isFinite(start) || !Number.isFinite(end)) return null;
  return Math.max(0, Math.round((end - start) / 86_400_000));
}

export function buildSyncStatus({
  todayISO,
  maxOrderDate,
  maxOrderItemDate,
  maxInventorySnapshot,
  lastEtl,
}: {
  todayISO: string;
  /** Freshness of amazon_orders (SP-API Orders import). */
  maxOrderDate: string | null;
  /** Latest purchase_date_berlin in amazon_order_items (SKU sales). */
  maxOrderItemDate: string | null;
  maxInventorySnapshot: string | null;
  lastEtl: {
    status: string | null;
    startedAt: string | null;
    finishedAt: string | null;
    marketplace: string | null;
    periodYear: number | null;
    periodMonth: number | null;
  } | null;
}): SyncStatusSnapshot {
  const orderAge = ageInDays(maxOrderDate, todayISO);
  const orderItemAge = ageInDays(maxOrderItemDate, todayISO);
  const inventoryAge = ageInDays(maxInventorySnapshot, todayISO);

  const orderThreshold = 2;
  const orderItemThreshold = 2;
  const inventoryThreshold = 1;

  const ordersStale = orderAge === null || orderAge > orderThreshold;
  const orderItemsStale = orderItemAge === null || orderItemAge > orderItemThreshold;
  const inventoryStale = inventoryAge === null || inventoryAge > inventoryThreshold;

  const etlStatus = (lastEtl?.status || "").toLowerCase();
  const etlOk = ["success", "ok", "done", "succeeded", "completed"].includes(etlStatus);
  const etlRunAt = lastEtl?.finishedAt || lastEtl?.startedAt || null;
  const etlAge = ageInDays(etlRunAt ? etlRunAt.slice(0, 10) : null, todayISO);
  const etlStale = !lastEtl || !etlOk || (etlAge !== null && etlAge > 2);

  const sources: SyncSourceStatus[] = [
    {
      key: "orders",
      label: "Bestellungen (Import)",
      lastDataAt: maxOrderDate,
      lastRunAt: null,
      status: null,
      ageDays: orderAge,
      thresholdDays: orderThreshold,
      stale: ordersStale,
      detail: maxOrderDate
        ? `Letzte Order: ${maxOrderDate.slice(0, 10)}`
        : "Keine Bestellungen in amazon_orders",
    },
    {
      key: "order_items",
      label: "Order-Items (SKU-Absatz)",
      lastDataAt: maxOrderItemDate,
      lastRunAt: null,
      status: null,
      ageDays: orderItemAge,
      thresholdDays: orderItemThreshold,
      stale: orderItemsStale,
      detail: maxOrderItemDate
        ? `SKU-Positionen bis ${maxOrderItemDate.slice(0, 10)}`
        : "Keine amazon_order_items – Backfill nötig",
    },
    {
      key: "inventory",
      label: "Lagerbestand",
      lastDataAt: maxInventorySnapshot,
      lastRunAt: null,
      status: null,
      ageDays: inventoryAge,
      thresholdDays: inventoryThreshold,
      stale: inventoryStale,
      detail: maxInventorySnapshot
        ? `Letzter Snapshot: ${maxInventorySnapshot.slice(0, 10)}`
        : "Kein Bestandssnapshot gefunden",
    },
    {
      key: "etl",
      label: "ETL-Lauf",
      lastDataAt: null,
      lastRunAt: etlRunAt,
      status: lastEtl?.status || null,
      ageDays: etlAge,
      thresholdDays: 2,
      stale: etlStale,
      detail: lastEtl
        ? `Status ${lastEtl.status || "unbekannt"}${
            lastEtl.periodYear && lastEtl.periodMonth
              ? ` · Periode ${lastEtl.periodYear}-${String(lastEtl.periodMonth).padStart(2, "0")}`
              : ""
          }${lastEtl.marketplace ? ` · ${lastEtl.marketplace}` : ""}`
        : "Kein ETL-Lauf protokolliert",
    },
  ];

  const warnings: string[] = [];
  if (ordersStale) {
    warnings.push(
      orderAge === null
        ? "Bestell-Import fehlt."
        : `Bestell-Import ist ${orderAge} Tage alt (Schwelle ${orderThreshold} Tage).`,
    );
  }
  if (orderItemsStale) {
    warnings.push(
      orderItemAge === null
        ? "Order-Items fehlen (SKU-Charts brauchen Backfill)."
        : `Order-Items enden vor ${orderItemAge} Tagen (Schwelle ${orderItemThreshold} Tage).`,
    );
  }
  if (inventoryStale) {
    warnings.push(
      inventoryAge === null
        ? "Lagerbestand fehlt."
        : `Lagerbestand ist ${inventoryAge} Tage alt (Schwelle ${inventoryThreshold} Tag).`,
    );
  }
  if (etlStale) {
    warnings.push(
      !lastEtl
        ? "Kein erfolgreicher ETL-Lauf gefunden."
        : etlOk
          ? "ETL-Lauf ist veraltet."
          : `Letzter ETL-Lauf nicht erfolgreich (${lastEtl.status}).`,
    );
  }

  let overall: SyncStatusSnapshot["overall"] = "ok";
  if (ordersStale || inventoryStale) overall = "critical";
  else if (orderItemsStale || etlStale) overall = "warning";

  return { sources, warnings, overall };
}

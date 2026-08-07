export type PipelineKey = "orders" | "order_items" | "inventory";

export type DayRunStatus = "success" | "error" | "missing";

export type EtlRunRow = {
  status: string | null;
  started_at: string | null;
  finished_at: string | null;
  marketplace: string | null;
  period_year: number | null;
  period_month: number | null;
  run_log: string | null;
};

export type PipelineDayCell = {
  status: DayRunStatus;
  runCount: number;
  lastAt: string | null;
  detail: string | null;
};

export type PipelineSeries = {
  key: PipelineKey;
  label: string;
  days: Record<string, PipelineDayCell>;
};

export function berlinDateISO(value: string | Date | null | undefined): string | null {
  if (!value) return null;
  const date = typeof value === "string" ? new Date(value) : value;
  if (Number.isNaN(date.getTime())) {
    const raw = String(value).slice(0, 10);
    return /^\d{4}-\d{2}-\d{2}$/.test(raw) ? raw : null;
  }
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: "Europe/Berlin",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(date);
}

export function lastNBerlinDays(todayISO: string, n: number): string[] {
  const days: string[] = [];
  const base = new Date(`${todayISO.slice(0, 10)}T12:00:00Z`);
  for (let i = n - 1; i >= 0; i -= 1) {
    const d = new Date(base);
    d.setUTCDate(base.getUTCDate() - i);
    days.push(d.toISOString().slice(0, 10));
  }
  return days;
}

/** Classify ETL row into a monitoring pipeline. */
export function classifyEtlPipeline(run: EtlRunRow): PipelineKey | "other" {
  const log = (run.run_log || "").toLowerCase();
  if (log.includes("[order_items]") || log.includes("order-items") || log.includes("order_items")) {
    return "order_items";
  }
  if (log.includes("[inventory]") || log.includes("inventory snapshot")) {
    return "inventory";
  }
  if (/\bwhich\s*=\s*fees\b/.test(log)) return "other";
  if (log.includes("[orders]") || /\bwhich\s*=\s*(orders|beides)\b/.test(log) || log.includes("ok via github")) {
    return "orders";
  }
  // Legacy rows without tag → treat as orders ETL (historically the only writer).
  return "orders";
}

function rankStatus(status: DayRunStatus): number {
  if (status === "error") return 2;
  if (status === "success") return 1;
  return 0;
}

function normalizeRunStatus(status: string | null): DayRunStatus {
  const s = (status || "").toLowerCase();
  if (s === "success" || s === "ok") return "success";
  if (s === "error" || s === "failed" || s === "failure") return "error";
  return "error";
}

export function buildPipelineSeries({
  dayKeys,
  runs,
  inventorySnapshotDates,
  orderDataDates,
  orderItemDataDates,
}: {
  dayKeys: string[];
  runs: EtlRunRow[];
  /** Calendar dates that have an inventory snapshot (data presence). */
  inventorySnapshotDates?: string[];
  /** Calendar dates that have amazon_orders rows (data presence). */
  orderDataDates?: string[];
  /** Calendar dates that have amazon_order_items rows (data presence). */
  orderItemDataDates?: string[];
}): PipelineSeries[] {
  const emptyDays = (): Record<string, PipelineDayCell> => {
    const map: Record<string, PipelineDayCell> = {};
    for (const day of dayKeys) {
      map[day] = { status: "missing", runCount: 0, lastAt: null, detail: null };
    }
    return map;
  };

  const orders = emptyDays();
  const orderItems = emptyDays();
  const inventory = emptyDays();

  for (const run of runs) {
    const pipeline = classifyEtlPipeline(run);
    if (pipeline === "other") continue;
    const day = berlinDateISO(run.finished_at || run.started_at);
    if (!day || !(day in orders)) continue;
    const target = pipeline === "orders" ? orders : pipeline === "order_items" ? orderItems : inventory;
    const nextStatus = normalizeRunStatus(run.status);
    const cell = target[day];
    cell.runCount += 1;
    const at = run.finished_at || run.started_at;
    if (!cell.lastAt || (at && at > cell.lastAt)) {
      cell.lastAt = at;
      cell.detail = run.run_log;
    }
    if (rankStatus(nextStatus) >= rankStatus(cell.status)) {
      cell.status = nextStatus;
    }
  }

  // Inventory: if no ETL rows, fall back to snapshot presence (grün = Daten da).
  for (const snap of inventorySnapshotDates || []) {
    const day = snap.slice(0, 10);
    if (!(day in inventory)) continue;
    if (inventory[day].runCount === 0) {
      inventory[day] = {
        status: "success",
        runCount: 1,
        lastAt: `${day}T12:00:00.000Z`,
        detail: "Inventory-Snapshot vorhanden",
      };
    }
  }

  // Orders / order-items: same idea — show green when business data landed that day,
  // even if etl_runs logging failed (e.g. status check constraint).
  for (const day of orderDataDates || []) {
    const key = day.slice(0, 10);
    if (!(key in orders)) continue;
    if (orders[key].runCount === 0) {
      orders[key] = {
        status: "success",
        runCount: 1,
        lastAt: `${key}T12:00:00.000Z`,
        detail: "Bestelldaten für diesen Tag vorhanden",
      };
    }
  }
  for (const day of orderItemDataDates || []) {
    const key = day.slice(0, 10);
    if (!(key in orderItems)) continue;
    if (orderItems[key].runCount === 0) {
      orderItems[key] = {
        status: "success",
        runCount: 1,
        lastAt: `${key}T12:00:00.000Z`,
        detail: "Order-Items für diesen Tag vorhanden",
      };
    }
  }

  return [
    { key: "orders", label: "Orders ETL", days: orders },
    { key: "order_items", label: "Order Items Report", days: orderItems },
    { key: "inventory", label: "Inventory Snapshot", days: inventory },
  ];
}

"use client";

import { useEffect, useState } from "react";

type SyncSource = {
  key: string;
  label: string;
  lastDataAt: string | null;
  lastRunAt: string | null;
  status: string | null;
  ageDays: number | null;
  stale: boolean;
  detail: string | null;
};

type SyncResponse = {
  ok: boolean;
  error?: string;
  overall: "ok" | "warning" | "critical";
  warnings: string[];
  sources: SyncSource[];
  generatedAt?: string;
};

const tone = {
  ok: "border-slate-200 bg-white text-slate-600",
  warning: "border-amber-200 bg-amber-50/80 text-amber-950",
  critical: "border-red-200 bg-red-50 text-red-950",
} as const;

function formatWhen(value: string | null): string {
  if (!value) return "unbekannt";
  const date = new Date(/T/.test(value) ? value : `${value}T12:00:00Z`);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("de-DE", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: /T/.test(value) ? "2-digit" : undefined,
    minute: /T/.test(value) ? "2-digit" : undefined,
    timeZone: "Europe/Berlin",
  }).format(date);
}

export default function SyncStatusBanner() {
  const [data, setData] = useState<SyncResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const response = await fetch("/api/sync/status", { cache: "no-store" });
        const json = (await response.json()) as SyncResponse;
        if (!response.ok || !json.ok) throw new Error(json.error || "Sync-Status nicht ladbar");
        if (!cancelled) {
          setData(json);
          setError(null);
        }
      } catch (err: unknown) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : String(err));
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  if (error) {
    return (
      <div className="inline-flex items-center gap-1.5 justify-self-end text-xs italic text-amber-800">
        <span>Daten aktualisiert am unbekannt</span>
        <span
          className="group relative inline-flex h-4 w-4 shrink-0 cursor-help items-center justify-center rounded-full border border-amber-400 text-[10px] font-semibold not-italic"
          title={error}
        >
          i
          <span className="pointer-events-none absolute right-0 top-full z-30 mt-1 hidden w-64 rounded-lg border border-amber-200 bg-white p-2 text-left text-[11px] not-italic font-normal leading-snug text-slate-700 shadow-lg group-hover:block">
            Datenstand konnte nicht geladen werden: {error}
          </span>
        </span>
      </div>
    );
  }

  if (!data) {
    return (
      <div className="inline-flex items-center gap-1.5 justify-self-end text-xs italic text-slate-400">
        Orders …
      </div>
    );
  }

  const itemsSource = data.sources.find((source) => source.key === "order_items");
  const orderSource = data.sources.find((source) => source.key === "orders");
  const ordersLine = orderSource?.lastDataAt
    ? `Daten aktualisiert am ${formatWhen(orderSource.lastDataAt)}`
    : "Daten aktualisiert am unbekannt";

  return (
    <div
      className={`inline-flex max-w-full items-center gap-1.5 justify-self-end rounded-md border px-2 py-1 text-xs ${tone[data.overall]}`}
    >
      <span className="truncate italic">{ordersLine}</span>
      <span
        className="group relative inline-flex h-4 w-4 shrink-0 cursor-help items-center justify-center rounded-full border border-current/30 text-[10px] font-semibold not-italic leading-none opacity-80 hover:opacity-100"
        tabIndex={0}
        aria-label="Mehr zum Datenstand"
      >
        i
        <span
          role="tooltip"
          className="pointer-events-none absolute right-0 top-full z-30 mt-1.5 hidden w-72 rounded-lg border border-slate-200 bg-white p-2.5 text-left text-[11px] not-italic font-normal leading-snug text-slate-700 shadow-lg group-hover:block group-focus-within:block"
        >
          <div className="mb-1.5 font-semibold text-slate-900">
            {data.overall === "ok"
              ? "Daten aktuell"
              : data.overall === "warning"
                ? "Hinweis zum Datenstand"
                : "Datenstand prüfen"}
          </div>
          {itemsSource?.lastDataAt && (
            <div className="mb-1 text-slate-600">
              SKU-Positionen bis {formatWhen(itemsSource.lastDataAt)}
            </div>
          )}
          <div className="space-y-0.5 text-slate-600">
            {data.sources.map((source) => (
              <div key={source.key}>
                {source.stale ? "⚠ " : "✓ "}
                {source.label}: {source.detail || "–"}
              </div>
            ))}
          </div>
          {data.warnings.length > 0 && (
            <ul className="mt-1.5 list-disc space-y-0.5 border-t border-slate-100 pt-1.5 pl-4 text-slate-600">
              {data.warnings.map((warning) => (
                <li key={warning}>{warning}</li>
              ))}
            </ul>
          )}
        </span>
      </span>
    </div>
  );
}

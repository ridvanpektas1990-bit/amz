"use client";

import { useEffect, useMemo, useState } from "react";

type StockStatus = "out" | "critical" | "warning" | "healthy" | "no_sales";

type InventoryItem = {
  asin: string;
  sku: string;
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
  daysOfCover: number | null;
  estimatedOosDate: string | null;
  status: StockStatus;
};

type OverviewResponse = {
  ok: boolean;
  error?: string;
  snapshotDate: string | null;
  generatedAt: string;
  items: InventoryItem[];
};

type Filter = "all" | "risk" | "out" | "inbound" | "no_sales";
type Sort = "risk" | "cover" | "available" | "sales" | "asin";

const statusMeta: Record<StockStatus, { label: string; badge: string; dot: string }> = {
  out: { label: "Ausverkauft", badge: "bg-red-50 text-red-700 ring-red-200", dot: "bg-red-500" },
  critical: { label: "Kritisch", badge: "bg-orange-50 text-orange-700 ring-orange-200", dot: "bg-orange-500" },
  warning: { label: "Beobachten", badge: "bg-amber-50 text-amber-700 ring-amber-200", dot: "bg-amber-400" },
  healthy: { label: "Stabil", badge: "bg-emerald-50 text-emerald-700 ring-emerald-200", dot: "bg-emerald-500" },
  no_sales: { label: "Kein Tempo", badge: "bg-slate-50 text-slate-600 ring-slate-200", dot: "bg-slate-400" },
};

const forecastLabels = {
  seasonal: "Saisonal",
  hybrid: "Saisonal + 30T-Backup",
  recent: "30T-Backup",
  none: "Keine Prognose",
} as const;

function formatDate(value: string | null): string {
  if (!value) return "–";
  const date = new Date(`${value}T12:00:00Z`);
  return new Intl.DateTimeFormat("de-DE", { day: "2-digit", month: "2-digit", year: "numeric" }).format(date);
}

function downloadCsv(items: InventoryItem[]) {
  const header = [
    "ASIN", "SKU", "Status", "Verfügbar", "Gesamt", "Reserviert", "Inbound",
    "Verkäufe 30T", "Verkäufe 90T", "Ø 30T pro Tag", "Prognose pro Tag",
    "Prognosemethode", "Wachstumsaufschlag %", "Reichweite Tage", "OOS-Datum",
  ];
  const lines = items.map((item) => [
    item.asin, item.sku, statusMeta[item.status].label, item.available, item.total, item.reserved,
    item.inbound, item.units30, item.units90, item.dailySales30, item.forecastDailySales,
    forecastLabels[item.forecastMethod], item.growthPercent,
    item.daysOfCover ?? "", item.estimatedOosDate ?? "",
  ]);
  const csv = [header, ...lines]
    .map((row) => row.map((value) => `"${String(value).replaceAll('"', '""')}"`).join(";"))
    .join("\n");
  const blob = new Blob(["\uFEFF", csv], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = `lagerbestand-${new Date().toISOString().slice(0, 10)}.csv`;
  link.click();
  URL.revokeObjectURL(url);
}

export default function InventoryOverviewTable() {
  const [data, setData] = useState<OverviewResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [reloadKey, setReloadKey] = useState(0);
  const [search, setSearch] = useState("");
  const [filter, setFilter] = useState<Filter>("all");
  const [sort, setSort] = useState<Sort>("risk");
  const [copiedAsin, setCopiedAsin] = useState<string | null>(null);

  useEffect(() => {
    let active = true;
    (async () => {
      setLoading(true);
      setError(null);
      try {
        const response = await fetch("/api/inventory/overview", { cache: "no-store" });
        const json = (await response.json()) as OverviewResponse;
        if (!response.ok || !json.ok) throw new Error(json.error || "Bestandsübersicht konnte nicht geladen werden");
        if (active) setData(json);
      } catch (caught) {
        if (active) setError(caught instanceof Error ? caught.message : String(caught));
      } finally {
        if (active) setLoading(false);
      }
    })();
    return () => { active = false; };
  }, [reloadKey]);

  const counts = useMemo(() => {
    const items = data?.items || [];
    return {
      total: items.length,
      out: items.filter((item) => item.status === "out").length,
      critical: items.filter((item) => item.status === "critical").length,
      warning: items.filter((item) => item.status === "warning").length,
      inbound: items.filter((item) => item.inbound > 0).length,
    };
  }, [data]);

  const visibleItems = useMemo(() => {
    const term = search.trim().toLowerCase();
    const items = (data?.items || []).filter((item) => {
      if (term && !item.asin.toLowerCase().includes(term) && !item.sku.toLowerCase().includes(term)) return false;
      if (filter === "risk" && !["out", "critical"].includes(item.status)) return false;
      if (filter === "out" && item.status !== "out") return false;
      if (filter === "inbound" && item.inbound <= 0) return false;
      if (filter === "no_sales" && item.status !== "no_sales") return false;
      return true;
    });

    const riskRank: Record<StockStatus, number> = { out: 0, critical: 1, warning: 2, healthy: 3, no_sales: 4 };
    return items.sort((a, b) => {
      if (sort === "cover") return (a.daysOfCover ?? Number.MAX_SAFE_INTEGER) - (b.daysOfCover ?? Number.MAX_SAFE_INTEGER);
      if (sort === "available") return b.available - a.available;
      if (sort === "sales") return b.units30 - a.units30;
      if (sort === "asin") return a.asin.localeCompare(b.asin);
      return riskRank[a.status] - riskRank[b.status] || (a.daysOfCover ?? 999999) - (b.daysOfCover ?? 999999);
    });
  }, [data, search, filter, sort]);

  async function copyAsin(asin: string) {
    await navigator.clipboard.writeText(asin);
    setCopiedAsin(asin);
    window.setTimeout(() => setCopiedAsin(null), 1200);
  }

  const nf = useMemo(() => new Intl.NumberFormat("de-DE"), []);
  const decimal = useMemo(() => new Intl.NumberFormat("de-DE", { maximumFractionDigits: 2 }), []);

  return (
    <section className="mt-10 overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm">
      <div className="border-b border-slate-200 bg-gradient-to-r from-slate-950 via-slate-900 to-slate-800 px-5 py-5 text-white md:px-6">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
          <div>
            <div className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-300">Bestandssteuerung</div>
            <h2 className="mt-1 text-xl font-semibold">ASIN-Reichweite &amp; Out-of-Stock-Prognose</h2>
            <p className="mt-1 max-w-2xl text-sm text-slate-300">
              Saisonale Reichweite auf Basis der entsprechenden Vorjahresmonate, angepasst um positives Wachstum dieses Jahres.
            </p>
          </div>
          <div className="text-sm text-slate-300">
            Snapshot: <span className="font-semibold text-white">{formatDate(data?.snapshotDate || null)}</span>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-px border-b border-slate-200 bg-slate-200 sm:grid-cols-5">
        {[
          ["ASINs", counts.total, "text-slate-900"],
          ["Ausverkauft", counts.out, "text-red-700"],
          ["≤ 30 Tage", counts.critical, "text-orange-700"],
          ["31–60 Tage", counts.warning, "text-amber-700"],
          ["Mit Zulauf", counts.inbound, "text-blue-700"],
        ].map(([label, value, color]) => (
          <div key={String(label)} className="bg-white px-4 py-3">
            <div className="text-xs text-slate-500">{label}</div>
            <div className={`mt-0.5 text-xl font-semibold ${color}`}>{value}</div>
          </div>
        ))}
      </div>

      <div className="flex flex-col gap-3 border-b border-slate-200 bg-slate-50 px-4 py-4 lg:flex-row lg:items-center lg:justify-between">
        <div className="flex flex-1 flex-col gap-2 sm:flex-row">
          <input
            value={search}
            onChange={(event) => setSearch(event.target.value)}
            placeholder="ASIN oder SKU suchen …"
            className="w-full rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm outline-none transition focus:border-slate-500 focus:ring-2 focus:ring-slate-200 sm:max-w-xs"
          />
          <select
            value={filter}
            onChange={(event) => setFilter(event.target.value as Filter)}
            className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm"
          >
            <option value="all">Alle Bestände</option>
            <option value="risk">Risiko ≤ 30 Tage</option>
            <option value="out">Nur ausverkauft</option>
            <option value="inbound">Mit eingehender Ware</option>
            <option value="no_sales">Ohne Absatztempo</option>
          </select>
          <select
            value={sort}
            onChange={(event) => setSort(event.target.value as Sort)}
            className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm"
          >
            <option value="risk">Sortierung: Dringlichkeit</option>
            <option value="cover">Reichweite aufsteigend</option>
            <option value="available">Bestand absteigend</option>
            <option value="sales">Absatz 30 Tage</option>
            <option value="asin">ASIN A–Z</option>
          </select>
        </div>
        <div className="flex gap-2">
          <button
            onClick={() => setReloadKey((value) => value + 1)}
            className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100"
          >
            Aktualisieren
          </button>
          <button
            onClick={() => downloadCsv(visibleItems)}
            disabled={!visibleItems.length}
            className="rounded-lg bg-slate-900 px-3 py-2 text-sm font-medium text-white hover:bg-slate-700 disabled:cursor-not-allowed disabled:opacity-40"
          >
            CSV exportieren
          </button>
        </div>
      </div>

      {loading && <div className="px-6 py-12 text-center text-sm text-slate-500">Bestandsdaten werden berechnet …</div>}
      {error && (
        <div className="m-5 rounded-xl border border-red-200 bg-red-50 p-4 text-sm text-red-700">
          <div className="font-semibold">Bestandsübersicht konnte nicht geladen werden</div>
          <div className="mt-1">{error}</div>
        </div>
      )}

      {!loading && !error && (
        <>
          <div className="overflow-x-auto">
            <table className="min-w-[1080px] w-full border-collapse text-left text-sm">
              <thead className="bg-white text-xs uppercase tracking-wide text-slate-500">
                <tr className="border-b border-slate-200">
                  <th className="px-5 py-3 font-semibold">Produkt</th>
                  <th className="px-4 py-3 font-semibold">Status</th>
                  <th className="px-4 py-3 text-right font-semibold">Verfügbar</th>
                  <th className="px-4 py-3 text-right font-semibold">Gesamt / reserviert</th>
                  <th className="px-4 py-3 text-right font-semibold">Inbound</th>
                  <th className="px-4 py-3 text-right font-semibold">Absatz &amp; Prognose</th>
                  <th className="px-4 py-3 font-semibold">Reichweite</th>
                  <th className="px-5 py-3 text-right font-semibold">Aktion</th>
                </tr>
              </thead>
              <tbody>
                {visibleItems.map((item) => {
                  const meta = statusMeta[item.status];
                  const barWidth = item.daysOfCover === null ? 0 : Math.min(100, (item.daysOfCover / 120) * 100);
                  return (
                    <tr key={`${item.asin}-${item.sku}`} className="border-b border-slate-100 hover:bg-slate-50/80">
                      <td className="px-5 py-4">
                        <a
                          href={`https://www.amazon.de/dp/${encodeURIComponent(item.asin)}`}
                          target="_blank"
                          rel="noreferrer"
                          className="font-semibold text-slate-900 underline decoration-slate-300 underline-offset-4 hover:decoration-slate-700"
                        >
                          {item.asin}
                        </a>
                        <div className="mt-1 max-w-[220px] truncate font-mono text-xs text-slate-500" title={item.sku}>{item.sku}</div>
                      </td>
                      <td className="px-4 py-4">
                        <span className={`inline-flex items-center gap-1.5 rounded-full px-2.5 py-1 text-xs font-semibold ring-1 ring-inset ${meta.badge}`}>
                          <span className={`h-1.5 w-1.5 rounded-full ${meta.dot}`} />
                          {meta.label}
                        </span>
                      </td>
                      <td className="px-4 py-4 text-right">
                        <div className="text-lg font-semibold tabular-nums text-slate-900">{nf.format(item.available)}</div>
                        <div className="text-xs text-slate-500">verkaufbar</div>
                      </td>
                      <td className="px-4 py-4 text-right tabular-nums">
                        <div className="font-medium text-slate-800">{nf.format(item.total)}</div>
                        <div className="text-xs text-slate-500">{nf.format(item.reserved)} reserviert</div>
                      </td>
                      <td className="px-4 py-4 text-right tabular-nums">
                        <div className={`font-medium ${item.inbound > 0 ? "text-blue-700" : "text-slate-400"}`}>{nf.format(item.inbound)}</div>
                        {item.pendingCustomerOrders > 0 && <div className="text-xs text-slate-500">{nf.format(item.pendingCustomerOrders)} Kundenorders</div>}
                      </td>
                      <td className="px-4 py-4 text-right tabular-nums">
                        <div className="font-medium text-slate-800">{nf.format(item.units30)} Stk / 30T</div>
                        <div className="text-xs text-slate-500">Prognose Ø {decimal.format(item.forecastDailySales)} / Tag</div>
                        <div
                          className="mt-1 text-[11px] font-medium text-indigo-700"
                          title={`Vergleich abgeschlossene Monate: ${nf.format(item.comparisonCurrentUnits)} vs. ${nf.format(item.comparisonPreviousUnits)} Stück; stabilisiert gegen kleine Vorjahresmengen`}
                        >
                          {forecastLabels[item.forecastMethod]}
                          {item.growthPercent > 0 ? ` · +${decimal.format(item.growthPercent)} %` : ""}
                        </div>
                      </td>
                      <td className="min-w-[210px] px-4 py-4">
                        <div className="flex items-baseline justify-between gap-3">
                          <span className="font-semibold text-slate-900">
                            {item.daysOfCover === null ? "Keine Prognose" : item.daysOfCover === 0 ? "Heute OOS" : `${nf.format(item.daysOfCover)} Tage`}
                          </span>
                          <span className="text-xs text-slate-500">{formatDate(item.estimatedOosDate)}</span>
                        </div>
                        <div className="mt-2 h-1.5 overflow-hidden rounded-full bg-slate-100">
                          <div className={`h-full rounded-full ${meta.dot}`} style={{ width: `${barWidth}%` }} />
                        </div>
                      </td>
                      <td className="px-5 py-4 text-right">
                        <button
                          onClick={() => copyAsin(item.asin)}
                          className="rounded-md border border-slate-200 px-2.5 py-1.5 text-xs font-medium text-slate-600 hover:bg-white hover:text-slate-900"
                        >
                          {copiedAsin === item.asin ? "Kopiert" : "ASIN kopieren"}
                        </button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>

          {!visibleItems.length && (
            <div className="px-6 py-12 text-center text-sm text-slate-500">Keine ASIN passt zu den ausgewählten Filtern.</div>
          )}

          <div className="flex flex-col gap-2 bg-slate-50 px-5 py-4 text-xs text-slate-500 sm:flex-row sm:items-center sm:justify-between">
            <span>{visibleItems.length} von {counts.total} ASINs angezeigt</span>
            <span>Prognose: gleicher Vorjahresmonat × stabilisiertes positives Wachstum der abgeschlossenen Monate. Ohne Vorjahresdaten gilt der 30T-Absatz. Inbound ist nicht eingerechnet.</span>
          </div>
        </>
      )}
    </section>
  );
}

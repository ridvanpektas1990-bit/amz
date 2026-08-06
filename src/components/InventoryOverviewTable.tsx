"use client";

import { useMemo, useState } from "react";
import {
  inventoryStatusLabel,
  type InventoryOverviewItem,
  type InventoryOverviewResponse,
  type StockStatus,
} from "@/lib/inventory-overview";

type Filter = "all" | "risk" | "out" | "inbound" | "no_sales";
type Sort = "risk" | "cover" | "available" | "sales" | "asin";

const statusMeta: Record<StockStatus, { label: string; badge: string; dot: string }> = {
  out: { label: "Leer", badge: "bg-red-50 text-red-700 ring-red-200", dot: "bg-red-500" },
  critical: { label: "≤30T", badge: "bg-orange-50 text-orange-700 ring-orange-200", dot: "bg-orange-500" },
  warning: { label: "≤60T", badge: "bg-amber-50 text-amber-700 ring-amber-200", dot: "bg-amber-400" },
  healthy: { label: "OK", badge: "bg-emerald-50 text-emerald-700 ring-emerald-200", dot: "bg-emerald-500" },
  no_sales: { label: "–", badge: "bg-slate-50 text-slate-600 ring-slate-200", dot: "bg-slate-400" },
};

function formatDate(value: string | null): string {
  if (!value) return "–";
  const date = new Date(`${value}T12:00:00Z`);
  return new Intl.DateTimeFormat("de-DE", { day: "2-digit", month: "2-digit" }).format(date);
}

function downloadCsv(items: InventoryOverviewItem[]) {
  const header = [
    "Produkt", "ASIN", "SKU", "Status", "Verfügbar", "Inbound",
    "Reichweite inkl. Inbound", "Reichweite ohne Inbound", "OOS-Datum",
  ];
  const lines = items.map((item) => [
    item.productName || "", item.asin, item.sku, statusMeta[item.status].label,
    item.available, item.inbound,
    item.daysOfCover ?? "", item.daysOfCoverOnHand ?? "", item.estimatedOosDate ?? "",
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

export default function InventoryOverviewTable({
  data,
  loading,
  error,
  onReload,
  selectedSku,
  onSelectSku,
}: {
  data: InventoryOverviewResponse | null;
  loading: boolean;
  error: string | null;
  onReload: () => void;
  selectedSku?: string;
  onSelectSku?: (sku: string) => void;
}) {
  const [search, setSearch] = useState("");
  const [filter, setFilter] = useState<Filter>("all");
  const [sort, setSort] = useState<Sort>("risk");

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
      if (
        term &&
        !item.asin.toLowerCase().includes(term) &&
        !item.sku.toLowerCase().includes(term) &&
        !(item.productName || "").toLowerCase().includes(term)
      ) {
        return false;
      }
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

  const nf = useMemo(() => new Intl.NumberFormat("de-DE"), []);

  const filterChip = (id: Filter, label: string, count?: number) => {
    const active = filter === id;
    return (
      <button
        key={id}
        type="button"
        onClick={() => setFilter(id)}
        className={`rounded-md px-2 py-1 text-xs font-medium tabular-nums transition ${
          active
            ? "bg-slate-900 text-white"
            : "bg-slate-100 text-slate-600 hover:bg-slate-200"
        }`}
      >
        {label}
        {typeof count === "number" ? ` ${count}` : ""}
      </button>
    );
  };

  return (
    <section id="bestandssteuerung" className="overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm">
      <div className="flex flex-col gap-2 border-b border-slate-200 px-3 py-2.5 sm:flex-row sm:items-center sm:justify-between">
        <div className="flex min-w-0 flex-wrap items-center gap-2">
          <h2 className="text-sm font-semibold text-slate-950">Bestandssteuerung</h2>
          <div className="flex flex-wrap gap-1">
            {filterChip("all", "Alle", counts.total)}
            {filterChip("risk", "Risiko", counts.out + counts.critical)}
            {filterChip("out", "Leer", counts.out)}
            {filterChip("inbound", "Inbound", counts.inbound)}
          </div>
        </div>
        <div className="flex flex-wrap items-center gap-1.5">
          <input
            value={search}
            onChange={(event) => setSearch(event.target.value)}
            placeholder="ASIN / SKU…"
            className="w-36 rounded-md border border-slate-300 px-2 py-1 text-xs outline-none focus:border-slate-500 sm:w-40"
          />
          <select
            value={sort}
            onChange={(event) => setSort(event.target.value as Sort)}
            className="rounded-md border border-slate-300 bg-white px-2 py-1 text-xs"
          >
            <option value="risk">Dringlichkeit</option>
            <option value="cover">Reichweite</option>
            <option value="sales">Absatz</option>
            <option value="available">Bestand</option>
          </select>
          <button
            type="button"
            onClick={onReload}
            className="rounded-md border border-slate-300 px-2 py-1 text-xs text-slate-700 hover:bg-slate-50"
          >
            Neu
          </button>
          <button
            type="button"
            onClick={() => downloadCsv(visibleItems)}
            disabled={!visibleItems.length}
            className="rounded-md bg-slate-900 px-2 py-1 text-xs font-medium text-white hover:bg-slate-700 disabled:opacity-40"
          >
            CSV
          </button>
        </div>
      </div>

      {loading && <div className="px-3 py-6 text-center text-sm text-slate-500">Lädt …</div>}
      {error && (
        <div className="m-2 rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">{error}</div>
      )}

      {!loading && !error && (
        <>
          <div className="h-[28rem] overflow-auto">
            <table className="min-w-[760px] w-full border-collapse text-left text-[13px]">
              <thead className="sticky top-0 z-10 bg-slate-50 text-[10px] uppercase tracking-wide text-slate-500">
                <tr className="border-b border-slate-200">
                  <th className="px-2.5 py-1.5 font-semibold">Produkt</th>
                  <th className="px-1.5 py-1.5 font-semibold">Status</th>
                  <th className="px-1.5 py-1.5 text-right font-semibold">Verf.</th>
                  <th className="px-1.5 py-1.5 pr-6 text-right font-semibold sm:pr-10">In</th>
                  <th className="py-1.5 pl-4 pr-2.5 font-semibold sm:pl-8">Reichweite</th>
                </tr>
              </thead>
              <tbody>
                {visibleItems.map((item) => {
                  const meta = statusMeta[item.status];
                  const coverForBar = item.daysOfCover;
                  const barWidth = coverForBar === null ? 0 : Math.min(100, (coverForBar / 120) * 100);
                  const active = selectedSku === item.sku;
                  const inboundHelps =
                    item.inbound > 0 &&
                    item.daysOfCoverOnHand !== null &&
                    item.daysOfCover !== null &&
                    item.daysOfCoverOnHand < item.daysOfCover;
                  const statusText = inventoryStatusLabel(item, meta.label);
                  const soonOos = item.status === "out" || item.status === "critical";
                  return (
                    <tr
                      key={`${item.asin}-${item.sku}`}
                      className={`border-b border-slate-100 ${
                        active
                          ? "bg-sky-50 shadow-[inset_0_0_0_2px_#38bdf8]"
                          : soonOos
                            ? "bg-orange-50/90"
                            : ""
                      }`}
                    >
                      <td className="px-2.5 py-1.5">
                        <button
                          type="button"
                          onClick={() => onSelectSku?.(item.sku)}
                          title="Produkt im Dashboard öffnen"
                          className={`flex max-w-[320px] cursor-pointer items-center gap-2 rounded-lg px-1.5 py-1 text-left transition ${
                            active
                              ? "bg-sky-100/80 ring-1 ring-sky-300"
                              : "bg-slate-50/80 ring-1 ring-slate-200/80 hover:bg-sky-50 hover:ring-sky-200"
                          }`}
                        >
                          <div className="flex h-8 w-8 shrink-0 items-center justify-center overflow-hidden rounded border border-slate-200 bg-white">
                            {item.imageUrl ? (
                              <img
                                src={item.imageUrl}
                                alt=""
                                className="h-full w-full object-contain p-0.5"
                                loading="lazy"
                                referrerPolicy="no-referrer"
                              />
                            ) : (
                              <span className={`h-2 w-2 rounded-full ${meta.dot}`} />
                            )}
                          </div>
                          <span className="min-w-0">
                            <span className="block truncate text-[13px] font-medium leading-tight text-slate-900">
                              {item.productName || item.asin}
                            </span>
                            <span className="block truncate text-[10px] text-slate-500">
                              {item.asin} · {item.sku}
                            </span>
                          </span>
                        </button>
                      </td>
                      <td className="px-1.5 py-1.5">
                        <span
                          className={`inline-flex rounded px-1.5 py-0.5 text-[10px] font-semibold ring-1 ring-inset ${
                            statusText === "Zulauf"
                              ? "bg-sky-50 text-sky-700 ring-sky-200"
                              : meta.badge
                          }`}
                        >
                          {statusText}
                        </span>
                      </td>
                      <td className="px-1.5 py-1.5 text-right font-semibold tabular-nums text-slate-900">
                        {nf.format(item.available)}
                      </td>
                      <td
                        className={`px-1.5 py-1.5 pr-6 text-right tabular-nums sm:pr-10 ${
                          item.inbound > 0 ? "font-semibold text-sky-700" : "text-slate-400"
                        }`}
                      >
                        {item.inbound > 0 ? `+${nf.format(item.inbound)}` : "0"}
                      </td>
                      <td className="min-w-[168px] py-1.5 pl-4 pr-2.5 sm:pl-8">
                        <div className="flex items-baseline justify-between gap-2">
                          <span className="text-[13px] font-semibold tabular-nums text-slate-900">
                            {item.daysOfCover === null
                              ? "–"
                              : item.daysOfCover === 0
                                ? "leer"
                                : `${nf.format(item.daysOfCover)} T`}
                          </span>
                          <span className="text-[10px] text-slate-500">{formatDate(item.estimatedOosDate)}</span>
                        </div>
                        {inboundHelps ? (
                          <div className="mt-0.5 text-[10px] leading-tight text-sky-700">
                            inkl. Zulauf · ohne {item.daysOfCoverOnHand === 0 ? "leer" : `${nf.format(item.daysOfCoverOnHand!)} T`}
                          </div>
                        ) : item.inbound > 0 ? (
                          <div className="mt-0.5 text-[10px] leading-tight text-slate-500">inkl. Zulauf</div>
                        ) : null}
                        <div className="mt-0.5 h-1 overflow-hidden rounded-full bg-slate-100">
                          <div className={`h-full rounded-full ${inboundHelps ? "bg-sky-500" : meta.dot}`} style={{ width: `${barWidth}%` }} />
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
            {!visibleItems.length && (
              <div className="px-3 py-6 text-center text-sm text-slate-500">Keine Treffer.</div>
            )}
          </div>
          <div className="border-t border-slate-100 px-3 py-1.5 text-[10px] text-slate-500">
            {visibleItems.length}/{counts.total} · Reichweite = Verfügbar + Inbound
          </div>
        </>
      )}
    </section>
  );
}

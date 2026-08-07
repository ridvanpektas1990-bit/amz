"use client";

import { useMemo, useState } from "react";
import {
  amazonShipActionLabel,
  coverageHealthBadgeClass,
  coverageHealthFromOverviewItem,
  supplierOrderActionLabel,
} from "@/lib/coverage-health";
import type { InventoryOverviewItem, InventoryOverviewResponse } from "@/lib/inventory-overview";
import type { StockStatus } from "@/lib/inventory-overview";

type Filter =
  | "all"
  | "risk"
  | "out"
  | "inbound"
  | "risk_amazon"
  | "risk_gesamt"
  | "no_sales";
type Sort = "risk" | "cover_amazon" | "cover_gesamt" | "available" | "sales" | "asin";

const legacyStatusDot: Record<StockStatus, string> = {
  out: "bg-slate-500",
  critical: "bg-orange-500",
  warning: "bg-amber-400",
  healthy: "bg-emerald-500",
  no_sales: "bg-slate-400",
};

function formatDate(value: string | null): string {
  if (!value) return "–";
  const date = new Date(`${value}T12:00:00Z`);
  return new Intl.DateTimeFormat("de-DE", { day: "2-digit", month: "2-digit" }).format(date);
}

function amazonCover(item: InventoryOverviewItem): number | null {
  return item.daysOfCover;
}

function gesamtCover(item: InventoryOverviewItem): number | null {
  return item.daysOfCoverAmazonAndLocal ?? item.daysOfCoverWithLocal ?? item.daysOfCover;
}

function amazonOosDate(item: InventoryOverviewItem): string | null {
  return item.estimatedOosDate;
}

function gesamtOosDate(item: InventoryOverviewItem): string | null {
  return (
    item.estimatedOosDateAmazonAndLocal ??
    item.estimatedOosDateWithLocal ??
    item.estimatedOosDate
  );
}

function isCoverRisk(cover: number | null): boolean {
  return cover !== null && cover <= 30;
}

function formatCoverDays(cover: number | null, nf: Intl.NumberFormat): string {
  if (cover === null) return "–";
  if (cover === 0) return "leer";
  return `${nf.format(cover)} T`;
}

function coverBarClass(cover: number | null): string {
  if (cover === null) return "bg-slate-300";
  if (cover <= 0) return "bg-red-500";
  if (cover <= 30) return "bg-orange-500";
  if (cover <= 60) return "bg-amber-400";
  return "bg-emerald-500";
}

function downloadCsv(items: InventoryOverviewItem[]) {
  const nf = new Intl.NumberFormat("de-DE");
  const header = [
    "Produkt",
    "ASIN",
    "SKU",
    "Status",
    "Verfügbar",
    "Inbound",
    "Lokal",
    "Bestellt",
    "Reichweite Amazon",
    "Amz Lager senden",
    "OOS Amazon",
    "Reichweite Gesamtlager",
    "Produktbestellung",
    "OOS Gesamtlager",
  ];
  const lines = items.map((item) => [
    item.productName || "",
    item.asin,
    item.sku,
    coverageHealthFromOverviewItem(item).shortLabel,
    item.available,
    item.inbound,
    item.localQty ?? 0,
    item.onOrderUnits ?? 0,
    amazonCover(item) ?? "",
    amazonShipActionLabel(item),
    amazonOosDate(item) ?? "",
    gesamtCover(item) ?? "",
    supplierOrderActionLabel(item, nf),
    gesamtOosDate(item) ?? "",
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

function CoverCell({
  cover,
  oosDate,
  nf,
}: {
  cover: number | null;
  oosDate: string | null;
  nf: Intl.NumberFormat;
}) {
  const barWidth = cover === null ? 0 : Math.min(100, (cover / 120) * 100);
  return (
    <div className="mx-auto min-w-[88px] max-w-[110px]">
      <div className="flex flex-col items-center gap-0.5">
        <span className="text-[13px] font-semibold tabular-nums text-slate-900">
          {formatCoverDays(cover, nf)}
        </span>
        <span className="text-[10px] text-slate-500">{formatDate(oosDate)}</span>
      </div>
      <div className="mt-0.5 h-1 overflow-hidden rounded-full bg-slate-100">
        <div
          className={`h-full rounded-full ${coverBarClass(cover)}`}
          style={{ width: `${barWidth}%` }}
        />
      </div>
    </div>
  );
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
      riskAmazon: items.filter((item) => isCoverRisk(amazonCover(item)) || item.status === "out")
        .length,
      riskGesamt: items.filter((item) => isCoverRisk(gesamtCover(item)) || item.status === "out")
        .length,
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
      if (filter === "risk_amazon" && !(isCoverRisk(amazonCover(item)) || item.status === "out")) {
        return false;
      }
      if (filter === "risk_gesamt" && !(isCoverRisk(gesamtCover(item)) || item.status === "out")) {
        return false;
      }
      return true;
    });

    const riskRank: Record<StockStatus, number> = {
      out: 0,
      critical: 1,
      warning: 2,
      healthy: 3,
      no_sales: 4,
    };
    return items.sort((a, b) => {
      if (sort === "cover_amazon") {
        return (amazonCover(a) ?? Number.MAX_SAFE_INTEGER) - (amazonCover(b) ?? Number.MAX_SAFE_INTEGER);
      }
      if (sort === "cover_gesamt") {
        return (gesamtCover(a) ?? Number.MAX_SAFE_INTEGER) - (gesamtCover(b) ?? Number.MAX_SAFE_INTEGER);
      }
      if (sort === "available") return b.available - a.available;
      if (sort === "sales") return b.units30 - a.units30;
      if (sort === "asin") return a.asin.localeCompare(b.asin);
      return (
        riskRank[a.status] - riskRank[b.status] ||
        (amazonCover(a) ?? 999999) - (amazonCover(b) ?? 999999)
      );
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
            {filterChip("risk_amazon", "Amazon ≤30T", counts.riskAmazon)}
            {filterChip("risk_gesamt", "Gesamt ≤30T", counts.riskGesamt)}
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
            <option value="cover_amazon">Reichweite Amazon</option>
            <option value="cover_gesamt">Reichweite Insgesamt</option>
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
            <table className="min-w-[1100px] w-full border-collapse text-center text-[13px]">
              <thead className="sticky top-0 z-10">
                <tr className="border-b border-slate-200/80 bg-white text-[10px] font-semibold tracking-wide text-slate-500">
                  <th colSpan={2} className="px-2 py-1.5 text-left font-medium text-slate-400">
                    Produkt
                  </th>
                  <th
                    colSpan={3}
                    className="border-l border-slate-200 bg-white px-2 py-1.5 text-[10px] font-semibold uppercase tracking-[0.08em] text-slate-600"
                  >
                    Stückzahlen
                  </th>
                  <th
                    colSpan={2}
                    className="border-l border-slate-200 bg-slate-50/90 px-2 py-1.5 text-[10px] font-semibold uppercase tracking-[0.08em] text-slate-600"
                  >
                    Amazon-Lager
                  </th>
                  <th
                    colSpan={2}
                    className="border-l border-slate-200 bg-slate-100/80 px-2 py-1.5 pr-2 text-[10px] font-semibold uppercase tracking-[0.08em] text-slate-600"
                  >
                    Gesamtlager
                  </th>
                </tr>
                <tr className="border-b border-slate-200 bg-slate-50/90 text-[10px] uppercase tracking-wide text-slate-500">
                  <th className="px-2 py-1.5 font-semibold">Produkt</th>
                  <th className="px-1 py-1.5 font-semibold">Status</th>
                  <th className="border-l border-slate-200 bg-white px-1 py-1.5 font-semibold text-slate-600">
                    Verf.
                  </th>
                  <th className="bg-white px-1 py-1.5 font-semibold text-slate-600">In</th>
                  <th className="bg-white px-1 py-1.5 font-semibold text-slate-600">Lokal</th>
                  <th className="border-l border-slate-200 bg-slate-50 px-1.5 py-1.5 font-semibold text-slate-600">
                    Reichweite
                  </th>
                  <th className="bg-slate-50 px-1.5 py-1.5 font-semibold text-slate-600">Senden</th>
                  <th className="border-l border-slate-200 bg-slate-100/70 px-1.5 py-1.5 font-semibold text-slate-600">
                    Reichweite
                  </th>
                  <th className="bg-slate-100/70 px-1.5 py-1.5 pr-2 font-semibold text-slate-600">
                    Produktbestellung
                  </th>
                </tr>
              </thead>
              <tbody>
                {visibleItems.map((item) => {
                  const health = coverageHealthFromOverviewItem(item);
                  const active = selectedSku === item.sku;
                  const amz = amazonCover(item);
                  const gesamt = gesamtCover(item);
                  const shipLabel = amazonShipActionLabel(item);
                  const orderLabel = supplierOrderActionLabel(item, nf);
                  const needsAttention =
                    health.health === "sold_out" ||
                    health.health === "stockout_risk" ||
                    health.health === "reorder_product";
                  return (
                    <tr
                      key={`${item.asin}-${item.sku}`}
                      className={`border-b border-slate-100 ${
                        active
                          ? "bg-sky-50 shadow-[inset_0_0_0_2px_#38bdf8]"
                          : needsAttention
                            ? "bg-orange-50/90"
                            : ""
                      }`}
                    >
                      <td className="px-2 py-1.5">
                        <button
                          type="button"
                          onClick={() => onSelectSku?.(item.sku)}
                          title="Produkt im Dashboard öffnen"
                          className={`mx-auto flex max-w-[200px] cursor-pointer items-center justify-center gap-2 rounded-lg px-1 py-1 text-center transition ${
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
                              <span className={`h-2 w-2 rounded-full ${legacyStatusDot[item.status]}`} />
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
                      <td className="px-1 py-1.5">
                        <span
                          title={health.label}
                          className={`inline-flex max-w-[9rem] rounded px-1.5 py-0.5 text-[10px] font-semibold leading-snug ring-1 ring-inset ${coverageHealthBadgeClass[health.tone]}`}
                        >
                          {health.shortLabel}
                        </span>
                      </td>
                      <td className="border-l border-slate-200 bg-slate-50/20 px-1 py-1.5 font-semibold tabular-nums text-slate-900">
                        {nf.format(item.available)}
                      </td>
                      <td
                        className={`bg-slate-50/20 px-1 py-1.5 tabular-nums ${
                          item.inbound > 0 ? "font-semibold text-sky-700" : "text-slate-400"
                        }`}
                      >
                        {item.inbound > 0 ? `+${nf.format(item.inbound)}` : "0"}
                      </td>
                      <td
                        className={`bg-slate-50/20 px-1 py-1.5 tabular-nums ${
                          (item.localQty || 0) > 0 ? "font-semibold text-violet-700" : "text-slate-400"
                        }`}
                      >
                        {(item.localQty || 0) > 0 ? nf.format(item.localQty || 0) : "–"}
                        {(item.onOrderUnits || 0) > 0 ? (
                          <div className="text-[10px] font-normal text-violet-500">
                            +{nf.format(item.onOrderUnits || 0)} best.
                          </div>
                        ) : null}
                      </td>
                      <td className="border-l border-slate-200 bg-slate-50/40 px-1.5 py-1.5">
                        <CoverCell cover={amz} oosDate={amazonOosDate(item)} nf={nf} />
                      </td>
                      <td className="bg-slate-50/40 px-1.5 py-1.5">
                        <div
                          title="Nächste Aktion: Amz Lager senden"
                          className={`text-[12px] font-medium tabular-nums leading-tight ${
                            shipLabel === "jetzt"
                              ? "text-red-700"
                              : shipLabel === "–"
                                ? "text-slate-400"
                                : "text-slate-800"
                          }`}
                        >
                          {shipLabel}
                        </div>
                      </td>
                      <td className="border-l border-slate-200 bg-slate-100/35 px-1.5 py-1.5">
                        <CoverCell cover={gesamt} oosDate={gesamtOosDate(item)} nf={nf} />
                      </td>
                      <td className="bg-slate-100/35 px-1.5 py-1.5 pr-2">
                        <div
                          title="Nächste Aktion: Produktbestellung"
                          className={`mx-auto max-w-[8.5rem] text-[12px] font-medium leading-tight ${
                            orderLabel === "jetzt"
                              ? "text-red-700"
                              : orderLabel.startsWith("bereits")
                                ? "text-emerald-700"
                                : orderLabel === "–" || orderLabel === "Leadzeit fehlt"
                                  ? "text-slate-400"
                                  : "text-slate-800"
                          }`}
                        >
                          {orderLabel}
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
            {visibleItems.length}/{counts.total} · Stückzahlen = FBA Verf./Inbound + Eigenlager ·
            Amazon-Lager = Reichweite FBA · Gesamtlager = Amazon + Eigenlager · Produktbestellung =
            nächste Lieferanten-PO
          </div>
        </>
      )}
    </section>
  );
}

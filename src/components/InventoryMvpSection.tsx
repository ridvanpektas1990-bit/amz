"use client";

import { useMemo } from "react";
import InventoryOverviewTable from "@/components/InventoryOverviewTable";
import {
  filterItemsBySelectedSku,
  inventoryActionHint,
  selectOosRiskItems,
  summarizeInventoryKpis,
  type InventoryOverviewResponse,
  type StockStatus,
} from "@/lib/inventory-overview";

const statusTone: Record<StockStatus, string> = {
  out: "bg-red-500",
  critical: "bg-orange-500",
  warning: "bg-amber-400",
  healthy: "bg-emerald-500",
  no_sales: "bg-slate-400",
};

function formatGrowth(value: number | null): string {
  if (value == null) return "–";
  const sign = value > 0 ? "+" : "";
  return `${sign}${new Intl.NumberFormat("de-DE", { maximumFractionDigits: 0 }).format(value)} %`;
}

export function InventorySummarySection({
  data,
  loading,
  error,
  selectedSku,
  onSelectSku,
}: {
  data: InventoryOverviewResponse | null;
  loading: boolean;
  error: string | null;
  selectedSku?: string;
  onSelectSku?: (sku: string) => void;
}) {
  const nf = useMemo(() => new Intl.NumberFormat("de-DE"), []);
  const kpiItems = useMemo(
    () => filterItemsBySelectedSku(data?.items ?? [], selectedSku),
    [data?.items, selectedSku],
  );
  const kpis = useMemo(() => summarizeInventoryKpis(kpiItems), [kpiItems]);
  const riskItems = useMemo(
    () => selectOosRiskItems(data?.items ?? [], 12),
    [data?.items],
  );
  const scopedAsin = selectedSku
    ? (data?.items ?? []).find((item) => item.sku === selectedSku)?.asin || null
    : null;

  // YTD-Vergleich in der Overview-API: Jan bis letzter abgeschlossener Monat vs. Vorjahr.
  const { previousYear, ytdMonthLabel } = useMemo(() => {
    const now = new Date(
      new Intl.DateTimeFormat("en-CA", {
        timeZone: "Europe/Berlin",
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
      }).format(new Date()) + "T12:00:00Z",
    );
    const year = now.getUTCFullYear();
    const lastCompleteMonthIndex = now.getUTCMonth() - 1; // 0-based; -1 im Januar
    const monthNames = ["Jan", "Feb", "Mär", "Apr", "Mai", "Jun", "Jul", "Aug", "Sep", "Okt", "Nov", "Dez"];
    return {
      previousYear: year - 1,
      ytdMonthLabel:
        lastCompleteMonthIndex >= 0 ? monthNames[lastCompleteMonthIndex] : "–",
    };
  }, []);

  return (
    <section className="mb-4 space-y-3">
      {scopedAsin && (
        <p className="text-xs text-slate-500">
          KPIs für ASIN <span className="font-medium text-slate-700">{scopedAsin}</span>
          {kpiItems.length > 1 ? ` (${kpiItems.length} SKUs)` : ""}
        </p>
      )}
      <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-4">
        {[
          {
            label: "Verkäufe YTD",
            value: loading ? "…" : nf.format(kpis.comparisonCurrent),
            hint: loading ? undefined : `Jan–${ytdMonthLabel}`,
          },
          {
            label: "Wachstum YTD",
            value: loading ? "…" : formatGrowth(kpis.growthPercent),
            hint: `gegenüber ${previousYear}`,
          },
          { label: "Bestand verfügbar", value: loading ? "…" : nf.format(kpis.available) },
          selectedSku
            ? {
                label: "Nachbestellen?",
                value: loading ? "…" : kpis.atRisk > 0 ? "Ja" : "Nein",
                accent: !loading && kpis.atRisk > 0,
                ok: !loading && kpis.atRisk === 0,
              }
            : {
                label: "Gefährdete Produkte",
                value: loading ? "…" : nf.format(kpis.atRisk),
                accent: kpis.atRisk > 0,
              },
        ].map((card) => (
          <div
            key={card.label}
            className="rounded-2xl border border-slate-200 bg-white px-3 py-3 text-center shadow-sm"
          >
            <div className="text-[11px] font-medium uppercase tracking-wide text-slate-500">
              {card.label}
            </div>
            <div
              className={`mt-1 text-xl font-semibold tabular-nums ${
                "ok" in card && card.ok
                  ? "text-emerald-700"
                  : card.accent
                    ? "text-red-700"
                    : "text-slate-950"
              }`}
            >
              {card.value}
            </div>
            {"hint" in card && card.hint ? (
              <div className="mt-0.5 text-[10px] leading-tight text-slate-400">{card.hint}</div>
            ) : null}
          </div>
        ))}
      </div>

      {!selectedSku && (
      <div className="rounded-2xl border border-slate-200 bg-white p-3 shadow-sm md:p-4">
        <div className="mb-2 flex items-baseline justify-between gap-2">
          <h2 className="text-base font-semibold text-slate-950">Handlungsbedarf</h2>
          <span className="text-xs text-slate-500">nur kritisch / leer</span>
        </div>

        {error && (
          <div className="rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">
            {error}
          </div>
        )}

        {loading && !data && (
          <div className="space-y-2 py-1">
            {Array.from({ length: 4 }).map((_, i) => (
              <div key={i} className="h-11 animate-pulse rounded-lg bg-slate-100" />
            ))}
          </div>
        )}

        {!loading && !error && riskItems.length === 0 && (
          <p className="py-4 text-center text-sm text-slate-500">
            Kein kritischer Handlungsbedarf – keine leeren oder ≤30-Tage-SKUs.
          </p>
        )}

        {riskItems.length > 0 && (
          <ul className="divide-y divide-slate-100">
            {riskItems.map((item) => (
              <li key={`${item.asin}-${item.sku}`}>
                <button
                  type="button"
                  onClick={() => onSelectSku?.(item.sku)}
                  className={`flex w-full items-center gap-3 px-1 py-2.5 text-left transition hover:bg-slate-50 ${
                    selectedSku === item.sku ? "bg-slate-50" : ""
                  }`}
                >
                  <span className={`h-2.5 w-2.5 shrink-0 rounded-full ${statusTone[item.status]}`} />
                  <div className="min-w-0 flex-1">
                    <div className="truncate text-sm font-medium text-slate-900">
                      {item.productName || item.sku}
                    </div>
                    <div className="truncate text-xs text-slate-500">
                      {item.sku}
                      {item.asin ? ` · ${item.asin}` : ""}
                    </div>
                  </div>
                  <div className="shrink-0 text-right">
                    <div className="text-xs font-medium text-slate-800">
                      {inventoryActionHint(item)}
                    </div>
                    <div className="text-[11px] tabular-nums text-slate-500">
                      {nf.format(item.available)} Stk
                      {item.inbound > 0 ? ` · +${nf.format(item.inbound)} In` : ""}
                    </div>
                  </div>
                </button>
              </li>
            ))}
          </ul>
        )}
      </div>
      )}
    </section>
  );
}

export function InventoryTableSection({
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
  return (
    <InventoryOverviewTable
      data={data}
      loading={loading}
      error={error}
      onReload={onReload}
      selectedSku={selectedSku}
      onSelectSku={onSelectSku}
    />
  );
}

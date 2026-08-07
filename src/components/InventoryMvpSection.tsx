"use client";

import { useMemo } from "react";
import InventoryOverviewTable from "@/components/InventoryOverviewTable";
import {
  coverActionKpisForItem,
  coverActionKpisForItems,
  formatCoverDaysDe,
  formatInDaysDe,
  inventoryActionHint,
  selectOosRiskItems,
  type CoverActionKpis,
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

function shipValue(kpis: CoverActionKpis, loading: boolean): { value: string; hint?: string; accent?: boolean } {
  if (loading) return { value: "…" };
  if (kpis.daysUntilShip != null) {
    return {
      value: formatInDaysDe(kpis.daysUntilShip),
      accent: kpis.daysUntilShip <= 7,
    };
  }
  if (kpis.shipUnavailableReason === "no_local") {
    return { value: "–", hint: "kein Lokalbestand" };
  }
  return { value: "–", hint: "kein Absatztempo" };
}

function orderValue(kpis: CoverActionKpis, loading: boolean): { value: string; hint?: string; accent?: boolean; ok?: boolean } {
  if (loading) return { value: "…" };
  if (kpis.daysUntilOrder != null) {
    return {
      value: formatInDaysDe(kpis.daysUntilOrder),
      accent: kpis.daysUntilOrder <= 14,
    };
  }
  if (kpis.orderUnavailableReason === "already_ordered") {
    return { value: "bereits bestellt", ok: true };
  }
  if (kpis.orderUnavailableReason === "no_lead") {
    return { value: "–", hint: "Leadzeit fehlt" };
  }
  return { value: "–", hint: "kein Absatztempo" };
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
  const focusItem = useMemo(() => {
    const sku = selectedSku?.trim();
    if (!sku) return null;
    return (data?.items ?? []).find((item) => item.sku === sku) || null;
  }, [data?.items, selectedSku]);

  const actionKpis = useMemo(() => {
    if (focusItem) return coverActionKpisForItem(focusItem);
    return coverActionKpisForItems(data?.items ?? []);
  }, [data?.items, focusItem]);

  const riskItems = useMemo(
    () => selectOosRiskItems(data?.items ?? [], 12),
    [data?.items],
  );

  const ship = shipValue(actionKpis, loading);
  const order = orderValue(actionKpis, loading);

  return (
    <section className="mb-4 space-y-3">
      {focusItem && (
        <p className="text-xs text-slate-500">
          KPIs für SKU <span className="font-medium text-slate-700">{focusItem.sku}</span>
          {focusItem.asin ? (
            <>
              {" "}
              · ASIN <span className="font-medium text-slate-700">{focusItem.asin}</span>
            </>
          ) : null}
        </p>
      )}
      {!focusItem && !loading && (data?.items?.length ?? 0) > 0 && (
        <p className="text-xs text-slate-500">
          Portfolio: kürzeste Reichweite / nächste Aktion über alle SKUs
        </p>
      )}
      <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-4">
        {[
          {
            label: "Amazon-Reichweite",
            value: loading ? "…" : formatCoverDaysDe(actionKpis.amazonCoverDays),
            accent:
              !loading &&
              actionKpis.amazonCoverDays != null &&
              actionKpis.amazonCoverDays <= 30,
          },
          {
            label: "Gesamt-Reichweite",
            value: loading ? "…" : formatCoverDaysDe(actionKpis.gesamtCoverDays),
            accent:
              !loading &&
              actionKpis.gesamtCoverDays != null &&
              actionKpis.gesamtCoverDays <= 30,
          },
          {
            label: "Nächste Aktion Amz Lager senden",
            value: ship.value,
            hint: ship.hint,
            accent: ship.accent,
          },
          {
            label: "Nächste Aktion Lieferantenbestellung",
            value: order.value,
            hint: order.hint,
            accent: order.accent,
            ok: order.ok,
          },
        ].map((card) => (
          <div
            key={card.label}
            className="rounded-2xl border border-slate-200 bg-white px-3 py-3 text-center shadow-sm"
          >
            <div className="text-[11px] font-medium leading-snug text-slate-500">
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
                      {(item.localQty || 0) > 0 ? ` · ${nf.format(item.localQty || 0)} lokal` : ""}
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

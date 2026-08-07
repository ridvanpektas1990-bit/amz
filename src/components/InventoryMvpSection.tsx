"use client";

import { useMemo } from "react";
import InventoryOverviewTable from "@/components/InventoryOverviewTable";
import {
  actionPlanWhenLabel,
  buildInventoryActionPlan,
  coverActionKpisForItem,
  formatCoverDaysDe,
  formatInDaysDe,
  suggestedAmazonShipQty,
  suggestedSupplierOrderQty,
  type CoverActionKpis,
  type InventoryActionKind,
  type InventoryOverviewResponse,
} from "@/lib/inventory-overview";

function formatOosDateDe(iso: string | null | undefined): string | null {
  if (!iso) return null;
  const ms = Date.parse(`${iso.slice(0, 10)}T12:00:00Z`);
  if (!Number.isFinite(ms)) return null;
  return new Intl.DateTimeFormat("de-DE", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
  }).format(new Date(ms));
}

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

const actionTone: Record<InventoryActionKind, string> = {
  sold_out: "bg-slate-100 text-slate-800 ring-slate-200",
  ship_amazon: "bg-sky-50 text-sky-800 ring-sky-200",
  order_supplier: "bg-amber-50 text-amber-900 ring-amber-200",
  missing_lead: "bg-violet-50 text-violet-800 ring-violet-200",
};

export function InventorySummarySection({
  data,
  loading,
  error,
  selectedSku,
  onSelectSku,
  /** When set (SKU detail on dashboard), override ship qty so KPIs match Nachbestellung. */
  recommendedShipQty = null,
  /** When set (SKU detail on dashboard), override supplier order qty so KPIs match Nachbestellung. */
  recommendedOrderQty = null,
}: {
  data: InventoryOverviewResponse | null;
  loading: boolean;
  error: string | null;
  selectedSku?: string;
  onSelectSku?: (sku: string) => void;
  recommendedShipQty?: number | null;
  recommendedOrderQty?: number | null;
}) {
  const focusItem = useMemo(() => {
    const sku = selectedSku?.trim();
    if (!sku) return null;
    return (data?.items ?? []).find((item) => item.sku === sku) || null;
  }, [data?.items, selectedSku]);

  const nf = useMemo(() => new Intl.NumberFormat("de-DE"), []);

  const actionKpis = useMemo(
    () => (focusItem ? coverActionKpisForItem(focusItem) : null),
    [focusItem],
  );

  const actionPlan = useMemo(
    () => buildInventoryActionPlan(data?.items ?? []),
    [data?.items],
  );

  const ship = actionKpis ? shipValue(actionKpis, loading) : null;
  const order = actionKpis ? orderValue(actionKpis, loading) : null;

  const amazonOosLabel = formatOosDateDe(focusItem?.estimatedOosDate);
  const gesamtOosLabel = formatOosDateDe(
    focusItem?.estimatedOosDateAmazonAndLocal ??
      focusItem?.estimatedOosDateWithLocal ??
      focusItem?.estimatedOosDate,
  );
  const shipQty =
    recommendedShipQty != null && recommendedShipQty > 0
      ? recommendedShipQty
      : focusItem && !loading
        ? suggestedAmazonShipQty(focusItem)
        : 0;
  const orderQty =
    recommendedOrderQty != null && recommendedOrderQty > 0
      ? recommendedOrderQty
      : focusItem && !loading
        ? suggestedSupplierOrderQty(focusItem)
        : 0;

  const onOrderUnits = Math.max(0, Math.floor(Number(focusItem?.onOrderUnits) || 0));
  const onOrderDateLabel = formatOosDateDe(focusItem?.onOrderOrderedAt);
  const alreadyOrderedSub =
    onOrderUnits > 0
      ? onOrderDateLabel
        ? `${nf.format(onOrderUnits)} Stk am ${onOrderDateLabel} bestellt`
        : `${nf.format(onOrderUnits)} Stk bestellt`
      : null;

  return (
    <section className="mb-4 space-y-3">
      {focusItem && actionKpis && ship && order ? (
        <>
          <p className="text-xs text-slate-500">
            KPIs für SKU <span className="font-medium text-slate-700">{focusItem.sku}</span>
            {focusItem.asin ? (
              <>
                {" "}
                · ASIN <span className="font-medium text-slate-700">{focusItem.asin}</span>
              </>
            ) : null}
          </p>
          <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-4">
            {[
              {
                label: "Amazon-Reichweite",
                value: loading ? "…" : formatCoverDaysDe(actionKpis.amazonCoverDays),
                sub: amazonOosLabel ? `OOS Datum: ${amazonOosLabel}` : null,
              },
              {
                label: "Gesamt-Reichweite",
                value: loading ? "…" : formatCoverDaysDe(actionKpis.gesamtCoverDays),
                sub: gesamtOosLabel ? `OOS Datum: ${gesamtOosLabel}` : null,
              },
              {
                label: "An Amazon senden",
                value: ship.value,
                hint: ship.hint,
                sub:
                  shipQty > 0 ? `Empfohlen: ${nf.format(shipQty)} Stück` : null,
              },
              {
                label: "Produktbestellung",
                value: order.value,
                hint: order.hint,
                sub:
                  order.ok && alreadyOrderedSub
                    ? alreadyOrderedSub
                    : orderQty > 0
                      ? `Empfohlen: ${nf.format(orderQty)} Stück`
                      : null,
                ok: order.ok,
              },
            ].map((card) => (
              <div
                key={card.label}
                className="rounded-2xl border border-slate-200 bg-white px-3 py-3 text-center shadow-sm"
              >
                <div className="text-[11px] font-medium leading-snug text-slate-900">
                  {card.label}
                </div>
                <div
                  className={`mt-1 text-xl font-semibold tabular-nums ${
                    "ok" in card && card.ok ? "text-emerald-700" : "text-slate-900"
                  }`}
                >
                  {card.value}
                </div>
                {card.sub ? (
                  <div className="mt-0.5 text-[10px] italic leading-tight text-slate-900">
                    {card.sub}
                  </div>
                ) : null}
                {"hint" in card && card.hint ? (
                  <div className="mt-0.5 text-[10px] leading-tight text-slate-500">{card.hint}</div>
                ) : null}
              </div>
            ))}
          </div>
        </>
      ) : (
        <div className="overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm">
          <div className="flex flex-wrap items-baseline justify-between gap-2 border-b border-slate-200 px-3 py-2.5">
            <div>
              <h2 className="text-sm font-semibold text-slate-950">Aktionsplan</h2>
              <p className="text-[11px] text-slate-500">
                Nächste To-dos · sortiert nach Fälligkeit · Horizont 21 Tage
              </p>
            </div>
            <span className="text-xs tabular-nums text-slate-500">
              {loading ? "…" : `${actionPlan.length} Aktionen`}
            </span>
          </div>

          {error && (
            <div className="m-2 rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">
              {error}
            </div>
          )}

          {loading && !data && (
            <div className="space-y-2 px-3 py-3">
              {Array.from({ length: 5 }).map((_, i) => (
                <div key={i} className="h-10 animate-pulse rounded-lg bg-slate-100" />
              ))}
            </div>
          )}

          {!loading && !error && actionPlan.length === 0 && (
            <p className="px-3 py-8 text-center text-sm text-slate-500">
              Keine anstehenden Aktionen in den nächsten 21 Tagen.
            </p>
          )}

          {actionPlan.length > 0 && (
            <div className="max-h-[22rem] overflow-auto">
              <table className="w-full min-w-[720px] border-collapse text-left text-[13px]">
                <thead className="sticky top-0 z-10 bg-slate-50/95 text-[10px] uppercase tracking-wide text-slate-500">
                  <tr className="border-b border-slate-200">
                    <th className="px-3 py-2 font-semibold">Wann</th>
                    <th className="px-2 py-2 font-semibold">Aktion</th>
                    <th className="px-2 py-2 font-semibold">Menge</th>
                    <th className="px-2 py-2 font-semibold">Produkt</th>
                    <th className="px-3 py-2 pr-3 font-semibold">Details</th>
                  </tr>
                </thead>
                <tbody>
                  {actionPlan.map((row) => {
                    const dueNow = row.daysUntil <= 0;
                    return (
                      <tr
                        key={row.id}
                        className={`border-b border-slate-100 transition hover:bg-slate-50/80 ${
                          dueNow ? "bg-orange-50/70" : ""
                        }`}
                      >
                        <td className="px-3 py-2.5 align-middle">
                          <button
                            type="button"
                            onClick={() => onSelectSku?.(row.sku)}
                            className={`text-left text-[13px] font-semibold tabular-nums ${
                              dueNow ? "text-red-700" : "text-slate-900"
                            }`}
                          >
                            {actionPlanWhenLabel(row.daysUntil)}
                          </button>
                        </td>
                        <td className="px-2 py-2.5 align-middle">
                          <button
                            type="button"
                            onClick={() => onSelectSku?.(row.sku)}
                            className={`inline-flex rounded px-1.5 py-0.5 text-[11px] font-semibold ring-1 ring-inset ${actionTone[row.kind]}`}
                          >
                            {row.actionLabel}
                          </button>
                        </td>
                        <td className="px-2 py-2.5 align-middle">
                          <button
                            type="button"
                            onClick={() => onSelectSku?.(row.sku)}
                            className="text-left"
                          >
                            {row.qtySuggested != null && row.qtySuggested > 0 ? (
                              <>
                                <span className="block text-[15px] font-semibold tabular-nums text-slate-950">
                                  {nf.format(row.qtySuggested)} Stk
                                </span>
                                {row.qtyBasis ? (
                                  <span className="block text-[10px] text-slate-500">{row.qtyBasis}</span>
                                ) : null}
                              </>
                            ) : (
                              <span className="text-[12px] text-slate-400">
                                {row.kind === "missing_lead"
                                  ? "–"
                                  : row.qtyBasis === "Leadzeit fehlt"
                                    ? "Lead fehlt"
                                    : "–"}
                              </span>
                            )}
                          </button>
                        </td>
                        <td className="px-2 py-2.5 align-middle">
                          <button
                            type="button"
                            onClick={() => onSelectSku?.(row.sku)}
                            className="flex max-w-[260px] items-center gap-2 text-left"
                          >
                            <span className="flex h-8 w-8 shrink-0 items-center justify-center overflow-hidden rounded border border-slate-200 bg-white">
                              {row.imageUrl ? (
                                <img
                                  src={row.imageUrl}
                                  alt=""
                                  className="h-full w-full object-contain p-0.5"
                                  loading="lazy"
                                  referrerPolicy="no-referrer"
                                />
                              ) : (
                                <span className="h-2 w-2 rounded-full bg-slate-300" />
                              )}
                            </span>
                            <span className="min-w-0">
                              <span className="block truncate text-[13px] font-medium text-slate-900">
                                {row.productName || row.sku}
                              </span>
                              <span className="block truncate text-[10px] text-slate-500">
                                {row.sku}
                                {row.asin ? ` · ${row.asin}` : ""}
                              </span>
                            </span>
                          </button>
                        </td>
                        <td className="px-3 py-2.5 pr-3 align-middle text-[12px] text-slate-600">
                          {row.context}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}

          <div className="border-t border-slate-100 px-3 py-1.5 text-[10px] text-slate-500">
            Menge = Amz senden: LY/Tempo für Zielreichweite (inkl. Growth) · Lieferant:
            Wochen-Charge Lead+Puffer · Klick öffnet das Produkt
          </div>
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

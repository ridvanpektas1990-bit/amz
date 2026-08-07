"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import {
  buildSupplierOrderMessage,
  type CartonSpec,
  type SupplierMessageLang,
} from "@/lib/carton-specs";
import type { InventoryOverviewResponse } from "@/lib/inventory-overview";
import {
  actionLabel,
  buildReorderBoardRows,
  timingLabel,
  type ReorderBoardRow,
} from "@/lib/reorder-board";

type SpecsResponse = {
  ok: boolean;
  error?: string;
  items?: Array<
    CartonSpec & {
      sellerSku: string;
    }
  >;
};

const nf = new Intl.NumberFormat("de-DE");

function timingTone(row: ReorderBoardRow): string {
  if (row.missingLeadTime) return "bg-amber-50 text-amber-900 ring-amber-200";
  const status = row.timing?.status;
  if (status === "already_oos" || status === "too_late") return "bg-rose-50 text-rose-800 ring-rose-200";
  if (status === "order_now") return "bg-amber-50 text-amber-900 ring-amber-200";
  return "bg-sky-50 text-sky-800 ring-sky-200";
}

export default function NachbestellungPage() {
  const [overview, setOverview] = useState<InventoryOverviewResponse | null>(null);
  const [specs, setSpecs] = useState<SpecsResponse["items"]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [copiedSku, setCopiedSku] = useState<string | null>(null);
  const [filter, setFilter] = useState<"action" | "all_due" | "all">("action");
  const [messageModal, setMessageModal] = useState<{
    sku: string;
    productName: string | null;
    asin: string;
    orderQty: number;
    cartons: number | null;
    unitsPerCarton: number | null;
    cartonLenCm: number | null;
    cartonWCm: number | null;
    cartonHCm: number | null;
    lang: SupplierMessageLang;
    text: string;
  } | null>(null);

  async function load() {
    setLoading(true);
    setError(null);
    try {
      const [overviewRes, specsRes] = await Promise.all([
        fetch("/api/inventory/overview", { cache: "no-store" }),
        fetch("/api/inventory/carton-specs", { cache: "no-store" }),
      ]);
      const overviewJson = (await overviewRes.json()) as InventoryOverviewResponse;
      const specsJson = (await specsRes.json()) as SpecsResponse;
      if (!overviewRes.ok || !overviewJson.ok) {
        throw new Error(overviewJson.error || "Bestandsübersicht nicht ladbar");
      }
      if (!specsRes.ok || !specsJson.ok) {
        throw new Error(specsJson.error || "Stammdaten nicht ladbar");
      }
      setOverview(overviewJson);
      setSpecs(specsJson.items || []);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void load();
  }, []);

  const rows = useMemo(() => {
    const map = new Map<
      string,
      Pick<
        CartonSpec,
        | "productionTimeDays"
        | "shippingTimeDays"
        | "bufferTimeDays"
        | "unitsPerCarton"
        | "cartonLenCm"
        | "cartonWCm"
        | "cartonHCm"
      >
    >();
    for (const spec of specs || []) {
      map.set(spec.sellerSku, {
        productionTimeDays: spec.productionTimeDays,
        shippingTimeDays: spec.shippingTimeDays,
        bufferTimeDays: spec.bufferTimeDays,
        unitsPerCarton: spec.unitsPerCarton,
        cartonLenCm: spec.cartonLenCm,
        cartonWCm: spec.cartonWCm,
        cartonHCm: spec.cartonHCm,
      });
    }
    return buildReorderBoardRows(overview?.items || [], map, {
      includeAllActive: filter === "all",
    });
  }, [overview, specs, filter]);

  const visibleRows = useMemo(() => {
    if (filter === "all" || filter === "all_due") return rows;
    return rows.filter(
      (row) =>
        row.missingLeadTime ||
        row.action === "order_supplier" ||
        row.action === "replenish_amazon" ||
        row.action === "awaiting_supplier" ||
        row.timing?.status === "already_oos" ||
        row.timing?.status === "too_late" ||
        row.timing?.status === "order_now",
    );
  }, [rows, filter]);

  function messageForRow(row: ReorderBoardRow, lang: SupplierMessageLang): string {
    return buildSupplierOrderMessage({
      productName: row.productName,
      sku: row.sku,
      asin: row.asin,
      orderQty: row.messageOrderQty || row.orderQty,
      cartons: row.messageCartons ?? row.cartons,
      unitsPerCarton: row.unitsPerCarton,
      cartonLenCm: row.cartonLenCm,
      cartonWCm: row.cartonWCm,
      cartonHCm: row.cartonHCm,
      lang,
    });
  }

  function openMessageModal(row: ReorderBoardRow) {
    if (!row.supplierMessage) return;
    const lang: SupplierMessageLang = "de";
    const orderQty = row.messageOrderQty || row.orderQty;
    const cartons = row.messageCartons ?? row.cartons;
    setCopiedSku(null);
    setMessageModal({
      sku: row.sku,
      productName: row.productName,
      asin: row.asin,
      orderQty,
      cartons,
      unitsPerCarton: row.unitsPerCarton,
      cartonLenCm: row.cartonLenCm,
      cartonWCm: row.cartonWCm,
      cartonHCm: row.cartonHCm,
      lang,
      text: messageForRow(row, lang),
    });
  }

  function setMessageLang(lang: SupplierMessageLang) {
    setMessageModal((current) => {
      if (!current) return current;
      return {
        ...current,
        lang,
        text: buildSupplierOrderMessage({
          productName: current.productName,
          sku: current.sku,
          asin: current.asin,
          orderQty: current.orderQty,
          cartons: current.cartons,
          unitsPerCarton: current.unitsPerCarton,
          cartonLenCm: current.cartonLenCm,
          cartonWCm: current.cartonWCm,
          cartonHCm: current.cartonHCm,
          lang,
        }),
      };
    });
    setCopiedSku(null);
  }

  function closeMessageModal() {
    setMessageModal(null);
    setCopiedSku(null);
  }

  async function copyModalText() {
    if (!messageModal?.text) return;
    try {
      await navigator.clipboard.writeText(messageModal.text);
      setCopiedSku(messageModal.sku);
      window.setTimeout(() => setCopiedSku((current) => (current === messageModal.sku ? null : current)), 2000);
    } catch {
      setError("Clipboard nicht verfügbar");
    }
  }

  useEffect(() => {
    if (!messageModal) return;
    function onKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        setMessageModal(null);
        setCopiedSku(null);
      }
    }
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [messageModal]);

  return (
    <div className="mx-auto max-w-6xl px-4 py-5 md:py-6">
      <div className="mb-4 flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 className="text-lg font-semibold tracking-tight text-slate-950 md:text-xl">Nachbestellung</h1>
          <p className="mt-0.5 text-sm text-slate-500">
            Bestellmengen · lokaler Bestand · Lieferanten-PO · optional alle aktiven SKUs
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <div className="flex rounded-full border border-slate-200 bg-white p-0.5 text-xs shadow-sm">
            <button
              type="button"
              onClick={() => setFilter("action")}
              className={`rounded-full px-3 py-1.5 font-medium transition ${
                filter === "action" ? "bg-slate-900 text-white" : "text-slate-600 hover:bg-slate-50"
              }`}
            >
              Jetzt / kritisch
            </button>
            <button
              type="button"
              onClick={() => setFilter("all_due")}
              className={`rounded-full px-3 py-1.5 font-medium transition ${
                filter === "all_due" ? "bg-slate-900 text-white" : "text-slate-600 hover:bg-slate-50"
              }`}
            >
              inkl. nächste 21 Tage
            </button>
            <button
              type="button"
              onClick={() => setFilter("all")}
              className={`rounded-full px-3 py-1.5 font-medium transition ${
                filter === "all" ? "bg-slate-900 text-white" : "text-slate-600 hover:bg-slate-50"
              }`}
            >
              Alle Produkte
            </button>
          </div>
          <button
            type="button"
            onClick={() => void load()}
            className="rounded-full border border-slate-200 bg-white px-3 py-1.5 text-xs font-medium text-slate-600 shadow-sm hover:bg-slate-50"
          >
            Neu laden
          </button>
          <Link
            href="/sku-stammdaten"
            className="rounded-full border border-slate-200 bg-white px-3 py-1.5 text-xs font-medium text-slate-600 shadow-sm hover:bg-slate-50"
          >
            Stammdaten
          </Link>
        </div>
      </div>

      {loading && (
        <div className="space-y-2">
          {Array.from({ length: 4 }).map((_, i) => (
            <div key={i} className="h-20 animate-pulse rounded-2xl bg-slate-100" />
          ))}
        </div>
      )}

      {error && (
        <div className="mb-4 rounded-xl border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-800">
          {error}
        </div>
      )}

      {!loading && !error && (
        <>
          <div className="mb-3 grid gap-2 sm:grid-cols-3">
            <div className="rounded-2xl border border-slate-200 bg-white px-3 py-3 text-center shadow-sm">
              <div className="text-[11px] font-medium uppercase tracking-wide text-slate-500">Treffer</div>
              <div className="mt-1 text-xl font-semibold tabular-nums text-slate-950">
                {nf.format(visibleRows.length)}
              </div>
            </div>
            <div className="rounded-2xl border border-slate-200 bg-white px-3 py-3 text-center shadow-sm">
              <div className="text-[11px] font-medium uppercase tracking-wide text-slate-500">
                Stück zu bestellen
              </div>
              <div className="mt-1 text-xl font-semibold tabular-nums text-teal-800">
                {nf.format(
                  visibleRows
                    .filter((row) => row.action === "order_supplier")
                    .reduce((sum, row) => sum + row.orderQty, 0),
                )}
              </div>
            </div>
            <div className="rounded-2xl border border-slate-200 bg-white px-3 py-3 text-center shadow-sm">
              <div className="text-[11px] font-medium uppercase tracking-wide text-slate-500">
                Ohne Lieferzeit
              </div>
              <div className="mt-1 text-xl font-semibold tabular-nums text-amber-800">
                {nf.format(visibleRows.filter((row) => row.missingLeadTime).length)}
              </div>
            </div>
          </div>

          {visibleRows.length === 0 ? (
            <div className="rounded-2xl border border-slate-200 bg-white px-4 py-10 text-center shadow-sm">
              <p className="text-sm font-medium text-slate-900">
                {filter === "all" ? "Keine aktiven Produkte" : "Kein akuter Bestellbedarf"}
              </p>
              <p className="mt-1 text-sm text-slate-500">
                {filter === "all"
                  ? "Es sind keine aktiven Listings vorhanden."
                  : "Im gewählten Filter gibt es gerade keine SKUs mit Bestelllücke. Mit „Alle Produkte“ kannst du trotzdem vorzeitig bestellen."}
              </p>
              {filter !== "all" ? (
                <button
                  type="button"
                  onClick={() => setFilter("all")}
                  className="mt-3 inline-block text-sm font-medium text-slate-900 underline"
                >
                  Alle Produkte anzeigen
                </button>
              ) : (
                <Link
                  href="/dashboard"
                  className="mt-3 inline-block text-sm font-medium text-slate-900 underline"
                >
                  Zum Dashboard
                </Link>
              )}
            </div>
          ) : (
            <div className="overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm">
              <div className="h-[min(36rem,70vh)] overflow-auto">
                <table className="min-w-[900px] w-full border-collapse text-left text-sm">
                  <thead className="sticky top-0 z-10 bg-slate-50 text-[10px] uppercase tracking-wide text-slate-500">
                    <tr className="border-b border-slate-200">
                      <th className="px-3 py-2 font-semibold">Produkt</th>
                      <th className="px-2 py-2 font-semibold">Aktion</th>
                      <th className="px-2 py-2 font-semibold">Timing</th>
                      <th className="px-2 py-2 text-right font-semibold">Cover</th>
                      <th className="px-2 py-2 text-right font-semibold">Lokal</th>
                      <th className="px-2 py-2 text-right font-semibold">Menge</th>
                      <th className="px-2 py-2 text-right font-semibold">Kartons</th>
                      <th className="px-3 py-2 font-semibold">Weiter</th>
                    </tr>
                  </thead>
                  <tbody>
                    {visibleRows.map((row) => (
                      <tr key={row.sku} className="border-b border-slate-100 align-top">
                        <td className="px-3 py-2.5">
                          <div className="flex max-w-[320px] items-center gap-2">
                            <div className="flex h-10 w-10 shrink-0 items-center justify-center overflow-hidden rounded-lg border border-slate-200 bg-white">
                              {row.imageUrl ? (
                                <img
                                  src={row.imageUrl}
                                  alt=""
                                  className="h-full w-full object-contain p-0.5"
                                  loading="lazy"
                                  referrerPolicy="no-referrer"
                                />
                              ) : (
                                <span className="text-[9px] font-semibold uppercase text-slate-400">SKU</span>
                              )}
                            </div>
                            <div className="min-w-0">
                              <div className="truncate font-medium text-slate-900">
                                {row.productName || row.sku}
                              </div>
                              <div className="truncate text-[11px] text-slate-500">
                                {row.sku}
                                {row.asin ? ` · ${row.asin}` : ""}
                              </div>
                              <div className="text-[11px] tabular-nums text-slate-500">
                                Bestand {nf.format(row.available)}
                                {row.inbound > 0 ? ` · +${nf.format(row.inbound)} In` : ""}
                                {row.onOrderUnits > 0
                                  ? ` · ${nf.format(row.onOrderUnits)} bestellt`
                                  : ""}
                              </div>
                            </div>
                          </div>
                        </td>
                        <td className="px-2 py-2.5">
                          <span
                            className={`inline-flex rounded-md px-2 py-1 text-[11px] font-semibold ring-1 ring-inset ${
                              row.action === "order_supplier"
                                ? "bg-rose-50 text-rose-800 ring-rose-200"
                                : row.action === "replenish_amazon"
                                  ? "bg-sky-50 text-sky-800 ring-sky-200"
                                  : row.action === "awaiting_supplier"
                                    ? "bg-violet-50 text-violet-800 ring-violet-200"
                                    : row.action === "ok"
                                      ? "bg-emerald-50 text-emerald-800 ring-emerald-200"
                                      : "bg-amber-50 text-amber-900 ring-amber-200"
                            }`}
                          >
                            {actionLabel(row.action)}
                          </span>
                          {row.action === "replenish_amazon" && (
                            <div className="mt-1 text-[10px] text-slate-500">
                              Transfer {row.transferLeadDays}T
                            </div>
                          )}
                        </td>
                        <td className="px-2 py-2.5">
                          <span
                            className={`inline-flex rounded-md px-2 py-1 text-[11px] font-semibold ring-1 ring-inset ${timingTone(row)}`}
                          >
                            {timingLabel(row.timing, row.missingLeadTime)}
                          </span>
                          {row.leadDays != null && (
                            <div className="mt-1 text-[10px] text-slate-500">
                              Lead {row.leadDays}T
                              {row.bufferDays > 0 ? ` + ${row.bufferDays}T Puffer` : ""}
                            </div>
                          )}
                        </td>
                        <td className="px-2 py-2.5 text-right tabular-nums text-slate-800">
                          {row.daysOfCover == null
                            ? "–"
                            : row.daysOfCover === 0
                              ? "leer"
                              : `${nf.format(row.daysOfCover)} T`}
                        </td>
                        <td className="px-2 py-2.5 text-right tabular-nums text-slate-700">
                          {row.localQty > 0 ? nf.format(row.localQty) : "–"}
                        </td>
                        <td className="px-2 py-2.5 text-right">
                          {row.missingLeadTime ? (
                            <span className="text-slate-400">–</span>
                          ) : (
                            <>
                              <div className="font-semibold tabular-nums text-teal-900">
                                {nf.format(row.messageOrderQty || row.orderQty)}
                              </div>
                              {row.orderQty === 0 &&
                              row.messageOrderQty > 0 &&
                              row.onOrderUnits > 0 ? (
                                <div className="text-[10px] text-slate-500">
                                  netto 0 · {nf.format(row.onOrderUnits)} bestellt
                                </div>
                              ) : row.rounded ? (
                                <div className="text-[10px] text-slate-500">
                                  roh {nf.format(row.rawQty)}
                                </div>
                              ) : null}
                            </>
                          )}
                        </td>
                        <td className="px-2 py-2.5 text-right tabular-nums text-slate-700">
                          {(row.messageCartons ?? row.cartons) != null && row.unitsPerCarton
                            ? `${nf.format(row.messageCartons ?? row.cartons!)} × ${nf.format(row.unitsPerCarton)}`
                            : "–"}
                        </td>
                        <td className="px-3 py-2.5">
                          <div className="flex flex-wrap gap-1.5">
                            {row.missingLeadTime || row.action === "missing_lead" ? (
                              <Link
                                href="/sku-stammdaten"
                                className="rounded-lg border border-amber-200 bg-amber-50 px-2.5 py-1.5 text-xs font-medium text-amber-950 hover:bg-amber-100"
                              >
                                Stammdaten
                              </Link>
                            ) : null}
                            {row.action === "replenish_amazon" ? (
                              <Link
                                href="/sku-stammdaten"
                                className="rounded-lg border border-sky-200 bg-sky-50 px-2.5 py-1.5 text-xs font-medium text-sky-950 hover:bg-sky-100"
                              >
                                Lokal prüfen
                              </Link>
                            ) : null}
                            {row.supplierMessage ? (
                              <button
                                type="button"
                                onClick={() => openMessageModal(row)}
                                className="rounded-lg bg-slate-900 px-2.5 py-1.5 text-xs font-medium text-white hover:bg-slate-700"
                              >
                                Text kopieren
                              </button>
                            ) : null}
                          </div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </>
      )}

      {messageModal && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-black/30 p-4"
          onClick={closeMessageModal}
          role="presentation"
        >
          <div
            className="w-full max-w-lg rounded-2xl border border-slate-200 bg-white p-4 shadow-xl"
            onClick={(event) => event.stopPropagation()}
            role="dialog"
            aria-modal="true"
            aria-labelledby="supplier-message-title"
          >
            <div className="mb-3 flex items-start justify-between gap-3">
              <div className="min-w-0">
                <h2 id="supplier-message-title" className="text-base font-semibold text-slate-950">
                  Supplier-Text
                </h2>
                <p className="mt-0.5 truncate text-sm text-slate-500">
                  {messageModal.productName || messageModal.sku}
                  {messageModal.productName ? ` · ${messageModal.sku}` : ""}
                </p>
              </div>
              <button
                type="button"
                onClick={closeMessageModal}
                className="rounded-lg border border-slate-200 px-2 py-1 text-sm text-slate-600 hover:bg-slate-50"
                aria-label="Schließen"
              >
                ✕
              </button>
            </div>

            <div className="mb-3 flex rounded-full border border-slate-200 bg-slate-50 p-0.5 text-xs">
              <button
                type="button"
                onClick={() => setMessageLang("de")}
                className={`flex-1 rounded-full px-3 py-1.5 font-medium transition ${
                  messageModal.lang === "de"
                    ? "bg-white text-slate-900 shadow-sm"
                    : "text-slate-600 hover:text-slate-900"
                }`}
              >
                Deutsch
              </button>
              <button
                type="button"
                onClick={() => setMessageLang("en")}
                className={`flex-1 rounded-full px-3 py-1.5 font-medium transition ${
                  messageModal.lang === "en"
                    ? "bg-white text-slate-900 shadow-sm"
                    : "text-slate-600 hover:text-slate-900"
                }`}
              >
                English
              </button>
            </div>

            <label className="sr-only" htmlFor="supplier-message-text">
              Text bearbeiten
            </label>
            <textarea
              id="supplier-message-text"
              value={messageModal.text}
              onChange={(event) =>
                setMessageModal((current) =>
                  current ? { ...current, text: event.target.value } : current,
                )
              }
              rows={8}
              className="w-full resize-y rounded-xl border border-slate-200 bg-slate-50 px-3 py-2.5 text-sm leading-relaxed text-slate-900 outline-none ring-slate-300 focus:bg-white focus:ring-2"
              autoFocus
            />

            <div className="mt-3 flex flex-wrap items-center justify-end gap-2">
              <button
                type="button"
                onClick={closeMessageModal}
                className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm font-medium text-slate-700 hover:bg-slate-50"
              >
                Schließen
              </button>
              <button
                type="button"
                onClick={() => void copyModalText()}
                disabled={!messageModal.text.trim()}
                className="rounded-lg bg-slate-900 px-3 py-2 text-sm font-medium text-white hover:bg-slate-700 disabled:opacity-40"
              >
                {copiedSku === messageModal.sku ? "Kopiert" : "In Zwischenablage"}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

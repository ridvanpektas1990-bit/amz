"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";
import type { CartonSpecRow } from "@/lib/carton-specs";
import { isActiveListing } from "@/lib/listing-activity";
import {
  DEFAULT_AMAZON_TARGET_COVER_DAYS,
  DEFAULT_TRANSFER_LEAD_DAYS,
} from "@/lib/local-stock";
import ShowInactiveListingsToggle from "@/components/ShowInactiveListingsToggle";

type Draft = {
  unitsPerCarton: string;
  productionTimeDays: string;
  shippingTimeDays: string;
  bufferTimeDays: string;
  localQty: string;
  onOrderUnits: string;
  onOrderOrderedAt: string;
  transferLeadDays: string;
  amazonTargetCoverDays: string;
  cartonLenCm: string;
  cartonWCm: string;
  cartonHCm: string;
  cartonWeightKg: string;
};

const COLUMN_HELP = [
  {
    key: "Produkt",
    text: "Amazon-Bild, Produktname, ASIN und SKU. Dient nur der Orientierung.",
  },
  {
    key: "Stk/Karton",
    text: "Wie viele Verkaufseinheiten in einen Karton passen. Nachbestellmengen werden darauf aufgerundet (z. B. 52 → 64 bei 32/Karton).",
  },
  {
    key: "Produktionsdauer",
    text: "Produktionstage beim Supplier – von Bestellung bis Ware fertig ist.",
  },
  {
    key: "Lieferdauer",
    text: "Versanddauer nach Produktion bis Ankunft beim Amazon Lager.",
  },
  {
    key: "Gesamtdauer",
    text: "Lieferzeit = Produktionsdauer + Lieferdauer. Steuert nur Wann bestellen (Bestellfrist). Beispiel: 25 + 65 = 90 Tage.",
  },
  {
    key: "Pufferzeit",
    text:
      "Wie lange die neue Charge nach Ankunft reichen soll – zusätzlich zur Lieferzeit. " +
      "Beispiel: Lieferzeit 90 Tage, Puffer 60 → Charge für 150 Tage. " +
      "Die Nachbestellmenge ist der Vorjahresbedarf über genau diesen Zeitraum, ab dem Tag an dem die Ware voraussichtlich ankommt. " +
      "Leer oder 0 = nur Lieferzeit als Charge-Reichweite.",
  },
  {
    key: "Lokal",
    text:
      "Bestand im eigenen / 3PL-Lager (nicht Amazon). Wird bei neuem Amazon-Zulauf automatisch reduziert.",
  },
  {
    key: "Bestellt",
    text: "Offene Bestellung beim Lieferanten – noch nicht im lokalen Lager angekommen.",
  },
  {
    key: "Bestelldatum",
    text:
      "Datum der Lieferantenbestellung. Daraus schätzen wir Ankunft und mögliche Sales-Lücken, bis Ware bei Amazon ist.",
  },
  {
    key: "Transfer",
    text: "Tage vom lokalen Lager bis Amazon (Standard 7). Steuert Wann „Amazon nachfüllen“.",
  },
  {
    key: "Amz-Ziel",
    text:
      "Wie lange die Ware idealerweise im Amazon-Lager reichen soll (z. B. 30 oder 60 Tage). " +
      "Daraus berechnen wir später die empfohlene Menge zum Reinschicken. " +
      `Standard ${DEFAULT_AMAZON_TARGET_COVER_DAYS} Tage.`,
  },
  {
    key: "L / B / H",
    text: "Kartonmaße Länge, Breite, Höhe in cm (optional, für spätere Fracht/Volumen-Rechnungen).",
  },
  {
    key: "kg",
    text: "Kartongewicht in kg (optional).",
  },
] as const;

const PUFFER_HOVER =
  "Zusätzliche Reichweite der Charge nach Ankunft (Tage). " +
  "Nachbestellmenge = Vorjahresverkäufe über (Gesamtdauer + Puffer), beginnend am Ankunftszeitraum. " +
  "Bsp.: 90 Tage Lieferzeit + 60 Puffer = 150 Tage Charge. Leer/0 = nur Gesamtdauer.";

function HeaderHint({
  label,
  hint,
  wide,
}: {
  label: string;
  hint: string;
  wide?: boolean;
}) {
  return (
    <th className="group relative px-1.5 py-3 text-center align-middle" title={hint}>
      <span className="inline-block max-w-[7.5rem] cursor-help whitespace-normal border-b border-dotted border-slate-400/80 text-center leading-tight">
        {label}
      </span>
      <span
        className={`pointer-events-none absolute left-1/2 top-full z-20 mt-1 hidden -translate-x-1/2 rounded-lg border border-slate-200 bg-white p-2.5 text-left text-[11px] font-normal normal-case tracking-normal leading-snug text-slate-700 shadow-lg group-hover:block ${
          wide ? "w-72" : "w-56"
        }`}
      >
        {hint}
      </span>
    </th>
  );
}

function toDraft(row: CartonSpecRow): Draft {
  return {
    unitsPerCarton: row.unitsPerCarton != null ? String(row.unitsPerCarton) : "",
    productionTimeDays: row.productionTimeDays != null ? String(row.productionTimeDays) : "",
    shippingTimeDays: row.shippingTimeDays != null ? String(row.shippingTimeDays) : "",
    bufferTimeDays: row.bufferTimeDays != null ? String(row.bufferTimeDays) : "",
    localQty: row.localQty != null && row.localQty > 0 ? String(row.localQty) : row.localQty === 0 ? "0" : "",
    onOrderUnits:
      row.onOrderUnits != null && row.onOrderUnits > 0
        ? String(row.onOrderUnits)
        : row.onOrderUnits === 0
          ? "0"
          : "",
    onOrderOrderedAt: row.onOrderOrderedAt ? String(row.onOrderOrderedAt).slice(0, 10) : "",
    transferLeadDays: String(row.transferLeadDays ?? DEFAULT_TRANSFER_LEAD_DAYS),
    amazonTargetCoverDays: String(
      row.amazonTargetCoverDays ?? DEFAULT_AMAZON_TARGET_COVER_DAYS,
    ),
    cartonLenCm: row.cartonLenCm != null ? String(row.cartonLenCm) : "",
    cartonWCm: row.cartonWCm != null ? String(row.cartonWCm) : "",
    cartonHCm: row.cartonHCm != null ? String(row.cartonHCm) : "",
    cartonWeightKg: row.cartonWeightKg != null ? String(row.cartonWeightKg) : "",
  };
}

function emptyToNull(value: string): number | null {
  const trimmed = value.trim();
  if (!trimmed) return null;
  const n = Number(trimmed.replace(",", "."));
  return Number.isFinite(n) ? n : null;
}

const inputClass =
  "w-full rounded-lg border border-slate-300 bg-white px-1.5 py-1.5 text-center text-xs tabular-nums text-slate-900 outline-none transition placeholder:text-slate-400 focus:border-sky-500 focus:ring-2 focus:ring-sky-100";

export default function SkuStammdatenTable() {
  const [items, setItems] = useState<CartonSpecRow[]>([]);
  const [drafts, setDrafts] = useState<Record<string, Draft>>({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const [onlyMissing, setOnlyMissing] = useState(false);
  const [savingSku, setSavingSku] = useState<string | null>(null);
  const [savedSku, setSavedSku] = useState<string | null>(null);
  const [helpOpen, setHelpOpen] = useState(false);
  const [bulkTransfer, setBulkTransfer] = useState(String(DEFAULT_TRANSFER_LEAD_DAYS));
  const [bulkAmazonTarget, setBulkAmazonTarget] = useState(
    String(DEFAULT_AMAZON_TARGET_COVER_DAYS),
  );
  const [bulkProduction, setBulkProduction] = useState("30");
  const [bulkShipping, setBulkShipping] = useState("60");
  const [bulkBuffer, setBulkBuffer] = useState("");
  const [bulkBusy, setBulkBusy] = useState<"transfer" | "amazon_target" | "lead" | null>(null);
  const [bulkMessage, setBulkMessage] = useState<string | null>(null);
  const [showInactiveListings, setShowInactiveListings] = useState(false);
  const [salesBySku, setSalesBySku] = useState<
    Record<string, { units30: number; units90: number }>
  >({});

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [specsRes, skusRes] = await Promise.all([
        fetch("/api/inventory/carton-specs", { cache: "no-store" }),
        fetch("/api/metrics/skus", { cache: "no-store" }),
      ]);
      const json = await specsRes.json();
      if (!specsRes.ok || !json.ok) throw new Error(json.error || "Laden fehlgeschlagen");
      const rows = (json.items || []) as CartonSpecRow[];
      setItems(rows);
      const next: Record<string, Draft> = {};
      for (const row of rows) next[row.sellerSku] = toDraft(row);
      setDrafts(next);

      const skusJson = await skusRes.json().catch(() => null);
      if (skusRes.ok && skusJson?.ok && Array.isArray(skusJson.skus)) {
        const map: Record<string, { units30: number; units90: number }> = {};
        for (const row of skusJson.skus as Array<{
          value?: string;
          units30?: number;
          units90?: number;
        }>) {
          const sku = String(row.value || "").trim();
          if (!sku) continue;
          map[sku] = {
            units30: Math.max(0, Number(row.units30) || 0),
            units90: Math.max(0, Number(row.units90) || 0),
          };
        }
        setSalesBySku(map);
      } else {
        setSalesBySku({});
      }
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  useEffect(() => {
    try {
      if (window.localStorage.getItem("amz_show_inactive_listings") === "1") {
        setShowInactiveListings(true);
      }
    } catch {
      /* ignore */
    }
  }, []);

  function updateShowInactiveListings(next: boolean) {
    setShowInactiveListings(next);
    try {
      window.localStorage.setItem("amz_show_inactive_listings", next ? "1" : "0");
    } catch {
      /* ignore */
    }
  }

  const skuIsActive = useCallback(
    (item: CartonSpecRow) => {
      const sales = salesBySku[item.sellerSku];
      return isActiveListing({
        available: item.available,
        inbound: item.inbound,
        localQty: item.localQty,
        onOrderUnits: item.onOrderUnits,
        units30: sales?.units30,
        units90: sales?.units90,
      });
    },
    [salesBySku],
  );

  const visible = useMemo(() => {
    const term = search.trim().toLowerCase();
    return items.filter((item) => {
      if (!showInactiveListings && !skuIsActive(item)) return false;
      if (onlyMissing && item.hasSpec && item.productionTimeDays != null && item.shippingTimeDays != null) {
        return false;
      }
      if (!term) return true;
      return (
        item.sellerSku.toLowerCase().includes(term) ||
        (item.asin || "").toLowerCase().includes(term) ||
        (item.productName || "").toLowerCase().includes(term)
      );
    });
  }, [items, search, onlyMissing, showInactiveListings, skuIsActive]);

  function updateDraft(sku: string, key: keyof Draft, value: string) {
    setDrafts((prev) => ({
      ...prev,
      [sku]: { ...(prev[sku] || toDraft(items.find((i) => i.sellerSku === sku)!)), [key]: value },
    }));
    if (savedSku === sku) setSavedSku(null);
  }

  async function saveRow(sku: string) {
    const draft = drafts[sku];
    if (!draft) return;
    const unitsPerCarton = emptyToNull(draft.unitsPerCarton);
    if (unitsPerCarton === null || unitsPerCarton <= 0) {
      setError(`${sku}: Stück/Karton muss > 0 sein`);
      return;
    }
    setSavingSku(sku);
    setError(null);
    try {
      const response = await fetch("/api/inventory/carton-specs", {
        method: "PUT",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          sellerSku: sku,
          unitsPerCarton,
          productionTimeDays: emptyToNull(draft.productionTimeDays),
          shippingTimeDays: emptyToNull(draft.shippingTimeDays),
          bufferTimeDays: emptyToNull(draft.bufferTimeDays),
          localQty: emptyToNull(draft.localQty) ?? 0,
          onOrderUnits: emptyToNull(draft.onOrderUnits) ?? 0,
          onOrderOrderedAt: draft.onOrderOrderedAt.trim() || null,
          transferLeadDays:
            emptyToNull(draft.transferLeadDays) ?? DEFAULT_TRANSFER_LEAD_DAYS,
          amazonTargetCoverDays: Math.max(
            1,
            emptyToNull(draft.amazonTargetCoverDays) ?? DEFAULT_AMAZON_TARGET_COVER_DAYS,
          ),
          cartonLenCm: emptyToNull(draft.cartonLenCm),
          cartonWCm: emptyToNull(draft.cartonWCm),
          cartonHCm: emptyToNull(draft.cartonHCm),
          cartonWeightKg: emptyToNull(draft.cartonWeightKg),
        }),
      });
      const json = await response.json();
      if (!response.ok || !json.ok) throw new Error(json.error || "Speichern fehlgeschlagen");
      setItems((prev) =>
        prev.map((row) =>
          row.sellerSku === sku
            ? {
                ...row,
                ...json.item,
                asin: row.asin,
                productName: row.productName,
                imageUrl: row.imageUrl,
                available: row.available,
                inbound: row.inbound,
                localQty: json.item.localQty ?? row.localQty,
                onOrderUnits: json.item.onOrderUnits ?? row.onOrderUnits,
                onOrderOrderedAt: json.item.onOrderOrderedAt ?? row.onOrderOrderedAt,
                transferLeadDays: json.item.transferLeadDays ?? row.transferLeadDays,
                amazonTargetCoverDays:
                  json.item.amazonTargetCoverDays ?? row.amazonTargetCoverDays,
                hasSpec: true,
              }
            : row,
        ),
      );
      setSavedSku(sku);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSavingSku(null);
    }
  }

  async function applyBulk(kind: "transfer" | "amazon_target" | "lead") {
    const payload =
      kind === "transfer"
        ? { transferLeadDays: emptyToNull(bulkTransfer) }
        : kind === "amazon_target"
          ? {
              amazonTargetCoverDays: Math.max(
                1,
                emptyToNull(bulkAmazonTarget) ?? DEFAULT_AMAZON_TARGET_COVER_DAYS,
              ),
            }
          : {
              productionTimeDays: emptyToNull(bulkProduction),
              shippingTimeDays: emptyToNull(bulkShipping),
              ...(emptyToNull(bulkBuffer) != null
                ? { bufferTimeDays: emptyToNull(bulkBuffer) }
                : {}),
            };

    if (kind === "transfer") {
      if (payload.transferLeadDays == null) {
        setError("Transfer-Tage ungültig");
        return;
      }
    } else if (kind === "amazon_target") {
      if (
        (payload as { amazonTargetCoverDays?: number }).amazonTargetCoverDays == null ||
        (payload as { amazonTargetCoverDays: number }).amazonTargetCoverDays < 1
      ) {
        setError("Amazon-Zielreichweite ungültig (min. 1 Tag)");
        return;
      }
    } else if (
      (payload as { productionTimeDays: number | null }).productionTimeDays == null &&
      (payload as { shippingTimeDays: number | null }).shippingTimeDays == null
    ) {
      setError("Produktions- und/oder Lieferdauer angeben");
      return;
    }

    const count = showInactiveListings ? items.length : items.filter((item) => skuIsActive(item)).length;
    const summary =
      kind === "transfer"
        ? `Transfer lokal → Amazon = ${payload.transferLeadDays} Tage für ${count} SKUs${
            showInactiveListings ? "" : " (aktive)"
          }?`
        : kind === "amazon_target"
          ? `Amazon-Zielreichweite = ${
              (payload as { amazonTargetCoverDays: number }).amazonTargetCoverDays
            } Tage für ${count} SKUs${showInactiveListings ? "" : " (aktive)"}?`
          : `Lieferzeiten (Prod. ${
              (payload as { productionTimeDays: number | null }).productionTimeDays ?? "–"
            } / Versand ${
              (payload as { shippingTimeDays: number | null }).shippingTimeDays ?? "–"
            } Tage) für ${count} SKUs${showInactiveListings ? "" : " (aktive)"} setzen?`;

    if (
      !window.confirm(
        `${summary}\n\nBestehende Werte werden überschrieben. Einzelne SKUs kannst du danach manuell anpassen.`,
      )
    ) {
      return;
    }

    const sellerSkus = showInactiveListings
      ? undefined
      : items.filter((item) => skuIsActive(item)).map((item) => item.sellerSku);

    setBulkBusy(kind);
    setBulkMessage(null);
    setError(null);
    try {
      const response = await fetch("/api/inventory/carton-specs/bulk", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ ...payload, sellerSkus }),
      });
      const json = await response.json();
      if (!response.ok || !json.ok) throw new Error(json.error || "Bulk-Update fehlgeschlagen");
      setBulkMessage(
        kind === "transfer"
          ? `Transfer ${json.applied?.transferLeadDays ?? "–"} Tage auf ${json.skus ?? 0} SKUs übernommen.`
          : kind === "amazon_target"
            ? `Amazon-Ziel ${json.applied?.amazonTargetCoverDays ?? "–"} Tage auf ${
                json.skus ?? 0
              } SKUs übernommen.`
            : `Lieferzeiten auf ${json.skus ?? 0} SKUs übernommen (Prod. ${
                json.applied?.productionTimeDays ?? "–"
              } / Versand ${json.applied?.shippingTimeDays ?? "–"}).`,
      );
      await load();
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBulkBusy(null);
    }
  }

  const missingCount = items.filter(
    (item) =>
      skuIsActive(item) &&
      (!item.hasSpec || item.productionTimeDays == null || item.shippingTimeDays == null),
  ).length;
  const activeCount = items.filter((item) => skuIsActive(item)).length;
  const inactiveCount = items.length - activeCount;
  const completeCount = activeCount - missingCount;

  return (
    <div className="mx-auto w-full max-w-[1600px] px-3 py-5 md:px-5 md:py-8">
      <div className="mb-6 flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <p className="text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-600">Stammdaten</p>
          <h1 className="mt-1 text-2xl font-semibold tracking-tight text-slate-950">SKU-Stammdaten</h1>
          <p className="mt-1.5 max-w-2xl text-sm leading-relaxed text-slate-700">
            Lieferzeiten, lokaler Bestand, offene Lieferantenbestellung und Kartonmaße. Der{" "}
            <strong className="font-semibold text-slate-900">lokale Bestand</strong> sinkt automatisch,
            wenn neuer Amazon-Zulauf erscheint.
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <ShowInactiveListingsToggle
            checked={showInactiveListings}
            onChange={updateShowInactiveListings}
            activeCount={activeCount}
            inactiveCount={inactiveCount}
          />
          <div className="rounded-full bg-white px-3 py-1.5 text-xs font-medium tabular-nums text-slate-700 ring-1 ring-slate-300">
            <span className="text-emerald-700">{completeCount} komplett</span>
            <span className="mx-1.5 text-slate-300">·</span>
            <span className="text-amber-800">{missingCount} offen</span>
          </div>
          <Link
            href="/dashboard"
            className="rounded-full bg-slate-900 px-3.5 py-1.5 text-xs font-medium text-white hover:bg-slate-800"
          >
            Dashboard
          </Link>
        </div>
      </div>

      <div className="mb-4 overflow-hidden rounded-2xl border border-slate-300 bg-white shadow-sm">
        <button
          type="button"
          onClick={() => setHelpOpen((open) => !open)}
          className="flex w-full items-center justify-between gap-3 px-4 py-3 text-left hover:bg-slate-50"
          aria-expanded={helpOpen}
        >
          <span className="text-sm font-semibold text-slate-900">Spalten erklärt</span>
          <span className="text-xs font-medium text-slate-500">{helpOpen ? "Zuklappen" : "Aufklappen"}</span>
        </button>
        {helpOpen && (
          <div className="border-t border-slate-200 bg-slate-50 px-4 py-3">
            <ul className="grid gap-2 sm:grid-cols-2">
              {COLUMN_HELP.map((item) => (
                <li
                  key={item.key}
                  className={`rounded-xl border px-3 py-2 ${
                    item.key === "Pufferzeit"
                      ? "border-sky-300 bg-sky-50"
                      : "border-slate-200 bg-white"
                  }`}
                >
                  <div className="text-xs font-semibold text-slate-900">{item.key}</div>
                  <div className="mt-0.5 text-[12px] leading-snug text-slate-600">{item.text}</div>
                </li>
              ))}
            </ul>
            <p className="mt-3 text-[11px] leading-relaxed text-slate-500">
              Alle Zeiten in <strong className="font-semibold text-slate-700">Tagen</strong>.{" "}
              <strong className="font-semibold text-slate-700">Gesamtdauer</strong> = Produktion + Lieferung
              (Wann bestellen). <strong className="font-semibold text-slate-700">Pufferzeit</strong> = extra
              Reichweite der Charge. Beispiel: 90 Tage Lieferzeit + 60 Puffer → Menge für 150 Tage Vorjahresbedarf
              ab Ankunft.
            </p>
          </div>
        )}
      </div>

      <div className="mb-4 rounded-2xl border border-slate-300 bg-white p-4 shadow-sm">
        <div className="flex flex-wrap items-start justify-between gap-2">
          <div>
            <h2 className="text-sm font-semibold text-slate-950">Universelle Vorgaben</h2>
            <p className="mt-0.5 max-w-2xl text-xs leading-relaxed text-slate-600">
              Einmal setzen und auf alle SKUs übernehmen. Danach kannst du einzelne Produkte in der
              Tabelle manuell abweichend speichern.
            </p>
          </div>
          {bulkMessage && (
            <p className="rounded-lg bg-emerald-50 px-2.5 py-1.5 text-xs font-medium text-emerald-800 ring-1 ring-emerald-200">
              {bulkMessage}
            </p>
          )}
        </div>

        <div className="mt-3 grid gap-3 lg:grid-cols-3">
          <div className="rounded-xl border border-slate-200 bg-slate-50/80 p-3">
            <div className="text-xs font-semibold text-slate-900">Transfer lokal → Amazon</div>
            <p className="mt-0.5 text-[11px] leading-snug text-slate-500">
              Tage vom Eigen-/3PL-Lager bis FBA (Standard {DEFAULT_TRANSFER_LEAD_DAYS}). Wann
              nachfüllen.
            </p>
            <div className="mt-2.5 flex flex-wrap items-center gap-2">
              <label className="flex items-center gap-1.5 text-xs text-slate-700">
                <input
                  type="number"
                  min={0}
                  step={1}
                  value={bulkTransfer}
                  onChange={(event) => setBulkTransfer(event.target.value)}
                  className="w-20 rounded-lg border border-slate-300 bg-white px-2 py-1.5 text-center text-sm tabular-nums outline-none focus:border-sky-500 focus:ring-2 focus:ring-sky-100"
                />
                Tage
              </label>
              <button
                type="button"
                disabled={bulkBusy != null || items.length === 0}
                onClick={() => void applyBulk("transfer")}
                className="rounded-lg bg-slate-900 px-3 py-1.5 text-xs font-medium text-white hover:bg-slate-800 disabled:opacity-40"
              >
                {bulkBusy === "transfer" ? "Übernehme…" : "Auf alle anwenden"}
              </button>
            </div>
          </div>

          <div className="rounded-xl border border-sky-200 bg-sky-50/70 p-3">
            <div className="text-xs font-semibold text-slate-900">Amazon-Zielreichweite</div>
            <p className="mt-0.5 text-[11px] leading-snug text-slate-600">
              Wie lange die Ware im Amazon-Lager reichen soll (z.&nbsp;B. 30 oder 60 Tage). Basis für
              die Mengenempfehlung beim Reinschicken. Standard{" "}
              {DEFAULT_AMAZON_TARGET_COVER_DAYS}.
            </p>
            <div className="mt-2.5 flex flex-wrap items-center gap-2">
              <label className="flex items-center gap-1.5 text-xs text-slate-700">
                <input
                  type="number"
                  min={1}
                  step={1}
                  value={bulkAmazonTarget}
                  onChange={(event) => setBulkAmazonTarget(event.target.value)}
                  className="w-20 rounded-lg border border-slate-300 bg-white px-2 py-1.5 text-center text-sm tabular-nums outline-none focus:border-sky-500 focus:ring-2 focus:ring-sky-100"
                />
                Tage
              </label>
              <button
                type="button"
                disabled={bulkBusy != null || items.length === 0}
                onClick={() => void applyBulk("amazon_target")}
                className="rounded-lg bg-slate-900 px-3 py-1.5 text-xs font-medium text-white hover:bg-slate-800 disabled:opacity-40"
              >
                {bulkBusy === "amazon_target" ? "Übernehme…" : "Auf alle anwenden"}
              </button>
            </div>
          </div>

          <div className="rounded-xl border border-slate-200 bg-slate-50/80 p-3 lg:col-span-1">
            <div className="text-xs font-semibold text-slate-900">
              China-Produktion / externe Lieferung
            </div>
            <p className="mt-0.5 text-[11px] leading-snug text-slate-500">
              Produktionsdauer + Lieferdauer (= Gesamtleadzeit zum Lager). Puffer optional.
            </p>
            <div className="mt-2.5 flex flex-wrap items-end gap-2">
              <label className="flex flex-col gap-0.5 text-[10px] font-medium text-slate-600">
                Produktion
                <input
                  type="number"
                  min={0}
                  step={1}
                  value={bulkProduction}
                  onChange={(event) => setBulkProduction(event.target.value)}
                  className="w-20 rounded-lg border border-slate-300 bg-white px-2 py-1.5 text-center text-sm tabular-nums text-slate-900 outline-none focus:border-sky-500 focus:ring-2 focus:ring-sky-100"
                />
              </label>
              <label className="flex flex-col gap-0.5 text-[10px] font-medium text-slate-600">
                Lieferung
                <input
                  type="number"
                  min={0}
                  step={1}
                  value={bulkShipping}
                  onChange={(event) => setBulkShipping(event.target.value)}
                  className="w-20 rounded-lg border border-slate-300 bg-white px-2 py-1.5 text-center text-sm tabular-nums text-slate-900 outline-none focus:border-sky-500 focus:ring-2 focus:ring-sky-100"
                />
              </label>
              <label className="flex flex-col gap-0.5 text-[10px] font-medium text-slate-600">
                Puffer
                <input
                  type="number"
                  min={0}
                  step={1}
                  value={bulkBuffer}
                  onChange={(event) => setBulkBuffer(event.target.value)}
                  placeholder="opt."
                  className="w-20 rounded-lg border border-slate-300 bg-white px-2 py-1.5 text-center text-sm tabular-nums text-slate-900 outline-none placeholder:text-slate-400 focus:border-sky-500 focus:ring-2 focus:ring-sky-100"
                />
              </label>
              <button
                type="button"
                disabled={bulkBusy != null || items.length === 0}
                onClick={() => void applyBulk("lead")}
                className="rounded-lg bg-slate-900 px-3 py-1.5 text-xs font-medium text-white hover:bg-slate-800 disabled:opacity-40"
              >
                {bulkBusy === "lead" ? "Übernehme…" : "Auf alle anwenden"}
              </button>
            </div>
            <p className="mt-1.5 text-[11px] tabular-nums text-slate-500">
              Summe:{" "}
              {(emptyToNull(bulkProduction) || 0) + (emptyToNull(bulkShipping) || 0)} Tage Leadzeit
              {emptyToNull(bulkBuffer) != null
                ? ` + ${emptyToNull(bulkBuffer)} Puffer`
                : ""}
            </p>
          </div>
        </div>
      </div>

      <div className="mb-4 flex flex-wrap items-center gap-2 rounded-2xl border border-slate-300 bg-white p-2.5 shadow-sm">
        <input
          value={search}
          onChange={(event) => setSearch(event.target.value)}
          placeholder="Suchen: SKU, ASIN, Name…"
          className="min-w-[200px] flex-1 rounded-xl border border-slate-300 bg-slate-50 px-3 py-2 text-sm text-slate-900 outline-none focus:border-sky-500 focus:bg-white focus:ring-2 focus:ring-sky-100"
        />
        <label className="inline-flex cursor-pointer items-center gap-2 rounded-xl border border-slate-300 bg-slate-50 px-3 py-2 text-xs font-medium text-slate-800 hover:bg-white">
          <input
            type="checkbox"
            checked={onlyMissing}
            onChange={(event) => setOnlyMissing(event.target.checked)}
            className="rounded border-slate-400"
          />
          Nur unvollständig
        </label>
        <button
          type="button"
          onClick={() => void load()}
          className="rounded-xl border border-slate-300 bg-white px-3 py-2 text-xs font-medium text-slate-800 hover:bg-slate-50"
        >
          Neu laden
        </button>
      </div>

      {error && (
        <div className="mb-4 rounded-xl border border-red-300 bg-red-50 px-3 py-2 text-sm text-red-800">{error}</div>
      )}
      {loading && <div className="py-16 text-center text-sm text-slate-600">Lädt Produkte …</div>}

      {!loading && (
        <div className="overflow-hidden rounded-2xl border border-slate-300 bg-white shadow-sm">
          <table className="w-full table-fixed border-collapse text-left text-[13px]">
            <colgroup>
              <col className="w-[18%]" />
              <col className="w-[5.5%]" />
              <col className="w-[5.5%]" />
              <col className="w-[5.5%]" />
              <col className="w-[5.5%]" />
              <col className="w-[5.5%]" />
              <col className="w-[5%]" />
              <col className="w-[5%]" />
              <col className="w-[7%]" />
              <col className="w-[5%]" />
              <col className="w-[5%]" />
              <col className="w-[4%]" />
              <col className="w-[4%]" />
              <col className="w-[4%]" />
              <col className="w-[4%]" />
              <col className="w-[6.5%]" />
            </colgroup>
            <thead>
              <tr className="border-b border-slate-300 bg-slate-800 text-[10px] font-semibold uppercase tracking-[0.06em] text-slate-100">
                <th
                  className="px-3 py-3 text-left align-middle"
                  title="Amazon-Bild, Produktname, ASIN und SKU"
                >
                  <span className="cursor-help border-b border-dotted border-slate-400/80">Produkt</span>
                </th>
                <HeaderHint
                  label="Stk/Karton"
                  hint="Einheiten pro Karton. Nachbestellung wird darauf aufgerundet."
                />
                <HeaderHint
                  label="Produktionsdauer"
                  hint="Produktionstage beim Supplier (Bestellung → fertig)."
                />
                <HeaderHint
                  label="Lieferdauer"
                  hint="Versanddauer bis Ankunft beim lokalen Lager / Supplier-Lead."
                />
                <HeaderHint
                  label="Gesamtdauer"
                  hint="Lieferzeit = Produktionsdauer + Lieferdauer. Steuert den Bestellzeitpunkt beim Lieferanten."
                />
                <HeaderHint label="Pufferzeit" hint={PUFFER_HOVER} wide />
                <HeaderHint
                  label="Lokal"
                  hint="Bestand im eigenen / 3PL-Lager. Wird bei neuem Amazon-Zulauf automatisch reduziert."
                />
                <HeaderHint
                  label="Bestellt"
                  hint="Offene Bestellung beim Lieferanten (noch nicht lokal angekommen)."
                />
                <HeaderHint
                  label="Bestelldatum"
                  hint="Wann beim Lieferanten bestellt wurde – für ETA und Sales-Lücken-Hinweis."
                />
                <HeaderHint
                  label="Transfer"
                  hint="Tage lokales Lager → Amazon (Standard 7). Wann nachfüllen."
                />
                <HeaderHint
                  label="Amz-Ziel"
                  hint={`Gewünschte Amazon-Reichweite in Tagen (Standard ${DEFAULT_AMAZON_TARGET_COVER_DAYS}). Basis für die Mengenempfehlung beim Reinschicken.`}
                  wide
                />
                <HeaderHint label="L" hint="Kartonlänge in cm." />
                <HeaderHint label="B" hint="Kartonbreite in cm." />
                <HeaderHint label="H" hint="Kartonhöhe in cm." />
                <HeaderHint label="kg" hint="Kartongewicht in kg." />
                <th className="px-2 py-3 text-center align-middle" />
              </tr>
            </thead>
            <tbody>
              {visible.map((item, index) => {
                const draft = drafts[item.sellerSku] || toDraft(item);
                const leadDays =
                  (emptyToNull(draft.productionTimeDays) || 0) +
                  (emptyToNull(draft.shippingTimeDays) || 0);
                const bufferDays = emptyToNull(draft.bufferTimeDays) || 0;
                const chargeDays = leadDays > 0 ? leadDays + bufferDays : 0;
                const incomplete =
                  !draft.unitsPerCarton.trim() ||
                  !draft.productionTimeDays.trim() ||
                  !draft.shippingTimeDays.trim();
                const isSaved = savedSku === item.sellerSku;
                const isSaving = savingSku === item.sellerSku;
                return (
                  <tr
                    key={item.sellerSku}
                    className={`border-b border-slate-200 transition ${
                      incomplete
                        ? "bg-amber-100/90 hover:bg-amber-100"
                        : index % 2 === 0
                          ? "bg-white hover:bg-sky-50/60"
                          : "bg-slate-100 hover:bg-sky-50/60"
                    }`}
                  >
                    <td className="px-3 py-2.5">
                      <div className="flex min-w-0 items-center gap-2.5">
                        <div className="flex h-11 w-11 shrink-0 items-center justify-center overflow-hidden rounded-xl border border-slate-300 bg-white shadow-sm">
                          {item.imageUrl ? (
                            <img
                              src={item.imageUrl}
                              alt=""
                              className="h-full w-full object-contain p-1"
                              loading="lazy"
                              referrerPolicy="no-referrer"
                            />
                          ) : (
                            <span className="h-2.5 w-2.5 rounded-full bg-slate-400" />
                          )}
                        </div>
                        <div className="min-w-0">
                          <div className="truncate font-semibold leading-snug text-slate-950">
                            {item.productName || item.sellerSku}
                          </div>
                          <div className="mt-0.5 truncate font-mono text-[11px] text-slate-600">
                            {item.asin || "–"}
                            <span className="text-slate-400"> · </span>
                            {item.sellerSku}
                          </div>
                          {incomplete && (
                            <div className="mt-1 inline-flex rounded-md bg-amber-200 px-1.5 py-0.5 text-[10px] font-semibold text-amber-950">
                              Angaben fehlen
                            </div>
                          )}
                        </div>
                      </div>
                    </td>
                    {(
                      [
                        "unitsPerCarton",
                        "productionTimeDays",
                        "shippingTimeDays",
                      ] as const
                    ).map((key) => (
                      <td key={key} className="px-1.5 py-2 text-center">
                        <input
                          value={draft[key]}
                          onChange={(event) => updateDraft(item.sellerSku, key, event.target.value)}
                          className={`${inputClass} mx-auto max-w-[4.5rem]`}
                        />
                      </td>
                    ))}
                    <td className="px-1.5 py-2 text-center">
                      <span
                        className="inline-flex rounded-full bg-sky-100 px-2 py-1 text-xs font-bold tabular-nums text-sky-900 ring-1 ring-sky-200"
                        title="Gesamtdauer = Produktionsdauer + Lieferdauer (Tage)"
                      >
                        {leadDays > 0 ? `${leadDays} T` : "–"}
                      </span>
                    </td>
                    <td className="px-1.5 py-2 text-center">
                      <input
                        value={draft.bufferTimeDays}
                        onChange={(event) => updateDraft(item.sellerSku, "bufferTimeDays", event.target.value)}
                        placeholder="0"
                        className={`${inputClass} mx-auto max-w-[4.5rem]`}
                        title={PUFFER_HOVER}
                      />
                      {chargeDays > 0 && (
                        <div className="mt-1 text-[10px] tabular-nums text-slate-500" title="Charge-Reichweite">
                          → {chargeDays} T
                        </div>
                      )}
                    </td>
                    {(
                      ["localQty", "onOrderUnits"] as const
                    ).map((key) => (
                      <td key={key} className="px-1.5 py-2 text-center">
                        <input
                          value={draft[key]}
                          onChange={(event) => updateDraft(item.sellerSku, key, event.target.value)}
                          placeholder="0"
                          className={`${inputClass} mx-auto max-w-[4.5rem]`}
                        />
                      </td>
                    ))}
                    <td className="px-1 py-2 text-center">
                      <input
                        type="date"
                        value={draft.onOrderOrderedAt}
                        onChange={(event) =>
                          updateDraft(item.sellerSku, "onOrderOrderedAt", event.target.value)
                        }
                        className={`${inputClass} mx-auto max-w-[9.5rem] text-[11px]`}
                        title="Bestelldatum beim Lieferanten"
                      />
                    </td>
                    <td className="px-1.5 py-2 text-center">
                      <input
                        value={draft.transferLeadDays}
                        onChange={(event) =>
                          updateDraft(item.sellerSku, "transferLeadDays", event.target.value)
                        }
                        placeholder={String(DEFAULT_TRANSFER_LEAD_DAYS)}
                        className={`${inputClass} mx-auto max-w-[4.5rem]`}
                      />
                    </td>
                    <td className="px-1.5 py-2 text-center">
                      <input
                        value={draft.amazonTargetCoverDays}
                        onChange={(event) =>
                          updateDraft(item.sellerSku, "amazonTargetCoverDays", event.target.value)
                        }
                        placeholder={String(DEFAULT_AMAZON_TARGET_COVER_DAYS)}
                        className={`${inputClass} mx-auto max-w-[4.5rem]`}
                        title="Gewünschte Amazon-Reichweite (Tage)"
                      />
                    </td>
                    {(
                      ["cartonLenCm", "cartonWCm", "cartonHCm", "cartonWeightKg"] as const
                    ).map((key) => (
                      <td key={key} className="px-1 py-2 text-center">
                        <input
                          value={draft[key]}
                          onChange={(event) => updateDraft(item.sellerSku, key, event.target.value)}
                          className={`${inputClass} mx-auto max-w-[3.5rem]`}
                        />
                      </td>
                    ))}
                    <td className="px-2 py-2 text-center">
                      <button
                        type="button"
                        disabled={isSaving}
                        onClick={() => void saveRow(item.sellerSku)}
                        className={`rounded-lg px-2.5 py-1.5 text-xs font-semibold transition disabled:opacity-50 ${
                          isSaved
                            ? "bg-emerald-600 text-white ring-1 ring-emerald-700"
                            : "bg-slate-900 text-white hover:bg-slate-800"
                        }`}
                      >
                        {isSaving ? "…" : isSaved ? "OK" : "Speichern"}
                      </button>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
          {!visible.length && (
            <div className="px-4 py-12 text-center text-sm text-slate-600">Keine Treffer.</div>
          )}
          <div className="border-t border-slate-300 bg-slate-50 px-4 py-2.5 text-[11px] font-medium text-slate-600">
            {visible.length} von {items.length} · Charge = Gesamtdauer + Pufferzeit (Vorjahresbedarf ab Ankunft)
          </div>
        </div>
      )}
    </div>
  );
}

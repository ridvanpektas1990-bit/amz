"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";
import type { CartonSpecRow } from "@/lib/carton-specs";

type Draft = {
  unitsPerCarton: string;
  productionTimeDays: string;
  shippingTimeDays: string;
  bufferTimeDays: string;
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

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch("/api/inventory/carton-specs", { cache: "no-store" });
      const json = await response.json();
      if (!response.ok || !json.ok) throw new Error(json.error || "Laden fehlgeschlagen");
      const rows = (json.items || []) as CartonSpecRow[];
      setItems(rows);
      const next: Record<string, Draft> = {};
      for (const row of rows) next[row.sellerSku] = toDraft(row);
      setDrafts(next);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  const visible = useMemo(() => {
    const term = search.trim().toLowerCase();
    return items.filter((item) => {
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
  }, [items, search, onlyMissing]);

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

  const missingCount = items.filter(
    (item) => !item.hasSpec || item.productionTimeDays == null || item.shippingTimeDays == null,
  ).length;
  const completeCount = items.length - missingCount;

  return (
    <div className="mx-auto w-full max-w-[1600px] px-3 py-5 md:px-5 md:py-8">
      <div className="mb-6 flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <p className="text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-600">Stammdaten</p>
          <h1 className="mt-1 text-2xl font-semibold tracking-tight text-slate-950">SKU-Stammdaten</h1>
          <p className="mt-1.5 max-w-2xl text-sm leading-relaxed text-slate-700">
            Lieferzeiten, Puffer und Kartonmaße pro Produkt. Die{" "}
            <strong className="font-semibold text-slate-900">Pufferzeit</strong> legt fest, wie lange eine
            Nachbestellung nach Ankunft reichen soll – daraus berechnet das Dashboard die Bestellmenge.
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
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
              <col className="w-[24%]" />
              <col className="w-[7%]" />
              <col className="w-[8%]" />
              <col className="w-[8%]" />
              <col className="w-[8%]" />
              <col className="w-[8%]" />
              <col className="w-[5%]" />
              <col className="w-[5%]" />
              <col className="w-[5%]" />
              <col className="w-[5%]" />
              <col className="w-[9%]" />
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
                  hint="Versanddauer bis Ankunft beim Amazon Lager."
                />
                <HeaderHint
                  label="Gesamtdauer"
                  hint="Lieferzeit = Produktionsdauer + Lieferdauer. Steuert nur den Bestellzeitpunkt (Wann bestellen?), nicht die Menge."
                />
                <HeaderHint label="Pufferzeit" hint={PUFFER_HOVER} wide />
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

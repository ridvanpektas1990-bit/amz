"use client";

import Link from "next/link";

export default function NachbestellungPage() {
  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      <h1 className="text-xl font-semibold tracking-tight text-slate-950">Nachbestellung</h1>
      <p className="mt-2 text-sm text-slate-600">
        Hier kommt als Nächstes die Übersicht aller SKUs mit Bestelllücke, Karton-Aufrundung und
        1-Klick Supplier-Text. Bis dahin nutze das Dashboard bei gewähltem Produkt.
      </p>
      <div className="mt-4 flex gap-3 text-sm">
        <Link href="/dashboard" className="font-medium text-slate-900 underline">
          Zum Dashboard
        </Link>
        <Link href="/sku-stammdaten" className="font-medium text-slate-600 underline">
          SKU-Stammdaten pflegen
        </Link>
      </div>
    </div>
  );
}

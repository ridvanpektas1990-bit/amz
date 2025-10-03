"use client";
/* eslint-disable @typescript-eslint/no-explicit-any */

import { useEffect, useState } from "react";

export default function OrdersPage() {
  const [marketplaceId, setMarketplaceId] = useState("A1PA6795UKMFR9"); // DE
  const [createdAfter, setCreatedAfter] = useState("");
  const [orders, setOrders] = useState<any[]>([]);
  const [nextToken, setNextToken] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  // Details/Modal
  const [detailsOpen, setDetailsOpen] = useState(false);
  const [detailsOrderId, setDetailsOrderId] = useState<string | null>(null);
  const [detailItems, setDetailItems] = useState<any[] | null>(null);
  const [detailFees, setDetailFees] = useState<{ items: any[]; totalFee: number; currency: string } | null>(null);
  const [detailsLoading, setDetailsLoading] = useState(false);
  const [detailsErr, setDetailsErr] = useState<string | null>(null);

  function fmtDate(iso?: string) {
    if (!iso) return "—";
    try {
      const d = new Date(iso);
      return `${d.toLocaleDateString("de-DE")} ${d.toLocaleTimeString("de-DE", { hour: "2-digit", minute: "2-digit" })}`;
    } catch {
      return iso;
    }
  }

  async function openDetails(orderId: string) {
    setDetailsOpen(true);
    setDetailsOrderId(orderId);
    setDetailsLoading(true);
    setDetailsErr(null);
    setDetailItems(null);
    setDetailFees(null);
    try {
      // Items
      const ri = await fetch(`/api/amazon/order-items/${encodeURIComponent(orderId)}`, { cache: "no-store" });
      const ji = await ri.json();
      if (!ri.ok || !ji.ok) throw new Error(ji?.error || `items ${ri.status}`);

      // Fees
      const rf = await fetch(`/api/amazon/orders/${encodeURIComponent(orderId)}/fees`, { cache: "no-store" });
      const jf = await rf.json();
      if (!rf.ok || !jf.ok) throw new Error(jf?.error || `fees ${rf.status}`);

      setDetailItems(ji.items || []);
      setDetailFees({ items: jf.items || [], totalFee: jf.totalFee || 0, currency: jf.currency || "EUR" });
    } catch (e: any) {
      setDetailsErr(e.message);
    } finally {
      setDetailsLoading(false);
    }
  }

  async function load(next?: string | null) {
    setLoading(true);
    setErr(null);
    const p = new URLSearchParams();
    if (next) {
      p.set("nextToken", next);
    } else {
      p.set("marketplaceId", marketplaceId);
      if (createdAfter) p.set("createdAfter", new Date(createdAfter).toISOString());
    }
    const r = await fetch(`/api/amazon/orders?${p.toString()}`, { cache: "no-store" });
    const j = await r.json();
    if (!r.ok || !j.ok) {
      setErr(j?.error || `HTTP ${r.status}`);
      setLoading(false);
      return;
    }
    setOrders(next ? [...orders, ...j.orders] : j.orders);
    setNextToken(j.nextToken);
    setLoading(false);
  }

  useEffect(() => {
    load(null); // beim ersten Laden: letzte 30 Tage
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <main className="p-6">
      <h1 className="text-xl font-semibold mb-4">Orders (ohne PII)</h1>

      <div className="flex flex-wrap gap-3 items-end mb-4">
        <div>
          <label className="block text-sm">Marketplace</label>
          <select
            className="border rounded px-2 py-1"
            value={marketplaceId}
            onChange={(e) => setMarketplaceId(e.target.value)}
          >
            <option value="A1PA6795UKMFR9">DE</option>
            <option value="A1F83G8C2ARO7P">UK</option>
            <option value="A13V1IB3VIYZZH">FR</option>
            <option value="APJ6JRA9NG5V4">IT</option>
            <option value="A1RKKUPIHCS9HS">ES</option>
            <option value="A33AVAJ2PDY3EV">TR</option>
            <option value="A28R8C7NBKEWEA">IE</option>
            <option value="A1805IZSGTT6HS">NL</option>
            <option value="A2NODRKZP88ZB9">SE</option>
            <option value="A1C3SOZRARQ6R3">PL</option>
          </select>
        </div>

        <div>
          <label className="block text-sm">Created After (optional)</label>
          <input
            type="date"
            className="border rounded px-2 py-1"
            value={createdAfter}
            onChange={(e) => setCreatedAfter(e.target.value)}
          />
        </div>

        <button className="px-3 py-2 border rounded" onClick={() => load(null)} disabled={loading}>
          {loading ? "Lädt…" : "Neu laden"}
        </button>

        {nextToken && (
          <button className="px-3 py-2 border rounded" onClick={() => load(nextToken)} disabled={loading}>
            {loading ? "Lädt…" : "Mehr laden"}
          </button>
        )}
      </div>

      {err && <div className="text-red-600 mb-3">Fehler: {String(err)}</div>}

      <div className="text-sm">
        {orders.length === 0 && <div>Keine Bestellungen gefunden.</div>}
        {orders.length > 0 && (
          <table className="w-full border-collapse">
            <thead>
              <tr className="text-left border-b">
                <th className="py-1">OrderId</th>
                <th className="py-1">Status</th>
                <th className="py-1">PurchaseDate</th>
                <th className="py-1">Items Shipped/Unshipped</th>
                <th className="py-1">Total</th>
                <th className="py-1">Channel</th>
                <th className="py-1">Details</th>
              </tr>
            </thead>
            <tbody>
              {orders.map((o, i) => (
                <tr key={i} className="border-b">
                  <td className="py-1">{o.amazonOrderId}</td>
                  <td className="py-1">{o.orderStatus}</td>
                  <td className="py-1">{fmtDate(o.purchaseDate)}</td>
                  <td className="py-1">
                    {o.numberOfItemsShipped}/{o.numberOfItemsUnshipped}
                  </td>
                  <td className="py-1">
                    {o.orderTotal ? `${o.orderTotal.Amount} ${o.orderTotal.CurrencyCode}` : "—"}
                  </td>
                  <td className="py-1">{o.salesChannel ?? "—"}</td>
                  <td className="py-1">
                    <button
                      className="px-2 py-1 text-xs border rounded"
                      onClick={() => openDetails(o.amazonOrderId)}
                    >
                      Anzeigen
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      {/* Modal für Details */}
      {detailsOpen && (
        <div className="fixed inset-0 bg-black/30 flex items-center justify-center z-50">
          <div className="bg-white rounded-xl shadow-xl w-full max-w-4xl p-4">
            <div className="flex items-center justify-between mb-3">
              <h2 className="text-lg font-semibold">Bestell-Details {detailsOrderId}</h2>
              <button
                className="text-sm px-2 py-1 border rounded"
                onClick={() => {
                  setDetailsOpen(false);
                  setDetailItems(null);
                  setDetailFees(null);
                }}
              >
                ✕
              </button>
            </div>

            {detailsLoading && <div>Lädt…</div>}
            {detailsErr && <div className="text-red-600">Fehler: {detailsErr}</div>}

            {!detailsLoading && !detailsErr && (
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {/* Items */}
                <div>
                  <h3 className="font-medium mb-2">Order-Items</h3>
                  {!detailItems || detailItems.length === 0 ? (
                    <div className="text-sm text-gray-600">Keine Items gefunden.</div>
                  ) : (
                    <table className="w-full text-sm border-collapse">
                      <thead>
                        <tr className="border-b">
                          <th className="py-1">SKU</th>
                          <th className="py-1">ASIN</th>
                          <th className="py-1">Qty</th>
                          <th className="py-1">Title</th>
                        </tr>
                      </thead>
                      <tbody>
                        {detailItems.map((it: any, i: number) => (
                          <tr key={i} className="border-b">
                            <td className="py-1">{it.sellerSKU ?? "—"}</td>
                            <td className="py-1">{it.asin ?? "—"}</td>
                            <td className="py-1">{it.quantityOrdered ?? it.quantityShipped ?? 0}</td>
                            <td className="py-1">{it.title ?? "—"}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  )}
                </div>

                {/* Fees */}
                <div>
                  <h3 className="font-medium mb-2">Gebühren</h3>
                  {!detailFees || detailFees.items.length === 0 ? (
                    <div className="text-sm text-gray-600">
                      Keine Gebühren-Events (evtl. noch nicht verbucht).
                    </div>
                  ) : (
                    <>
                      <table className="w-full text-sm border-collapse">
                        <thead>
                          <tr className="border-b">
                            <th className="py-1">OrderItemID</th>
                            <th className="py-1">FeeType</th>
                            <th className="py-1">Amount</th>
                          </tr>
                        </thead>
                        <tbody>
                          {detailFees.items.flatMap((row: any, i: number) =>
                            row.fees.map((f: any, j: number) => (
                              <tr key={`${i}-${j}`} className="border-b">
                                <td className="py-1">{row.orderItemId || "—"}</td>
                                <td className="py-1">{f.type}</td>
                                <td className="py-1">
                                  {Number(f.amount).toFixed(2)} {f.currency}
                                </td>
                              </tr>
                            ))
                          )}
                        </tbody>
                      </table>
                      <div className="mt-2 text-sm font-medium">
                        Summe Fees: {Number(detailFees.totalFee).toFixed(2)} {detailFees.currency}
                      </div>
                    </>
                  )}
                </div>
              </div>
            )}
          </div>
        </div>
      )}
    </main>
  );
}

import { NextRequest, NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";
import { tenantIdFromRequest } from "@/lib/amazon-tenant-cookie";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

type InventoryRow = {
  marketplace: string | null;
  snapshot_date: string | null;
  asin: string | null;
  seller_sku: string | null;
  inventory_left: number | null;
  inventory_total: number | null;
  reserved_total: number | null;
  pending_customer_orders: number | null;
  inbound_total: number | null;
};

type SalesRow = {
  seller_sku: string | null;
  purchase_date_berlin: string | null;
  quantity: number | null;
};

function must(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing env ${name}`);
  return value;
}

function supabase() {
  return createClient(must("SUPABASE_URL"), must("SUPABASE_SERVICE_ROLE_KEY"), {
    auth: { persistSession: false },
  });
}

function isoDate(date: Date): string {
  return date.toISOString().slice(0, 10);
}

function berlinDate(): string {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: "Europe/Berlin",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(new Date());
}

function addDays(dateISO: string, days: number): string {
  const date = new Date(`${dateISO}T12:00:00Z`);
  date.setUTCDate(date.getUTCDate() + days);
  return isoDate(date);
}

function number(value: unknown): number {
  const parsed = Number(value ?? 0);
  return Number.isFinite(parsed) ? parsed : 0;
}

export async function GET(req: NextRequest) {
  try {
    const tenantId = tenantIdFromRequest(req);
    if (!tenantId) {
      return NextResponse.json({ ok: false, error: "not_connected" }, { status: 401 });
    }

    const marketplace = (new URL(req.url).searchParams.get("marketplace") || "DE").trim().toUpperCase();
    const sb = supabase();
    const today = berlinDate();
    const start30 = new Date(`${today}T00:00:00Z`);
    start30.setUTCDate(start30.getUTCDate() - 29);
    const start90 = new Date(`${today}T00:00:00Z`);
    start90.setUTCDate(start90.getUTCDate() - 89);

    const { data: inventoryData, error: inventoryError } = await sb
      .from("vw_inventory_latest_per_asin_max")
      .select(
        "marketplace,snapshot_date,asin,seller_sku,inventory_left,inventory_total,reserved_total,pending_customer_orders,inbound_total"
      )
      .eq("tenant_id", tenantId)
      .eq("marketplace", marketplace)
      .order("asin", { ascending: true });

    if (inventoryError) throw new Error(`Inventory: ${inventoryError.message}`);

    const inventorySkus = (inventoryData as InventoryRow[])
      .map((row) => String(row.seller_sku || "").trim())
      .filter(Boolean);

    const salesRows: SalesRow[] = [];
    const pageSize = 1000;
    if (inventorySkus.length) {
      for (let from = 0; ; from += pageSize) {
        const { data, error } = await sb
          .from("vw_amazon_fees_orders")
          .select("seller_sku,purchase_date_berlin,quantity")
          .eq("tenant_id", tenantId)
          .eq("marketplace", marketplace)
          .in("seller_sku", inventorySkus)
          .gte("purchase_date_berlin", isoDate(start90))
          .lte("purchase_date_berlin", today)
          .range(from, from + pageSize - 1);

        if (error) throw new Error(`Sales page ${from}: ${error.message}`);
        if (!data?.length) break;
        salesRows.push(...(data as SalesRow[]));
        if (data.length < pageSize) break;
      }
    }

    const salesBySku = new Map<string, { units30: number; units90: number }>();
    const start30ISO = isoDate(start30);
    for (const sale of salesRows) {
      const sku = String(sale.seller_sku || "").trim();
      const date = String(sale.purchase_date_berlin || "").slice(0, 10);
      if (!sku || !date) continue;
      const quantity = Math.max(0, number(sale.quantity));
      const aggregate = salesBySku.get(sku) || { units30: 0, units90: 0 };
      aggregate.units90 += quantity;
      if (date >= start30ISO) aggregate.units30 += quantity;
      salesBySku.set(sku, aggregate);
    }

    const items = (inventoryData as InventoryRow[])
      .filter((row) => row.asin && row.seller_sku)
      .map((row) => {
        const sku = String(row.seller_sku);
        const sales = salesBySku.get(sku) || { units30: 0, units90: 0 };
        const available = Math.max(0, number(row.inventory_left));
        const dailySales30 = sales.units30 / 30;
        const daysOfCover = dailySales30 > 0 ? Math.max(0, Math.ceil(available / dailySales30)) : null;

        let status: "out" | "critical" | "warning" | "healthy" | "no_sales";
        if (available <= 0) status = "out";
        else if (daysOfCover === null) status = "no_sales";
        else if (daysOfCover <= 30) status = "critical";
        else if (daysOfCover <= 60) status = "warning";
        else status = "healthy";

        return {
          asin: String(row.asin),
          sku,
          marketplace: row.marketplace || "DE",
          snapshotDate: row.snapshot_date,
          available,
          total: Math.max(0, number(row.inventory_total)),
          reserved: Math.max(0, number(row.reserved_total)),
          pendingCustomerOrders: Math.max(0, number(row.pending_customer_orders)),
          inbound: Math.max(0, number(row.inbound_total)),
          units30: sales.units30,
          units90: sales.units90,
          dailySales30: Number(dailySales30.toFixed(2)),
          daysOfCover,
          estimatedOosDate: daysOfCover === null ? null : addDays(today, daysOfCover),
          status,
        };
      });

    return NextResponse.json(
      {
        ok: true,
        generatedAt: new Date().toISOString(),
        marketplace,
        salesWindow: { days: 30, start: start30ISO, end: today },
        snapshotDate: items.map((item) => item.snapshotDate).filter(Boolean).sort().at(-1) || null,
        items,
      },
      { headers: { "cache-control": "no-store" } }
    );
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}

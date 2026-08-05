import { NextRequest, NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";
import { tenantIdFromRequest } from "@/lib/amazon-tenant-cookie";
import { loadCatalogMetadata, type CatalogMetadata } from "@/lib/amazon-catalog";
import { dailySalesAverage, isActiveListing, sortByDailySalesDesc } from "@/lib/listing-activity";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function must(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env ${name}`);
  return v;
}
function supa() {
  return createClient(
    must("SUPABASE_URL"),
    must("SUPABASE_SERVICE_ROLE_KEY"),
    { auth: { persistSession: false } }
  );
}

function berlinTodayISO(): string {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: "Europe/Berlin",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(new Date());
}

function addDaysISO(dateISO: string, days: number): string {
  const date = new Date(`${dateISO}T12:00:00Z`);
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
}

export async function GET(req: NextRequest) {
  try {
    const tenantId = tenantIdFromRequest(req);
    if (!tenantId) {
      return NextResponse.json({ ok: false, error: "not_connected" }, { status: 401 });
    }

    const sb = supa();
    const today = berlinTodayISO();
    const start30 = addDaysISO(today, -29);
    const start90 = addDaysISO(today, -89);

    const PAGE = 1000;
    type Agg = {
      rawSku: string;
      units30: number;
      units90: number;
    };
    const salesBySku = new Map<string, Agg>();

    for (let from = 0; ; from += PAGE) {
      const { data, error } = await sb
        .from("amazon_order_items")
        .select("seller_sku,purchase_date_berlin,quantity_ordered,quantity_shipped")
        .eq("tenant_id", tenantId)
        .gte("purchase_date_berlin", start90)
        .lte("purchase_date_berlin", today)
        .range(from, from + PAGE - 1);
      if (error) throw new Error(`SKU sales page ${from}: ${error.message}`);
      if (!data?.length) break;

      for (const row of data as Array<{
        seller_sku: string | null;
        purchase_date_berlin: string | null;
        quantity_ordered: number | null;
        quantity_shipped: number | null;
      }>) {
        const raw = String(row.seller_sku ?? "").trim();
        if (!raw) continue;
        const norm = raw.toUpperCase();
        const date = String(row.purchase_date_berlin || "").slice(0, 10);
        const qty = Math.max(
          Math.max(0, Number(row.quantity_ordered) || 0),
          Math.max(0, Number(row.quantity_shipped) || 0),
        );
        const agg = salesBySku.get(norm) || { rawSku: raw, units30: 0, units90: 0 };
        if (!salesBySku.has(norm)) agg.rawSku = raw;
        if (date >= start90 && date <= today) agg.units90 += qty;
        if (date >= start30 && date <= today) agg.units30 += qty;
        salesBySku.set(norm, agg);
      }

      if (data.length < PAGE) break;
    }

    // Also collect historical SKUs with no recent sales (inactive candidates).
    for (let from = 0; ; from += PAGE) {
      const { data, error } = await sb
        .from("amazon_order_items")
        .select("seller_sku")
        .eq("tenant_id", tenantId)
        .range(from, from + PAGE - 1);
      if (error) throw new Error(`SKU page ${from}: ${error.message}`);
      if (!data?.length) break;
      for (const row of data as Array<{ seller_sku: string | null }>) {
        const raw = String(row.seller_sku ?? "").trim();
        if (!raw) continue;
        const norm = raw.toUpperCase();
        if (!salesBySku.has(norm)) {
          salesBySku.set(norm, { rawSku: raw, units30: 0, units90: 0 });
        }
      }
      if (data.length < PAGE) break;
    }

    const { data: inventoryData, error: inventoryError } = await sb
      .from("vw_inventory_latest_per_asin_max")
      .select("seller_sku,asin,inventory_left,inbound_total")
      .eq("tenant_id", tenantId)
      .eq("marketplace", "DE");
    if (inventoryError) console.warn("SKU inventory metadata:", inventoryError.message);

    const inventoryBySku = new Map<
      string,
      { asin: string; available: number; inbound: number; imageUrl: string | null; productName: string | null }
    >();
    const inventoryRows = (inventoryData || []) as Array<{
      seller_sku: string | null;
      asin: string | null;
      inventory_left: number | null;
      inbound_total: number | null;
    }>;
    const asins = inventoryRows.map((row) => String(row.asin || "").trim()).filter(Boolean);
    let catalog = new Map<string, CatalogMetadata>();
    try {
      catalog = await loadCatalogMetadata(req, asins, "A1PA6795UKMFR9");
    } catch (error) {
      console.warn("SKU catalog metadata:", error instanceof Error ? error.message : String(error));
    }

    for (const row of inventoryRows) {
      const rawSku = String(row.seller_sku || "").trim();
      const asin = String(row.asin || "").trim();
      if (!rawSku) continue;
      const metadata = asin ? catalog.get(asin) : undefined;
      inventoryBySku.set(rawSku.toUpperCase(), {
        asin,
        available: Math.max(0, Number(row.inventory_left) || 0),
        inbound: Math.max(0, Number(row.inbound_total) || 0),
        imageUrl: metadata?.imageUrl || null,
        productName: metadata?.productName || null,
      });
      if (!salesBySku.has(rawSku.toUpperCase())) {
        salesBySku.set(rawSku.toUpperCase(), { rawSku, units30: 0, units90: 0 });
      }
    }

    const skus = sortByDailySalesDesc(
      Array.from(salesBySku.entries()).map(([norm, agg]) => {
        const inventory = inventoryBySku.get(norm);
        const units30 = agg.units30;
        const units90 = agg.units90;
        const available = inventory?.available ?? 0;
        const inbound = inventory?.inbound ?? 0;
        const active = isActiveListing({ available, inbound, units30, units90 });
        const daily = dailySalesAverage(units30);
        return {
          value: agg.rawSku,
          label: agg.rawSku.trim(),
          asin: inventory?.asin || null,
          imageUrl: inventory?.imageUrl || null,
          productName: inventory?.productName || null,
          units30,
          units90,
          dailySales30: Number(daily.toFixed(2)),
          available,
          inbound,
          active,
        };
      }),
    );

    return NextResponse.json({
      ok: true,
      skus,
      meta: {
        activeCount: skus.filter((sku) => sku.active).length,
        inactiveCount: skus.filter((sku) => !sku.active).length,
        sortedBy: "dailySales30_desc",
        inactiveRule: "available=0 AND inbound=0 AND units90=0",
      },
    });
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e);
    return NextResponse.json({ ok: false, error: msg }, { status: 500 });
  }
}

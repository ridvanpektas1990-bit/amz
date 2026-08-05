import { NextRequest, NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";
import { tenantIdFromRequest } from "@/lib/amazon-tenant-cookie";
import { loadCatalogMetadata, type CatalogMetadata } from "@/lib/amazon-catalog";

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

export async function GET(req: NextRequest) {
  try {
    const tenantId = tenantIdFromRequest(req);
    if (!tenantId) {
      return NextResponse.json({ ok: false, error: "not_connected" }, { status: 401 });
    }

    const sb = supa();

    const PAGE = 1000;
    let from = 0;
    let to = PAGE - 1;

    // Map: normalizedKey -> rawValue (erstes Vorkommen)
    const map = new Map<string, string>();

    for (;;) {
      const { data, error } = await sb
        .from("vw_amazon_fees_orders")
        .select("seller_sku")
        .eq("tenant_id", tenantId)
        .range(from, to);
      if (error) throw new Error(`SKU page ${from}-${to}: ${error.message}`);
      if (!data || data.length === 0) break;

      for (const row of data as any[]) {
        const raw: string = String(row.seller_sku ?? "");
        if (!raw) continue;
        const norm = raw.trim().toUpperCase();
        if (!map.has(norm)) map.set(norm, raw); // rohen Wert behalten!
      }

      if (data.length < PAGE) break;
      from += PAGE;
      to += PAGE;
    }

    const { data: inventoryData, error: inventoryError } = await sb
      .from("vw_inventory_latest_per_asin_max")
      .select("seller_sku,asin")
      .eq("tenant_id", tenantId)
      .eq("marketplace", "DE");
    if (inventoryError) console.warn("SKU inventory metadata:", inventoryError.message);

    const inventoryBySku = new Map<string, { asin: string; imageUrl: string | null; productName: string | null }>();
    const inventoryRows = (inventoryData || []) as Array<{ seller_sku: string | null; asin: string | null }>;
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
      if (!rawSku || !asin) continue;
      const metadata = catalog.get(asin);
      inventoryBySku.set(rawSku.toUpperCase(), {
        asin,
        imageUrl: metadata?.imageUrl || null,
        productName: metadata?.productName || null,
      });
    }

    const skus = Array.from(map.values())
      .map((value) => {
        const metadata = inventoryBySku.get(value.trim().toUpperCase());
        return {
          value,
          label: value.trim(),
          asin: metadata?.asin || null,
          imageUrl: metadata?.imageUrl || null,
          productName: metadata?.productName || null,
        };
      })
      .sort((a, b) => a.label.localeCompare(b.label, "en"));

    return NextResponse.json({ ok: true, skus });
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e);
    return NextResponse.json({ ok: false, error: msg }, { status: 500 });
  }
}

import { NextRequest, NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";
import { tenantIdFromRequest } from "@/lib/amazon-tenant-cookie";
import { berlinTodayISO, buildSyncStatus } from "@/lib/sync-status";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

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

export async function GET(req: NextRequest) {
  try {
    const tenantId = tenantIdFromRequest(req);
    if (!tenantId) {
      return NextResponse.json({ ok: false, error: "not_connected" }, { status: 401 });
    }

    const marketplace = (new URL(req.url).searchParams.get("marketplace") || "DE").trim().toUpperCase();
    const sb = supabase();
    const todayISO = berlinTodayISO();

    const [ordersRes, itemsRes, inventoryRes, etlRes] = await Promise.all([
      sb
        .from("amazon_orders")
        .select("purchase_date_local")
        .eq("tenant_id", tenantId)
        .eq("marketplace", marketplace)
        .order("purchase_date_local", { ascending: false })
        .limit(1)
        .maybeSingle(),
      sb
        .from("amazon_order_items")
        .select("purchase_date_berlin")
        .eq("tenant_id", tenantId)
        .eq("marketplace", marketplace)
        .order("purchase_date_berlin", { ascending: false })
        .limit(1)
        .maybeSingle(),
      sb
        .from("vw_inventory_latest_per_asin_max")
        .select("snapshot_date")
        .eq("tenant_id", tenantId)
        .eq("marketplace", marketplace)
        .order("snapshot_date", { ascending: false })
        .limit(1)
        .maybeSingle(),
      sb
        .from("etl_runs")
        .select("status,started_at,finished_at,marketplace,period_year,period_month")
        .eq("tenant_id", tenantId)
        .eq("marketplace", marketplace)
        .order("started_at", { ascending: false })
        .limit(1)
        .maybeSingle(),
    ]);

    if (ordersRes.error) throw new Error(`Orders freshness: ${ordersRes.error.message}`);
    if (itemsRes.error && !/amazon_order_items|schema cache|does not exist/i.test(itemsRes.error.message)) {
      throw new Error(`Order items freshness: ${itemsRes.error.message}`);
    }
    if (inventoryRes.error) throw new Error(`Inventory freshness: ${inventoryRes.error.message}`);
    if (etlRes.error) throw new Error(`ETL status: ${etlRes.error.message}`);

    const maxOrderLocal = ordersRes.data?.purchase_date_local
      ? String(ordersRes.data.purchase_date_local)
      : null;
    // purchase_date_local is timestamptz; expose Berlin calendar date for age checks.
    const maxOrderDate = maxOrderLocal
      ? new Intl.DateTimeFormat("en-CA", {
          timeZone: "Europe/Berlin",
          year: "numeric",
          month: "2-digit",
          day: "2-digit",
        }).format(new Date(maxOrderLocal))
      : null;

    const snapshot = buildSyncStatus({
      todayISO,
      maxOrderDate,
      maxOrderItemDate: itemsRes.data?.purchase_date_berlin
        ? String(itemsRes.data.purchase_date_berlin).slice(0, 10)
        : null,
      maxInventorySnapshot: inventoryRes.data?.snapshot_date
        ? String(inventoryRes.data.snapshot_date).slice(0, 10)
        : null,
      lastEtl: etlRes.data
        ? {
            status: etlRes.data.status ?? null,
            startedAt: etlRes.data.started_at ?? null,
            finishedAt: etlRes.data.finished_at ?? null,
            marketplace: etlRes.data.marketplace ?? null,
            periodYear: etlRes.data.period_year ?? null,
            periodMonth: etlRes.data.period_month ?? null,
          }
        : null,
    });

    return NextResponse.json(
      {
        ok: true,
        marketplace,
        todayISO,
        ...snapshot,
      },
      { headers: { "Cache-Control": "no-store" } },
    );
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e);
    return NextResponse.json({ ok: false, error: msg }, { status: 500 });
  }
}

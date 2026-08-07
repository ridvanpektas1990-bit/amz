import { NextRequest, NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";
import { tenantIdFromRequest } from "@/lib/amazon-tenant-cookie";
import {
  berlinDateISO,
  buildPipelineSeries,
  lastNBerlinDays,
  type EtlRunRow,
} from "@/lib/monitoring";
import { berlinTodayISO, buildSyncStatus } from "@/lib/sync-status";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const LOOKBACK_DAYS = 14;

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
    const dayKeys = lastNBerlinDays(todayISO, LOOKBACK_DAYS);
    const windowStart = `${dayKeys[0]}T00:00:00.000Z`;

    const [ordersRes, itemsRes, inventoryLatestRes, inventoryDaysRes, etlRes, orderDaysRes, itemDaysRes] =
      await Promise.all([
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
        .from("amazon_inventory_daily")
        .select("snapshot_date")
        .eq("tenant_id", tenantId)
        .eq("marketplace", marketplace)
        .gte("snapshot_date", dayKeys[0])
        .order("snapshot_date", { ascending: false })
        .limit(500),
      sb
        .from("etl_runs")
        .select("status,started_at,finished_at,marketplace,period_year,period_month,run_log")
        .eq("tenant_id", tenantId)
        .eq("marketplace", marketplace)
        .gte("started_at", windowStart)
        .order("started_at", { ascending: false })
        .limit(200),
      sb
        .from("amazon_orders")
        .select("purchase_date_local")
        .eq("tenant_id", tenantId)
        .eq("marketplace", marketplace)
        .gte("purchase_date_local", windowStart)
        .order("purchase_date_local", { ascending: false })
        .limit(2000),
      sb
        .from("amazon_order_items")
        .select("purchase_date_berlin")
        .eq("tenant_id", tenantId)
        .eq("marketplace", marketplace)
        .gte("purchase_date_berlin", dayKeys[0])
        .order("purchase_date_berlin", { ascending: false })
        .limit(2000),
    ]);

    if (ordersRes.error) throw new Error(`Orders: ${ordersRes.error.message}`);
    if (itemsRes.error && !/amazon_order_items|schema cache|does not exist/i.test(itemsRes.error.message)) {
      throw new Error(`Order items: ${itemsRes.error.message}`);
    }
    if (inventoryLatestRes.error) throw new Error(`Inventory: ${inventoryLatestRes.error.message}`);
    // Inventory daily history is optional if table name differs.
    const inventoryDaysError = inventoryDaysRes.error?.message || "";
    if (
      inventoryDaysRes.error &&
      !/amazon_inventory_daily|schema cache|does not exist|permission/i.test(inventoryDaysError)
    ) {
      throw new Error(`Inventory days: ${inventoryDaysError}`);
    }
    if (etlRes.error) throw new Error(`ETL runs: ${etlRes.error.message}`);
    if (orderDaysRes.error) throw new Error(`Order days: ${orderDaysRes.error.message}`);
    if (
      itemDaysRes.error &&
      !/amazon_order_items|schema cache|does not exist/i.test(itemDaysRes.error.message)
    ) {
      throw new Error(`Order item days: ${itemDaysRes.error.message}`);
    }

    const maxOrderLocal = ordersRes.data?.purchase_date_local
      ? String(ordersRes.data.purchase_date_local)
      : null;
    const maxOrderDate = maxOrderLocal
      ? new Intl.DateTimeFormat("en-CA", {
          timeZone: "Europe/Berlin",
          year: "numeric",
          month: "2-digit",
          day: "2-digit",
        }).format(new Date(maxOrderLocal))
      : null;

    const snapshotDates = new Set<string>();
    for (const row of inventoryDaysRes.data || []) {
      const day = berlinDateISO(row.snapshot_date) || String(row.snapshot_date || "").slice(0, 10);
      if (day) snapshotDates.add(day);
    }
    const latestSnap = inventoryLatestRes.data?.snapshot_date
      ? String(inventoryLatestRes.data.snapshot_date).slice(0, 10)
      : null;
    if (latestSnap) snapshotDates.add(latestSnap);

    const orderDataDates = new Set<string>();
    for (const row of orderDaysRes.data || []) {
      const day = berlinDateISO(row.purchase_date_local);
      if (day) orderDataDates.add(day);
    }
    if (maxOrderDate) orderDataDates.add(maxOrderDate);

    const orderItemDataDates = new Set<string>();
    for (const row of itemDaysRes.data || []) {
      const day =
        berlinDateISO(row.purchase_date_berlin) ||
        String(row.purchase_date_berlin || "").slice(0, 10);
      if (day) orderItemDataDates.add(day);
    }
    const maxItemDate = itemsRes.data?.purchase_date_berlin
      ? String(itemsRes.data.purchase_date_berlin).slice(0, 10)
      : null;
    if (maxItemDate) orderItemDataDates.add(maxItemDate);

    const runs = (etlRes.data || []) as EtlRunRow[];
    const pipelines = buildPipelineSeries({
      dayKeys,
      runs,
      inventorySnapshotDates: [...snapshotDates],
      orderDataDates: [...orderDataDates],
      orderItemDataDates: [...orderItemDataDates],
    });

    const freshness = buildSyncStatus({
      todayISO,
      maxOrderDate,
      maxOrderItemDate: itemsRes.data?.purchase_date_berlin
        ? String(itemsRes.data.purchase_date_berlin).slice(0, 10)
        : null,
      maxInventorySnapshot: latestSnap,
      lastEtl: runs[0]
        ? {
            status: runs[0].status ?? null,
            startedAt: runs[0].started_at ?? null,
            finishedAt: runs[0].finished_at ?? null,
            marketplace: runs[0].marketplace ?? null,
            periodYear: runs[0].period_year ?? null,
            periodMonth: runs[0].period_month ?? null,
          }
        : null,
    });

    return NextResponse.json(
      {
        ok: true,
        marketplace,
        todayISO,
        dayKeys,
        pipelines,
        freshness,
        recentRuns: runs.slice(0, 40).map((run) => ({
          status: run.status,
          startedAt: run.started_at,
          finishedAt: run.finished_at,
          marketplace: run.marketplace,
          periodYear: run.period_year,
          periodMonth: run.period_month,
          runLog: run.run_log,
          day: berlinDateISO(run.finished_at || run.started_at),
        })),
      },
      { headers: { "Cache-Control": "no-store" } },
    );
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e);
    return NextResponse.json({ ok: false, error: msg }, { status: 500 });
  }
}

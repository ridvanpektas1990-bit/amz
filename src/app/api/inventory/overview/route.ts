import { NextRequest, NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";
import { tenantIdFromRequest } from "@/lib/amazon-tenant-cookie";
import { loadCatalogMetadata, type CatalogMetadata } from "@/lib/amazon-catalog";
import { calculatePositiveGrowthFactor, chooseForecastDemand } from "@/lib/inventory-forecast";

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

function monthKey(dateISO: string): string {
  return dateISO.slice(0, 7);
}

function daysInMonth(year: number, monthIndex: number): number {
  return new Date(Date.UTC(year, monthIndex + 1, 0)).getUTCDate();
}

function previousYearDate(date: Date): Date {
  const previous = new Date(date);
  previous.setUTCFullYear(previous.getUTCFullYear() - 1);
  return previous;
}

function isCompleteMonth(year: number, monthIndex: number, todayISO: string): boolean {
  const end = new Date(Date.UTC(year, monthIndex + 1, 0));
  return isoDate(end) < todayISO;
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
    const todayDate = new Date(`${today}T12:00:00Z`);
    const currentYear = todayDate.getUTCFullYear();
    const start30 = new Date(`${today}T00:00:00Z`);
    start30.setUTCDate(start30.getUTCDate() - 29);
    const start90 = new Date(`${today}T00:00:00Z`);
    start90.setUTCDate(start90.getUTCDate() - 89);
    const historyStart = `${currentYear - 1}-01-01`;
    const previousCompleteMonth = todayDate.getUTCMonth() - 1;
    const comparisonAvailable = previousCompleteMonth >= 0;
    const currentComparisonStart = `${currentYear}-01-01`;
    const currentComparisonEnd = comparisonAvailable
      ? isoDate(new Date(Date.UTC(currentYear, previousCompleteMonth + 1, 0)))
      : null;
    const previousComparisonStart = `${currentYear - 1}-01-01`;
    const previousComparisonEnd = comparisonAvailable
      ? isoDate(new Date(Date.UTC(currentYear - 1, previousCompleteMonth + 1, 0)))
      : null;

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
    const inventoryAsins = (inventoryData as InventoryRow[])
      .map((row) => String(row.asin || "").trim())
      .filter(Boolean);
    const catalogPromise = loadCatalogMetadata(req, inventoryAsins, "A1PA6795UKMFR9")
      .catch((error) => {
        console.warn("Catalog metadata unavailable:", error instanceof Error ? error.message : String(error));
        return new Map<string, CatalogMetadata>();
      });

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
          .gte("purchase_date_berlin", historyStart)
          .lte("purchase_date_berlin", today)
          .range(from, from + pageSize - 1);

        if (error) throw new Error(`Sales page ${from}: ${error.message}`);
        if (!data?.length) break;
        salesRows.push(...(data as SalesRow[]));
        if (data.length < pageSize) break;
      }
    }

    type SalesAggregate = {
      units30: number;
      units90: number;
      currentComparable: number;
      previousComparable: number;
      byMonth: Map<string, number>;
    };
    const salesBySku = new Map<string, SalesAggregate>();
    const start30ISO = isoDate(start30);
    const start90ISO = isoDate(start90);
    for (const sale of salesRows) {
      const sku = String(sale.seller_sku || "").trim();
      const date = String(sale.purchase_date_berlin || "").slice(0, 10);
      if (!sku || !date) continue;
      const quantity = Math.max(0, number(sale.quantity));
      const aggregate = salesBySku.get(sku) || {
        units30: 0,
        units90: 0,
        currentComparable: 0,
        previousComparable: 0,
        byMonth: new Map<string, number>(),
      };
      if (date >= start90ISO) aggregate.units90 += quantity;
      if (date >= start30ISO) aggregate.units30 += quantity;
      if (currentComparisonEnd && date >= currentComparisonStart && date <= currentComparisonEnd) {
        aggregate.currentComparable += quantity;
      }
      if (previousComparisonEnd && date >= previousComparisonStart && date <= previousComparisonEnd) {
        aggregate.previousComparable += quantity;
      }
      const key = monthKey(date);
      aggregate.byMonth.set(key, (aggregate.byMonth.get(key) || 0) + quantity);
      salesBySku.set(sku, aggregate);
    }

    const catalogByAsin = await catalogPromise;
    const items = (inventoryData as InventoryRow[])
      .filter((row) => row.asin && row.seller_sku)
      .map((row) => {
        const sku = String(row.seller_sku);
        const asin = String(row.asin);
        const catalog = catalogByAsin.get(asin);
        const sales = salesBySku.get(sku) || {
          units30: 0,
          units90: 0,
          currentComparable: 0,
          previousComparable: 0,
          byMonth: new Map<string, number>(),
        };
        const available = Math.max(0, number(row.inventory_left));
        const dailySales30 = sales.units30 / 30;
        // Stabilisiert kleine Vorjahresbasen, ohne Wachstum großer Listings nennenswert zu verwässern.
        const growthFactor = calculatePositiveGrowthFactor(
          sales.currentComparable,
          sales.previousComparable,
        );
        const growthPercent = Math.round((growthFactor - 1) * 1000) / 10;

        const forecastDemandForDate = (forecastDate: Date) => {
          const sourceDate = previousYearDate(forecastDate);
          const sourceYear = sourceDate.getUTCFullYear();
          const sourceMonth = sourceDate.getUTCMonth();
          const sourceKey = `${sourceYear}-${String(sourceMonth + 1).padStart(2, "0")}`;
          const seasonalUnits = sales.byMonth.get(sourceKey) || 0;
          const canUseSeason = isCompleteMonth(sourceYear, sourceMonth, today);
          const seasonalRate = canUseSeason && seasonalUnits > 0
            ? seasonalUnits / daysInMonth(sourceYear, sourceMonth)
            : 0;
          const forecast = chooseForecastDemand({
            seasonalDemand: seasonalRate,
            recentDemand: dailySales30,
            growthFactor,
          });
          return {
            demand: forecast.demand,
            seasonal: forecast.source === "seasonal",
          };
        };

        let remaining = available;
        let daysOfCover: number | null = available <= 0 ? 0 : null;
        let seasonalDays = 0;
        let fallbackDays = 0;
        let hasDemand = false;
        const currentForecast = forecastDemandForDate(todayDate);
        let forecastDailySales = currentForecast.demand;

        if (available <= 0 && currentForecast.demand > 0) {
          hasDemand = true;
          if (currentForecast.seasonal) seasonalDays = 1;
          else fallbackDays = 1;
        }

        for (let day = 0; day < 730 && remaining > 0; day++) {
          const forecastDate = new Date(todayDate);
          forecastDate.setUTCDate(forecastDate.getUTCDate() + day);
          const forecast = forecastDemandForDate(forecastDate);
          const dailyDemand = forecast.demand;

          if (dailyDemand <= 0) continue;
          hasDemand = true;
          if (forecast.seasonal) seasonalDays += 1;
          else fallbackDays += 1;
          remaining -= dailyDemand;
          if (remaining <= 0) daysOfCover = day + 1;
        }

        let forecastMethod: "seasonal" | "hybrid" | "recent" | "none" = "none";
        if (seasonalDays > 0 && fallbackDays > 0) forecastMethod = "hybrid";
        else if (seasonalDays > 0) forecastMethod = "seasonal";
        else if (fallbackDays > 0) forecastMethod = "recent";
        if (!hasDemand && available > 0) daysOfCover = null;

        let status: "out" | "critical" | "warning" | "healthy" | "no_sales";
        if (available <= 0) status = "out";
        else if (daysOfCover === null) status = "no_sales";
        else if (daysOfCover <= 30) status = "critical";
        else if (daysOfCover <= 60) status = "warning";
        else status = "healthy";

        return {
          asin,
          sku,
          imageUrl: catalog?.imageUrl || null,
          productName: catalog?.productName || null,
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
          forecastDailySales: Number(forecastDailySales.toFixed(2)),
          forecastMethod,
          growthFactor: Number(growthFactor.toFixed(3)),
          growthPercent,
          comparisonCurrentUnits: sales.currentComparable,
          comparisonPreviousUnits: sales.previousComparable,
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
        seasonalHistory: { start: historyStart, end: today },
        growthComparison: {
          current: { start: currentComparisonStart, end: currentComparisonEnd },
          previous: { start: previousComparisonStart, end: previousComparisonEnd },
          positiveGrowthOnly: true,
          stabilizationUnits: 100,
        },
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

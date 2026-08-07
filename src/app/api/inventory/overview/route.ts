import { NextRequest, NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";
import { tenantIdFromRequest } from "@/lib/amazon-tenant-cookie";
import { loadCatalogMetadata, type CatalogMetadata } from "@/lib/amazon-catalog";
import {
  FORECAST_GROWTH_PRIOR_UNITS,
  calculatePositiveGrowthFactor,
  chooseForecastDemand,
  projectDailyOos,
} from "@/lib/inventory-forecast";
import {
  classifyStockStatus,
  effectiveInventoryUnits,
} from "@/lib/inventory-overview";
import { loadSkuSalesLinkedToOrders } from "@/lib/amazon-order-units";
import {
  dailyUnitsSeriesFromMap,
  recentSalesTempoFromDaily,
  RECENT_TEMPO_LOOKBACK_DAYS,
} from "@/lib/recent-sales-tempo";
import { DEFAULT_TRANSFER_LEAD_DAYS } from "@/lib/local-stock";
import { applyInboundLocalDeductions, loadLocalStockBySku } from "@/lib/local-stock-db";

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
    const startTempo = new Date(`${today}T00:00:00Z`);
    startTempo.setUTCDate(startTempo.getUTCDate() - (RECENT_TEMPO_LOOKBACK_DAYS - 1));
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

    const inboundBySku = new Map<string, number>();
    for (const row of (inventoryData as InventoryRow[]) || []) {
      const sku = String(row.seller_sku || "").trim();
      if (!sku) continue;
      inboundBySku.set(sku, Math.max(0, number(row.inbound_total)));
    }
    await applyInboundLocalDeductions(sb, tenantId, inboundBySku).catch((error) => {
      console.warn(
        "Local stock inbound deduct skipped:",
        error instanceof Error ? error.message : String(error),
      );
    });

    const inventorySkus = (inventoryData as InventoryRow[])
      .map((row) => String(row.seller_sku || "").trim())
      .filter(Boolean);
    const localBySku = await loadLocalStockBySku(sb, tenantId, inventorySkus).catch((error) => {
      console.warn(
        "Local stock load skipped:",
        error instanceof Error ? error.message : String(error),
      );
      return new Map();
    });

    const leadBySku = new Map<string, number>();
    if (inventorySkus.length) {
      const { data: leadRows } = await sb
        .from("inventory_carton_specs")
        .select("seller_sku,production_time_days,shipping_time_days")
        .eq("tenant_id", tenantId)
        .in("seller_sku", inventorySkus);
      for (const row of leadRows || []) {
        const lead =
          Math.max(0, Number((row as { production_time_days: number | null }).production_time_days) || 0) +
          Math.max(0, Number((row as { shipping_time_days: number | null }).shipping_time_days) || 0);
        if (lead > 0) leadBySku.set(String((row as { seller_sku: string }).seller_sku), lead);
      }
    }

    const inventoryAsins = (inventoryData as InventoryRow[])
      .map((row) => String(row.asin || "").trim())
      .filter(Boolean);
    const catalogPromise = loadCatalogMetadata(req, inventoryAsins, "A1PA6795UKMFR9")
      .catch((error) => {
        console.warn("Catalog metadata unavailable:", error instanceof Error ? error.message : String(error));
        return new Map<string, CatalogMetadata>();
      });

    const salesRows: SalesRow[] = [];
    if (inventorySkus.length) {
      const linked = await loadSkuSalesLinkedToOrders({
        sb,
        tenantId,
        marketplace,
        startISO: historyStart,
        endISO: today,
        sellerSkus: inventorySkus,
      });
      for (const row of linked) {
        salesRows.push({
          seller_sku: row.sellerSku,
          purchase_date_berlin: row.dateISO,
          quantity: row.quantity,
        });
      }
    }

    type SalesAggregate = {
      units30: number;
      units90: number;
      currentComparable: number;
      previousComparable: number;
      byMonth: Map<string, number>;
      byDate30: Map<string, number>;
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
        byDate30: new Map<string, number>(),
      };
      if (date >= start90ISO) aggregate.units90 += quantity;
      if (date >= start30ISO) {
        aggregate.units30 += quantity;
        aggregate.byDate30.set(date, (aggregate.byDate30.get(date) || 0) + quantity);
      }
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
          byDate30: new Map<string, number>(),
        };
        const available = Math.max(0, number(row.inventory_left));
        const inbound = Math.max(0, number(row.inbound_total));
        const local = localBySku.get(sku);
        const localQty = local?.localQty ?? 0;
        const onOrderUnits = local?.onOrderUnits ?? 0;
        const transferLeadDays = local?.transferLeadDays ?? DEFAULT_TRANSFER_LEAD_DAYS;
        const onOrderOrderedAt = local?.onOrderOrderedAt ?? null;
        const effectiveUnits = effectiveInventoryUnits(available, inbound);
        const recentTempo = recentSalesTempoFromDaily(
          dailyUnitsSeriesFromMap(start30ISO, today, sales.byDate30),
          RECENT_TEMPO_LOOKBACK_DAYS,
        );
        const dailySales30 = recentTempo.dailyRate;
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

        const currentForecast = forecastDemandForDate(todayDate);
        const demandForDayOffset = (day: number) => {
          const forecastDate = new Date(todayDate);
          forecastDate.setUTCDate(forecastDate.getUTCDate() + day);
          return forecastDemandForDate(forecastDate);
        };

        const onHand = projectDailyOos({
          inventory: available,
          todayDemand: currentForecast,
          demandForDayOffset,
        });
        const withInbound = projectDailyOos({
          inventory: effectiveUnits,
          todayDemand: currentForecast,
          demandForDayOffset,
        });

        const delayedLocalOnly: Array<{ dayOffset: number; units: number }> = [];
        if (localQty > 0) {
          delayedLocalOnly.push({ dayOffset: transferLeadDays, units: localQty });
        }

        const delayedFullPipeline = [...delayedLocalOnly];
        if (onOrderUnits > 0) {
          const supplierLead = leadBySku.get(sku) || 0;
          let onOrderDelay = Math.max(transferLeadDays, supplierLead + transferLeadDays);
          if (onOrderOrderedAt && supplierLead > 0) {
            const ordered = Date.parse(`${onOrderOrderedAt}T12:00:00Z`);
            const todayMs = Date.parse(`${today}T12:00:00Z`);
            if (Number.isFinite(ordered) && Number.isFinite(todayMs)) {
              const daysSinceOrder = Math.max(
                0,
                Math.round((todayMs - ordered) / 86_400_000),
              );
              const remainingToLocal = Math.max(0, supplierLead - daysSinceOrder);
              onOrderDelay = remainingToLocal + transferLeadDays;
            }
          }
          delayedFullPipeline.push({
            dayOffset: onOrderDelay,
            units: onOrderUnits,
          });
        }

        const withAmazonAndLocal = projectDailyOos({
          inventory: effectiveUnits,
          todayDemand: currentForecast,
          demandForDayOffset,
          delayedAdditions: delayedLocalOnly,
        });
        const withFullPipeline = projectDailyOos({
          inventory: effectiveUnits,
          todayDemand: currentForecast,
          demandForDayOffset,
          delayedAdditions: delayedFullPipeline,
        });

        const daysOfCover = withInbound.daysOfCover;
        const daysOfCoverOnHand = onHand.daysOfCover;
        const daysOfCoverAmazonAndLocal = withAmazonAndLocal.daysOfCover;
        const daysOfCoverWithLocal = withFullPipeline.daysOfCover;
        const statusCover =
          localQty > 0 || onOrderUnits > 0 ? daysOfCoverWithLocal : daysOfCover;
        const status = classifyStockStatus(available, inbound, statusCover, localQty);

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
          inbound,
          localQty,
          onOrderUnits,
          transferLeadDays,
          onOrderOrderedAt,
          units30: sales.units30,
          units90: sales.units90,
          dailySales30: Number(dailySales30.toFixed(2)),
          recentTempoDays: recentTempo.activeDays,
          recentTempoTruncated: recentTempo.truncated,
          forecastDailySales: Number(withInbound.forecastDailySales.toFixed(2)),
          forecastMethod: withInbound.forecastMethod,
          growthFactor: Number(growthFactor.toFixed(3)),
          growthPercent,
          comparisonCurrentUnits: sales.currentComparable,
          comparisonPreviousUnits: sales.previousComparable,
          daysOfCover,
          estimatedOosDate: daysOfCover === null ? null : addDays(today, daysOfCover),
          daysOfCoverOnHand,
          estimatedOosDateOnHand: daysOfCoverOnHand === null ? null : addDays(today, daysOfCoverOnHand),
          daysOfCoverAmazonAndLocal,
          estimatedOosDateAmazonAndLocal:
            daysOfCoverAmazonAndLocal === null ? null : addDays(today, daysOfCoverAmazonAndLocal),
          daysOfCoverWithLocal,
          estimatedOosDateWithLocal:
            daysOfCoverWithLocal === null ? null : addDays(today, daysOfCoverWithLocal),
          supplierLeadDays: leadBySku.get(sku) || null,
          status,
        };
      });

    return NextResponse.json(
      {
        ok: true,
        generatedAt: new Date().toISOString(),
        marketplace,
        salesWindow: {
          days: RECENT_TEMPO_LOOKBACK_DAYS,
          start: isoDate(startTempo),
          end: today,
          units30Start: start30ISO,
        },
        seasonalHistory: { start: historyStart, end: today },
        growthComparison: {
          current: { start: currentComparisonStart, end: currentComparisonEnd },
          previous: { start: previousComparisonStart, end: previousComparisonEnd },
          positiveGrowthOnly: true,
          stabilizationUnits: FORECAST_GROWTH_PRIOR_UNITS,
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

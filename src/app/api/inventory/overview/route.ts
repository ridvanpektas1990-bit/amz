import { NextRequest, NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";
import { tenantIdFromRequest } from "@/lib/amazon-tenant-cookie";
import { loadCatalogMetadata, type CatalogMetadata } from "@/lib/amazon-catalog";
import {
  FORECAST_GROWTH_PRIOR_UNITS,
  coverDaysFromWeeklyWeeks,
  isoWeekFromDateISO,
  planArrivalShipmentReorder,
  projectWeeklyOos,
  weeklyGrowthFactorFromMaps,
  ytdUnitsBeforeWeek,
} from "@/lib/inventory-forecast";
import {
  classifyStockStatus,
  effectiveInventoryUnits,
} from "@/lib/inventory-overview";
import { loadSkuSalesLinkedToOrders } from "@/lib/amazon-order-units";
import {
  dailyUnitsSeriesFromMap,
  recentSalesTempoFromDaily,
  recentSalesWindow,
  salesAsOfYesterdayISO,
  RECENT_SALES_UNITS30_DAYS,
  RECENT_TEMPO_LOOKBACK_DAYS,
} from "@/lib/recent-sales-tempo";
import {
  DEFAULT_AMAZON_TARGET_COVER_DAYS,
  DEFAULT_TRANSFER_LEAD_DAYS,
  supplierOrderQtyAfterPipeline,
} from "@/lib/local-stock";
import { applyInboundLocalDeductions, loadLocalStockBySku } from "@/lib/local-stock-db";
import { roundUpToCartons } from "@/lib/carton-specs";

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

function weeksCeilFromDays(days: number): number {
  const value = Math.max(0, Number(days) || 0);
  if (value <= 0) return 0;
  return Math.ceil(value / 7);
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
    // Sales complete only through yesterday — windows end there, not today.
    const salesAsOf = salesAsOfYesterdayISO(today);
    const { isoYear: currentIsoYear, isoWeek: currentIsoWeek } = isoWeekFromDateISO(today);
    const window30 = recentSalesWindow(RECENT_SALES_UNITS30_DAYS, salesAsOf);
    const windowTempo = recentSalesWindow(RECENT_TEMPO_LOOKBACK_DAYS, salesAsOf);
    const start30ISO = window30.startISO;
    const startTempoISO = windowTempo.startISO;
    const start90 = new Date(`${salesAsOf}T00:00:00Z`);
    start90.setUTCDate(start90.getUTCDate() - 89);
    const start90ISO = isoDate(start90);
    // Include late Dec of year-2 so ISO week 1 of previous year is complete.
    const historyStart = `${currentIsoYear - 2}-12-01`;

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
    const bufferBySku = new Map<string, number>();
    const unitsPerCartonBySku = new Map<string, number>();
    if (inventorySkus.length) {
      const { data: leadRows } = await sb
        .from("inventory_carton_specs")
        .select("seller_sku,production_time_days,shipping_time_days,buffer_time_days,units_per_carton")
        .eq("tenant_id", tenantId)
        .in("seller_sku", inventorySkus);
      for (const row of leadRows || []) {
        const sku = String((row as { seller_sku: string }).seller_sku);
        const lead =
          Math.max(0, Number((row as { production_time_days: number | null }).production_time_days) || 0) +
          Math.max(0, Number((row as { shipping_time_days: number | null }).shipping_time_days) || 0);
        if (lead > 0) leadBySku.set(sku, lead);
        const buffer = Math.max(
          0,
          Math.round(Number((row as { buffer_time_days: number | null }).buffer_time_days) || 0),
        );
        if (buffer > 0) bufferBySku.set(sku, buffer);
        const upc = Math.max(
          0,
          Math.floor(Number((row as { units_per_carton: number | null }).units_per_carton) || 0),
        );
        if (upc > 0) unitsPerCartonBySku.set(sku, upc);
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
        endISO: salesAsOf,
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
      byDate30: Map<string, number>;
      /** ISO week → units for previous ISO year (Vorjahr). */
      previousYearWeeks: Map<number, number>;
      /** ISO week → units for current ISO year. */
      currentYearWeeks: Map<number, number>;
    };
    const salesBySku = new Map<string, SalesAggregate>();
    for (const sale of salesRows) {
      const sku = String(sale.seller_sku || "").trim();
      const date = String(sale.purchase_date_berlin || "").slice(0, 10);
      if (!sku || !date || date > salesAsOf) continue;
      const quantity = Math.max(0, number(sale.quantity));
      const aggregate = salesBySku.get(sku) || {
        units30: 0,
        units90: 0,
        byDate30: new Map<string, number>(),
        previousYearWeeks: new Map<number, number>(),
        currentYearWeeks: new Map<number, number>(),
      };
      if (date >= start90ISO && date <= salesAsOf) aggregate.units90 += quantity;
      if (date >= start30ISO && date <= salesAsOf) {
        aggregate.units30 += quantity;
        aggregate.byDate30.set(date, (aggregate.byDate30.get(date) || 0) + quantity);
      }
      const { isoYear, isoWeek } = isoWeekFromDateISO(date);
      if (isoYear === currentIsoYear) {
        aggregate.currentYearWeeks.set(
          isoWeek,
          (aggregate.currentYearWeeks.get(isoWeek) || 0) + quantity,
        );
      } else if (isoYear === currentIsoYear - 1) {
        aggregate.previousYearWeeks.set(
          isoWeek,
          (aggregate.previousYearWeeks.get(isoWeek) || 0) + quantity,
        );
      }
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
          byDate30: new Map<string, number>(),
          previousYearWeeks: new Map<number, number>(),
          currentYearWeeks: new Map<number, number>(),
        };
        const available = Math.max(0, number(row.inventory_left));
        const inbound = Math.max(0, number(row.inbound_total));
        const local = localBySku.get(sku);
        const localQty = local?.localQty ?? 0;
        const onOrderUnits = local?.onOrderUnits ?? 0;
        const transferLeadDays = local?.transferLeadDays ?? DEFAULT_TRANSFER_LEAD_DAYS;
        const amazonTargetCoverDays =
          local?.amazonTargetCoverDays ?? DEFAULT_AMAZON_TARGET_COVER_DAYS;
        const onOrderOrderedAt = local?.onOrderOrderedAt ?? null;
        const effectiveUnits = effectiveInventoryUnits(available, inbound);
        const recentTempo = recentSalesTempoFromDaily(
          dailyUnitsSeriesFromMap(start30ISO, salesAsOf, sales.byDate30),
          RECENT_TEMPO_LOOKBACK_DAYS,
        );
        const dailySales30 = recentTempo.dailyRate;
        const recent30Units = Math.max(0, Math.round(dailySales30 * recentTempo.activeDays));
        const previousYearWeekTotals = sales.previousYearWeeks;
        const currentYearWeekTotals = sales.currentYearWeeks;
        const growthFactor = weeklyGrowthFactorFromMaps(
          currentYearWeekTotals,
          previousYearWeekTotals,
          currentIsoWeek,
        );
        const growthPercent = Math.round((growthFactor - 1) * 1000) / 10;
        const hasSeasonal = [...previousYearWeekTotals.values()].some((v) => v > 0);
        const hasRecent = dailySales30 > 0;
        const forecastMethod = hasSeasonal
          ? hasRecent
            ? "hybrid"
            : "seasonal"
          : hasRecent
            ? "recent"
            : "none";

        const weeklyBase = {
          currentWeek: currentIsoWeek,
          previousYearWeekTotals,
          currentYearWeekTotals,
          recent30Units,
          recentTempoDays: recentTempo.activeDays,
        };

        const onHand = projectWeeklyOos({ inventory: available, ...weeklyBase });
        const withInbound = projectWeeklyOos({ inventory: effectiveUnits, ...weeklyBase });

        const transferWeeks = weeksCeilFromDays(transferLeadDays);
        const delayedLocalOnly: Array<{ weekOffset: number; units: number }> = [];
        if (localQty > 0) {
          delayedLocalOnly.push({ weekOffset: transferWeeks, units: localQty });
        }

        const delayedFullPipeline = [...delayedLocalOnly];
        const supplierLead = leadBySku.get(sku) || 0;
        if (onOrderUnits > 0) {
          let onOrderDelayDays = Math.max(transferLeadDays, supplierLead + transferLeadDays);
          if (onOrderOrderedAt && supplierLead > 0) {
            const ordered = Date.parse(`${onOrderOrderedAt}T12:00:00Z`);
            const todayMs = Date.parse(`${today}T12:00:00Z`);
            if (Number.isFinite(ordered) && Number.isFinite(todayMs)) {
              const daysSinceOrder = Math.max(
                0,
                Math.round((todayMs - ordered) / 86_400_000),
              );
              const remainingToLocal = Math.max(0, supplierLead - daysSinceOrder);
              onOrderDelayDays = remainingToLocal + transferLeadDays;
            }
          }
          delayedFullPipeline.push({
            weekOffset: weeksCeilFromDays(onOrderDelayDays),
            units: onOrderUnits,
          });
        }

        const withAmazonAndLocal = projectWeeklyOos({
          inventory: effectiveUnits,
          ...weeklyBase,
          delayedAdditions: delayedLocalOnly,
        });
        const withFullPipeline = projectWeeklyOos({
          inventory: effectiveUnits,
          ...weeklyBase,
          delayedAdditions: delayedFullPipeline,
        });

        const daysOfCover = coverDaysFromWeeklyWeeks(withInbound.weeks);
        const daysOfCoverOnHand = coverDaysFromWeeklyWeeks(onHand.weeks);
        const daysOfCoverAmazonAndLocal = coverDaysFromWeeklyWeeks(withAmazonAndLocal.weeks);
        const daysOfCoverWithLocal = coverDaysFromWeeklyWeeks(withFullPipeline.weeks);
        const statusCover =
          localQty > 0 || onOrderUnits > 0 ? daysOfCoverWithLocal : daysOfCover;
        const status = classifyStockStatus(available, inbound, statusCover, localQty);

        const bufferDays = bufferBySku.get(sku) ?? 0;
        const unitsPerCarton = unitsPerCartonBySku.get(sku) ?? null;

        let recommendedShipQty: number | null = null;
        if (localQty > 0 && (hasSeasonal || hasRecent)) {
          const shipPlan = planArrivalShipmentReorder({
            inventory: effectiveUnits,
            currentIsoYear,
            currentIsoWeek,
            previousYearWeekTotals,
            currentYearWeekTotals,
            recent30Units,
            recentTempoDays: recentTempo.activeDays,
            leadTimeDays: 0,
            bufferDays: amazonTargetCoverDays,
          });
          if (shipPlan && shipPlan.reorderQty > 0) {
            const rounded = roundUpToCartons(shipPlan.reorderQty, unitsPerCarton);
            const qty = Math.min(rounded.orderQty, localQty);
            recommendedShipQty = qty > 0 ? qty : null;
          }
        }

        let recommendedOrderQty: number | null = null;
        if (supplierLead > 0 && (hasSeasonal || hasRecent)) {
          const orderPlan = planArrivalShipmentReorder({
            inventory: effectiveUnits,
            currentIsoYear,
            currentIsoWeek,
            previousYearWeekTotals,
            currentYearWeekTotals,
            recent30Units,
            recentTempoDays: recentTempo.activeDays,
            leadTimeDays: supplierLead,
            bufferDays,
          });
          if (orderPlan && orderPlan.reorderQty > 0) {
            const afterPipeline = supplierOrderQtyAfterPipeline({
              rawChargeQty: orderPlan.reorderQty,
              onOrderUnits,
            });
            const rounded = roundUpToCartons(afterPipeline, unitsPerCarton);
            recommendedOrderQty = rounded.orderQty > 0 ? rounded.orderQty : null;
          }
        }

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
          amazonTargetCoverDays,
          bufferTimeDays: bufferBySku.get(sku) ?? null,
          unitsPerCarton,
          onOrderOrderedAt,
          units30: sales.units30,
          units90: sales.units90,
          dailySales30: Number(dailySales30.toFixed(2)),
          recentTempoDays: recentTempo.activeDays,
          recentTempoTruncated: recentTempo.truncated,
          forecastDailySales: Number(dailySales30.toFixed(2)),
          forecastMethod,
          growthFactor: Number(growthFactor.toFixed(3)),
          growthPercent,
          comparisonCurrentUnits: ytdUnitsBeforeWeek(currentYearWeekTotals, currentIsoWeek),
          comparisonPreviousUnits: ytdUnitsBeforeWeek(previousYearWeekTotals, currentIsoWeek),
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
          supplierLeadDays: supplierLead > 0 ? supplierLead : null,
          recommendedShipQty,
          recommendedOrderQty,
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
          start: startTempoISO,
          end: salesAsOf,
          units30Start: start30ISO,
          units30End: salesAsOf,
          excludesToday: true,
        },
        seasonalHistory: { start: historyStart, end: salesAsOf, engine: "weekly_ly_growth" },
        growthComparison: {
          method: "iso_week_ytd_before_current_kw",
          currentIsoYear,
          currentIsoWeek,
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

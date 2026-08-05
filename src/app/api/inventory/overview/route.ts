import { NextRequest, NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";
import aws4 from "aws4";
import { tenantIdFromRequest } from "@/lib/amazon-tenant-cookie";
import { loadAmazonConnection } from "@/lib/amazon-connection";

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

type CatalogMetadata = {
  imageUrl: string | null;
  productName: string | null;
};

type CatalogItem = {
  asin?: string;
  images?: Array<{
    marketplaceId?: string;
    images?: Array<{ variant?: string; link?: string; width?: number; height?: number }>;
  }>;
  summaries?: Array<{ marketplaceId?: string; itemName?: string }>;
};

const CATALOG_CACHE_TTL_MS = 24 * 60 * 60 * 1000;
const catalogCache = new Map<string, { value: CatalogMetadata; expiresAt: number }>();

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

async function refreshAccessToken(refreshToken: string): Promise<string> {
  const body = new URLSearchParams({
    grant_type: "refresh_token",
    refresh_token: refreshToken,
    client_id: must("LWA_CLIENT_ID"),
    client_secret: must("LWA_CLIENT_SECRET"),
  });
  const response = await fetch("https://api.amazon.com/auth/o2/token", {
    method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded" },
    body,
    cache: "no-store",
  });
  if (!response.ok) throw new Error(`LWA refresh failed ${response.status}`);
  const json = (await response.json()) as { access_token?: string };
  if (!json.access_token) throw new Error("LWA access token missing");
  return json.access_token;
}

function safeAmazonImage(url: string | undefined): string | null {
  if (!url) return null;
  try {
    const parsed = new URL(url);
    const allowed = parsed.protocol === "https:" && (
      parsed.hostname.endsWith("media-amazon.com") ||
      parsed.hostname.endsWith("ssl-images-amazon.com")
    );
    return allowed ? parsed.toString() : null;
  } catch {
    return null;
  }
}

async function loadCatalogMetadata(
  req: NextRequest,
  asins: string[],
  marketplaceId: string,
): Promise<Map<string, CatalogMetadata>> {
  const result = new Map<string, CatalogMetadata>();
  const missing: string[] = [];
  const now = Date.now();

  for (const asin of asins) {
    const cached = catalogCache.get(asin);
    if (cached && cached.expiresAt > now) result.set(asin, cached.value);
    else missing.push(asin);
  }
  if (!missing.length) return result;

  const connectionResult = await loadAmazonConnection(req, "eu");
  if (!connectionResult.connection) return result;
  const accessToken = await refreshAccessToken(connectionResult.connection.refresh_token);
  const host = "sellingpartnerapi-eu.amazon.com";

  for (let start = 0; start < missing.length; start += 20) {
    const batch = missing.slice(start, start + 20);
    const query = new URLSearchParams({
      identifiers: batch.join(","),
      identifiersType: "ASIN",
      marketplaceIds: marketplaceId,
      includedData: "images,summaries",
      pageSize: String(batch.length),
    });
    const path = `/catalog/2022-04-01/items?${query.toString()}`;
    const headers: Record<string, string> = {
      "x-amz-access-token": accessToken,
      accept: "application/json",
      "user-agent": "amz-profit/1.0",
    };
    const signed = aws4.sign(
      { host, path, service: "execute-api", region: "eu-west-1", method: "GET", headers },
      { accessKeyId: must("AWS_ACCESS_KEY_ID"), secretAccessKey: must("AWS_SECRET_ACCESS_KEY") },
    );
    const response = await fetch(`https://${host}${path}`, {
      headers: signed.headers as HeadersInit,
      cache: "no-store",
    });
    if (!response.ok) throw new Error(`Catalog Items failed ${response.status}`);
    const json = (await response.json()) as { items?: CatalogItem[] };

    for (const item of json.items || []) {
      const asin = String(item.asin || "").trim();
      if (!asin) continue;
      const marketplaceImages = item.images?.find((entry) => entry.marketplaceId === marketplaceId)
        || item.images?.[0];
      const main = marketplaceImages?.images?.find((image) => image.variant === "MAIN")
        || marketplaceImages?.images?.[0];
      const summary = item.summaries?.find((entry) => entry.marketplaceId === marketplaceId)
        || item.summaries?.[0];
      const value = {
        imageUrl: safeAmazonImage(main?.link),
        productName: summary?.itemName?.trim() || null,
      };
      result.set(asin, value);
      catalogCache.set(asin, { value, expiresAt: now + CATALOG_CACHE_TTL_MS });
    }

    for (const asin of batch) {
      if (result.has(asin)) continue;
      const value = { imageUrl: null, productName: null };
      result.set(asin, value);
      catalogCache.set(asin, { value, expiresAt: now + CATALOG_CACHE_TTL_MS });
    }
  }
  return result;
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
        const growthPriorUnits = 100;
        const stabilizedGrowthFactor = sales.previousComparable > 0 && sales.currentComparable > sales.previousComparable
          ? (sales.currentComparable + growthPriorUnits) / (sales.previousComparable + growthPriorUnits)
          : 1;
        const growthFactor = stabilizedGrowthFactor > 1 ? stabilizedGrowthFactor : 1;
        const growthPercent = Math.round((growthFactor - 1) * 1000) / 10;

        const forecastDemandForDate = (forecastDate: Date) => {
          const sourceDate = previousYearDate(forecastDate);
          const sourceYear = sourceDate.getUTCFullYear();
          const sourceMonth = sourceDate.getUTCMonth();
          const sourceKey = `${sourceYear}-${String(sourceMonth + 1).padStart(2, "0")}`;
          const seasonalUnits = sales.byMonth.get(sourceKey) || 0;
          const canUseSeason = isCompleteMonth(sourceYear, sourceMonth, today);
          const seasonalRate = canUseSeason && seasonalUnits > 0
            ? (seasonalUnits / daysInMonth(sourceYear, sourceMonth)) * growthFactor
            : 0;
          return {
            demand: seasonalRate > 0 ? seasonalRate : dailySales30,
            seasonal: seasonalRate > 0,
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

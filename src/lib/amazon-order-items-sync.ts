import { createClient, type SupabaseClient } from "@supabase/supabase-js";
import aws4 from "aws4";

type SpOrderItem = {
  OrderItemId?: string;
  ASIN?: string;
  SellerSKU?: string;
  Title?: string;
  QuantityOrdered?: number;
  QuantityShipped?: number;
};

const HOSTS = {
  eu: "sellingpartnerapi-eu.amazon.com",
  na: "sellingpartnerapi-na.amazon.com",
  fe: "sellingpartnerapi-fe.amazon.com",
} as const;

const AWS_REGION = {
  eu: "eu-west-1",
  na: "us-east-1",
  fe: "us-west-2",
} as const;

function must(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing env ${name}`);
  return value;
}

function berlinDateFromTs(value: string): string {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: "Europe/Berlin",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(new Date(value));
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
  });
  if (!response.ok) throw new Error(`LWA refresh failed ${response.status}`);
  const json = (await response.json()) as { access_token: string };
  return json.access_token;
}

async function fetchOrderItemsSpApi({
  orderId,
  accessToken,
  region,
}: {
  orderId: string;
  accessToken: string;
  region: keyof typeof HOSTS;
}): Promise<SpOrderItem[]> {
  const host = HOSTS[region] ?? HOSTS.eu;
  const path = `/orders/v0/orders/${encodeURIComponent(orderId)}/orderItems`;
  const headers: Record<string, string> = {
    "x-amz-access-token": accessToken,
    accept: "application/json",
    "user-agent": "amz-profit/1.0",
  };
  const signed = aws4.sign(
    {
      host,
      path,
      method: "GET",
      service: "execute-api",
      region: AWS_REGION[region] ?? AWS_REGION.eu,
      headers,
    },
    {
      accessKeyId: must("AWS_ACCESS_KEY_ID"),
      secretAccessKey: must("AWS_SECRET_ACCESS_KEY"),
    },
  );

  const response = await fetch(`https://${host}${path}`, {
    method: "GET",
    headers: signed.headers as Record<string, string>,
    cache: "no-store",
  });
  const text = await response.text();
  if (response.status === 429) {
    const err = new Error("throttled") as Error & { status: number };
    err.status = 429;
    throw err;
  }
  if (!response.ok) {
    throw new Error(`OrderItems ${orderId}: ${response.status} ${text.slice(0, 200)}`);
  }
  const json = JSON.parse(text) as { payload?: { OrderItems?: SpOrderItem[] } };
  return json.payload?.OrderItems ?? [];
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export type OrderItemsSyncResult = {
  considered: number;
  alreadyCached: number;
  fetched: number;
  upserted: number;
  errors: string[];
};

/**
 * Backfill amazon_order_items for amazon_orders missing line items.
 * Prefer startISO/endISO for a fixed range; otherwise last `days` ending today.
 */
export async function syncRecentOrderItems({
  sb,
  tenantId,
  marketplace = "DE",
  region = "eu",
  refreshToken,
  days = 21,
  startISO: startISOArg,
  endISO: endISOArg,
  paceMs = 800,
  maxOrders = 400,
}: {
  sb: SupabaseClient;
  tenantId: string;
  marketplace?: string;
  region?: keyof typeof HOSTS;
  refreshToken: string;
  days?: number;
  startISO?: string;
  endISO?: string;
  paceMs?: number;
  maxOrders?: number;
}): Promise<OrderItemsSyncResult> {
  const todayISO = berlinDateFromTs(new Date().toISOString());
  let startISO = startISOArg;
  let endISO = endISOArg || todayISO;
  if (!startISO) {
    const start = new Date(`${todayISO}T12:00:00Z`);
    start.setUTCDate(start.getUTCDate() - Math.max(1, days) + 1);
    startISO = start.toISOString().slice(0, 10);
  }

  // Paginate all orders in range (newest first), then cap by maxOrders.
  type OrderRow = {
    amazon_order_id: string;
    purchase_date_local: string | null;
    iso_year: number | null;
    iso_week: number | null;
    order_status: string | null;
  };
  const orderRows: OrderRow[] = [];
  const PAGE = 1000;
  for (let from = 0; orderRows.length < maxOrders; from += PAGE) {
    const to = from + PAGE - 1;
    const { data, error } = await sb
      .from("amazon_orders")
      .select("amazon_order_id,purchase_date_local,iso_year,iso_week,order_status")
      .eq("tenant_id", tenantId)
      .eq("marketplace", marketplace)
      .gte("purchase_date_local", `${startISO}T00:00:00+00:00`)
      .lte("purchase_date_local", `${endISO}T23:59:59.999+00:00`)
      .neq("order_status", "Canceled")
      .order("purchase_date_local", { ascending: false })
      .range(from, to);
    if (error) throw new Error(`Orders: ${error.message}`);
    if (!data?.length) break;
    for (const row of data as OrderRow[]) {
      orderRows.push(row);
      if (orderRows.length >= maxOrders) break;
    }
    if (data.length < PAGE) break;
  }
  const orderIds = orderRows.map((row) => String(row.amazon_order_id));

  const cached = new Set<string>();
  for (let i = 0; i < orderIds.length; i += 200) {
    const chunk = orderIds.slice(i, i + 200);
    const { data, error } = await sb
      .from("amazon_order_items")
      .select("amazon_order_id")
      .eq("tenant_id", tenantId)
      .eq("marketplace", marketplace)
      .in("amazon_order_id", chunk);
    if (error) throw new Error(`Cached items: ${error.message}`);
    for (const row of data || []) cached.add(String(row.amazon_order_id));
  }

  const missing = orderRows.filter((row) => !cached.has(String(row.amazon_order_id)));
  const result: OrderItemsSyncResult = {
    considered: orderRows.length,
    alreadyCached: orderRows.length - missing.length,
    fetched: 0,
    upserted: 0,
    errors: [],
  };

  if (!missing.length) return result;

  let accessToken = await refreshAccessToken(refreshToken);
  const upserts: Record<string, unknown>[] = [];
  const FLUSH_EVERY = 40;
  const MAX_RETRIES = 6;

  async function flushUpserts() {
    if (!upserts.length) return;
    for (let i = 0; i < upserts.length; i += 200) {
      const chunk = upserts.slice(i, i + 200);
      const { error } = await sb.from("amazon_order_items").upsert(chunk, {
        onConflict: "tenant_id,marketplace,amazon_order_id,order_item_id",
      });
      if (error) throw new Error(`Upsert items: ${error.message}`);
      result.upserted += chunk.length;
    }
    upserts.length = 0;
  }

  async function fetchWithRetry(orderId: string): Promise<SpOrderItem[] | null> {
    for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
      try {
        const items = await fetchOrderItemsSpApi({ orderId, accessToken, region });
        result.fetched += 1;
        return items;
      } catch (err: unknown) {
        const status = (err as { status?: number }).status;
        const message = err instanceof Error ? err.message : String(err);
        if (status === 429 && attempt < MAX_RETRIES) {
          const waitMs = Math.min(30_000, Math.max(paceMs * 2, 2000) * 2 ** attempt);
          await sleep(waitMs);
          if (attempt % 2 === 1) accessToken = await refreshAccessToken(refreshToken);
          continue;
        }
        result.errors.push(`${orderId}: ${message}`);
        return null;
      }
    }
    return null;
  }

  for (let idx = 0; idx < missing.length; idx++) {
    const order = missing[idx];
    const orderId = String(order.amazon_order_id);
    const items = await fetchWithRetry(orderId);
    if (!items) {
      await sleep(paceMs);
      continue;
    }

    const local = order.purchase_date_local ? String(order.purchase_date_local) : null;
    const berlin = local ? berlinDateFromTs(local) : null;
    for (const item of items) {
      const orderItemId = String(item.OrderItemId || "").trim();
      if (!orderItemId) continue;
      upserts.push({
        tenant_id: tenantId,
        marketplace,
        amazon_order_id: orderId,
        order_item_id: orderItemId,
        seller_sku: item.SellerSKU ? String(item.SellerSKU) : null,
        asin: item.ASIN ? String(item.ASIN) : null,
        title: item.Title ? String(item.Title) : null,
        quantity_ordered: Math.max(0, Number(item.QuantityOrdered) || 0),
        quantity_shipped: Math.max(0, Number(item.QuantityShipped) || 0),
        purchase_date_local: local,
        purchase_date_berlin: berlin,
        iso_year: Number(order.iso_year) || null,
        iso_week: Number(order.iso_week) || null,
        updated_at: new Date().toISOString(),
      });
    }

    if ((idx + 1) % FLUSH_EVERY === 0) {
      await flushUpserts();
      // Refresh token every ~30 min of wall time (~2000 orders @ 800ms).
      if ((idx + 1) % 2000 === 0) {
        accessToken = await refreshAccessToken(refreshToken);
      }
      console.log(
        JSON.stringify({
          phase: "progress",
          fetched: result.fetched,
          upserted: result.upserted,
          errors: result.errors.length,
          of: missing.length,
          lastError: result.errors[result.errors.length - 1] || null,
          at: new Date().toISOString(),
        }),
      );
    }
    await sleep(paceMs);
  }

  await flushUpserts();
  return result;
}

export function createServiceSupabase() {
  return createClient(must("SUPABASE_URL"), must("SUPABASE_SERVICE_ROLE_KEY"), {
    auth: { persistSession: false },
  });
}

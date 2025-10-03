import { NextRequest, NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";
import aws4 from "aws4";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function must(name: string){ const v = process.env[name]; if(!v) throw new Error(`Missing env ${name}`); return v; }

async function refreshAccessToken(refreshToken: string){
  const body = new URLSearchParams({
    grant_type: "refresh_token",
    refresh_token: refreshToken,
    client_id: must("LWA_CLIENT_ID"),
    client_secret: must("LWA_CLIENT_SECRET"),
  });
  const r = await fetch("https://api.amazon.com/auth/o2/token", {
    method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded" },
    body,
  });
  if (!r.ok) throw new Error(`LWA refresh failed ${r.status}`);
  return r.json() as Promise<{ access_token: string; expires_in: number }>;
}

const REGION_HOST: Record<string,string> = {
  eu: "sellingpartnerapi-eu.amazon.com",
  na: "sellingpartnerapi-na.amazon.com",
  fe: "sellingpartnerapi-fe.amazon.com",
};

function toIsoZ(d: Date){
  // YYYY-MM-DDTHH:mm:ssZ
  return new Date(d.getTime() - (d.getTime()%1000)).toISOString().replace(/\.\d{3}Z$/, "Z");
}

export async function GET(req: NextRequest) {
  try {
    const url = new URL(req.url);

    // ---- Query-Params (mit sinnvollen Defaults) ----
    const region = (url.searchParams.get("region") ?? process.env.NEXT_PUBLIC_DEFAULT_REGION ?? "eu").toLowerCase();
    const marketplaceId = url.searchParams.get("marketplaceId") ?? "A1PA6795UKMFR9"; // DE
    const createdAfterParam = url.searchParams.get("createdAfter");
    const nextToken = url.searchParams.get("nextToken") || undefined;
    const orderStatuses = url.searchParams.get("orderStatuses") || ""; // z.B. "Unshipped,Shipped"
    const maxPerPage = Math.min(Math.max(Number(url.searchParams.get("pageSize") ?? 20), 1), 100);

    // Default: letzte 30 Tage
    const createdAfter = createdAfterParam
      ? new Date(createdAfterParam)
      : new Date(Date.now() - 30 * 24 * 3600 * 1000);
    const createdAfterIso = toIsoZ(createdAfter);

    // ---- Refresh-Token laden ----
    const sb = createClient(must("SUPABASE_URL"), must("SUPABASE_SERVICE_ROLE_KEY"), { auth: { persistSession: false } });
    const { data: conn, error: dbErr } = await sb
      .from("amazon_connections")
      .select("seller_id, refresh_token")
      .eq("region", region)
      .order("created_at", { ascending: false })
      .limit(1)
      .maybeSingle();
    if (dbErr) return NextResponse.json({ ok:false, error: dbErr.message }, { status: 500 });
    if (!conn?.refresh_token) return NextResponse.json({ ok:false, error:"no_refresh_token" }, { status: 400 });

    // ---- LWA Access-Token ----
    const { access_token } = await refreshAccessToken(conn.refresh_token);

    // ---- Request bauen (Orders v0) ----
    const host = REGION_HOST[region] ?? REGION_HOST.eu;

    // Wenn NextToken genutzt wird, darfst du KEINE anderen Filter mitsenden.
    const qs = nextToken
      ? new URLSearchParams({ NextToken: nextToken })
      : new URLSearchParams({
          MarketplaceIds: marketplaceId,
          CreatedAfter: createdAfterIso,
          MaxResultsPerPage: String(maxPerPage),
          ...(orderStatuses ? { OrderStatuses: orderStatuses } : {}),
          // Optional weitere Filter: FulfillmentChannels, PaymentMethods, EasyShipShipmentStatuses...
        });

    const path = `/orders/v0/orders?${qs.toString()}`;
    const headers: Record<string,string> = {
      "x-amz-access-token": access_token,
      "accept": "application/json",
      "user-agent": "amz-profit/1.0"
    };

    const signed = aws4.sign(
      {
        host,
        path,
        service: "execute-api",
        region: region === "eu" ? "eu-west-1" : region === "na" ? "us-east-1" : "us-west-2", // FE = us-west-2 (laut AWS)
        method: "GET",
        headers,
      },
      {
        accessKeyId: must("AWS_ACCESS_KEY_ID"),
        secretAccessKey: must("AWS_SECRET_ACCESS_KEY"),
        // sessionToken: must("AWS_SESSION_TOKEN") // nur falls STS genutzt wird
      }
    );

    const sp = await fetch(`https://${host}${path}`, {
      method: "GET",
      headers: signed.headers as any,
      cache: "no-store",
    });

    const text = await sp.text();
    let json: any = undefined;
    try { json = JSON.parse(text); } catch { /* leave undefined */ }

    if (!sp.ok) {
      return NextResponse.json({ ok:false, status: sp.status, error: (json?.errors ?? text).toString().slice(0,3000) }, { status: sp.status });
    }

    // Nur „nicht-restricted“ Felder an den Client geben (PII bleibt weg)
    const safeOrders = (json?.payload?.Orders ?? []).map((o: any) => ({
      amazonOrderId: o.AmazonOrderId,
      marketplaceId: o.MarketplaceId,
      purchaseDate: o.PurchaseDate,
      lastUpdateDate: o.LastUpdateDate,
      orderStatus: o.OrderStatus,
      orderTotal: o.OrderTotal ?? null, // {CurrencyCode, Amount}
      numberOfItemsShipped: o.NumberOfItemsShipped,
      numberOfItemsUnshipped: o.NumberOfItemsUnshipped,
      salesChannel: o.SalesChannel ?? null,
      shipmentServiceLevelCategory: o.ShipmentServiceLevelCategory ?? null,
      isPrime: o.IsPrime ?? false,
      isPremiumOrder: o.IsPremiumOrder ?? false,
      isBusinessOrder: o.IsBusinessOrder ?? false,
      // KEINE Adresse/Käuferdaten etc. (Restricted)
    }));

    return NextResponse.json({
      ok: true,
      seller_id: conn.seller_id ?? null,
      count: safeOrders.length,
      nextToken: json?.payload?.NextToken ?? null,
      orders: safeOrders,
      rawNote: "This payload excludes PII and restricted fields."
    });
  } catch (e:any) {
    return NextResponse.json({ ok:false, error: e.message }, { status: 500 });
  }
}

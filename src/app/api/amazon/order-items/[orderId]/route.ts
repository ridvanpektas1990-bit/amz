import { NextRequest, NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";
import aws4 from "aws4";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function must(n: string) {
  const v = process.env[n];
  if (!v) throw new Error(`Missing env ${n}`);
  return v;
}

async function refreshAccessToken(refreshToken: string) {
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
  return r.json() as Promise<{ access_token: string }>;
}

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

export async function GET(
  req: NextRequest,
  context: { params: Promise<{ orderId: string }> }
) {
  try {
    const { orderId } = await context.params;
    const url = new URL(req.url);
    const region = (url.searchParams.get("region") ?? process.env.NEXT_PUBLIC_DEFAULT_REGION ?? "eu")
      .toLowerCase() as keyof typeof HOSTS;

    // Refresh-Token aus DB
    const sb = createClient(must("SUPABASE_URL"), must("SUPABASE_SERVICE_ROLE_KEY"), {
      auth: { persistSession: false },
    });
    const { data: conn, error } = await sb
      .from("amazon_connections")
      .select("seller_id, refresh_token")
      .eq("region", region)
      .order("created_at", { ascending: false })
      .limit(1)
      .maybeSingle();
    if (error) return NextResponse.json({ ok: false, error: error.message }, { status: 500 });
    if (!conn?.refresh_token) return NextResponse.json({ ok: false, error: "no_refresh_token" }, { status: 400 });

    const { access_token } = await refreshAccessToken(conn.refresh_token);

    const host = HOSTS[region] ?? HOSTS.eu;
    const path = `/orders/v0/orders/${encodeURIComponent(orderId)}/orderItems`;

    const signed = aws4.sign(
      {
        host,
        path,
        method: "GET",
        service: "execute-api",
        region: AWS_REGION[region] ?? AWS_REGION.eu,
        headers: { "x-amz-access-token": access_token, accept: "application/json", "user-agent": "amz-profit/1.0" },
      },
      {
        accessKeyId: must("AWS_ACCESS_KEY_ID"),
        secretAccessKey: must("AWS_SECRET_ACCESS_KEY"),
      }
    );

    const resp = await fetch(`https://${host}${path}`, {
      method: "GET",
      headers: signed.headers as any,
      cache: "no-store",
    });

    const txt = await resp.text();
    if (!resp.ok) return NextResponse.json({ ok: false, status: resp.status, error: txt.slice(0, 2000) }, { status: resp.status });

    const json = JSON.parse(txt);
    const items = (json?.payload?.OrderItems ?? []).map((it: any) => ({
      asin: it.ASIN,
      sellerSKU: it.SellerSKU ?? null,
      title: it.Title ?? null,
      quantityOrdered: it.QuantityOrdered,
      quantityShipped: it.QuantityShipped ?? 0,
      itemPrice: it.ItemPrice ?? null,
      itemTax: it.ItemTax ?? null,
    }));

    return NextResponse.json({ ok: true, orderId, items });
  } catch (e: any) {
    return NextResponse.json({ ok: false, error: e.message }, { status: 500 });
  }
}

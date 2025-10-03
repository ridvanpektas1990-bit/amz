import { NextRequest, NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";
import aws4 from "aws4";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function must(name: string) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env ${name}`);
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
  return r.json() as Promise<{ access_token: string; expires_in: number }>;
}

const HOSTS = {
  eu: "sellingpartnerapi-eu.amazon.com",
  na: "sellingpartnerapi-na.amazon.com",
  fe: "sellingpartnerapi-fe.amazon.com",
} as const;
const AWS_REGION = { eu: "eu-west-1", na: "us-east-1", fe: "us-west-2" } as const;

export async function GET(
  req: NextRequest,
  context: { params: Promise<{ orderId: string }> }
) {
  try {
    const { orderId } = await context.params;
    const url = new URL(req.url);
    const region = (url.searchParams.get("region") ?? process.env.NEXT_PUBLIC_DEFAULT_REGION ?? "eu")
      .toLowerCase() as keyof typeof HOSTS;

    // 1) Refresh-Token holen
    const sb = createClient(must("SUPABASE_URL"), must("SUPABASE_SERVICE_ROLE_KEY"), {
      auth: { persistSession: false },
    });
    const { data: conn, error: dbErr } = await sb
      .from("amazon_connections")
      .select("seller_id, refresh_token")
      .eq("region", region)
      .order("created_at", { ascending: false })
      .limit(1)
      .maybeSingle();
    if (dbErr) return NextResponse.json({ ok: false, error: dbErr.message }, { status: 500 });
    if (!conn?.refresh_token) return NextResponse.json({ ok: false, error: "no_refresh_token" }, { status: 400 });

    // 2) LWA Access Token
    const { access_token } = await refreshAccessToken(conn.refresh_token);

    // 3) Finances-Call
    const host = HOSTS[region] ?? HOSTS.eu;
    const path = `/finances/v0/orders/${encodeURIComponent(orderId)}/financialEvents`;

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

    const data = JSON.parse(txt);
    const fe = data?.payload?.FinancialEvents ?? data?.payload?.financialEvents ?? {};
    const shipments: any[] = fe.ShipmentEventList ?? fe.shipmentEventList ?? [];
    const adjustments: any[] = fe.ShipmentEventAdjustmentList ?? fe.shipmentEventAdjustmentList ?? [];

    type FeeRow = { type: string; amount: number; currency: string };
    type ItemRow = { orderItemId: string; sellerSKU?: string | null; fees: FeeRow[]; feeTotal: number };

    const byItem = new Map<string, ItemRow>();
    let currency = "EUR";

    const addFees = (orderItemId: string, sellerSKU: string | undefined | null, list: any[] | undefined, adjust = false) => {
      if (!list || !Array.isArray(list) || list.length === 0) return;
      const key = orderItemId || sellerSKU || "unknown";
      const row = byItem.get(key) ?? { orderItemId, sellerSKU: sellerSKU ?? null, fees: [], feeTotal: 0 };
      for (const f of list) {
        const type = String(f?.FeeType ?? (adjust ? "Adjustment" : "Fee"));
        const amt = Number(f?.FeeAmount?.Amount ?? 0);
        const cur = String(f?.FeeAmount?.CurrencyCode ?? currency);
        currency = cur || currency;
        if (!Number.isFinite(amt) || amt === 0) continue;
        row.fees.push({ type, amount: amt, currency: cur });
        row.feeTotal += amt;
      }
      byItem.set(key, row);
    };

    for (const se of shipments) {
      const items = se?.ShipmentItemList ?? se?.shipmentItemList ?? [];
      for (const it of items) {
        addFees(String(it?.OrderItemId ?? it?.orderItemId ?? ""), it?.SellerSKU ?? it?.sellerSKU, it?.ItemFeeList ?? it?.itemFeeList, false);
      }
    }
    for (const adj of adjustments) {
      const items = adj?.ShipmentItemAdjustmentList ?? adj?.shipmentItemAdjustmentList ?? [];
      for (const it of items) {
        addFees(String(it?.OrderItemId ?? it?.orderItemId ?? ""), it?.SellerSKU ?? it?.sellerSKU, it?.ItemFeeAdjustmentList ?? it?.itemFeeAdjustmentList, true);
      }
    }

    const items: ItemRow[] = Array.from(byItem.values()).map((r) => ({
      ...r,
      fees: Object.values(
        r.fees.reduce((acc: Record<string, FeeRow>, f) => {
          const k = `${f.type}|${f.currency}`;
          acc[k] = acc[k] ? { ...acc[k], amount: acc[k].amount + f.amount } : f;
          return acc;
        }, {})
      ),
    }));

    const totalFee = items.reduce((s, r) => s + r.feeTotal, 0);

    return NextResponse.json({
      ok: true,
      orderId,
      currency,
      totalFee,
      items,
      note: "Nur Fees (keine Charges/Promotions/Taxes). Events können bis zu ~48h verzögert sein.",
    });
  } catch (e: any) {
    return NextResponse.json({ ok: false, error: e.message }, { status: 500 });
  }
}

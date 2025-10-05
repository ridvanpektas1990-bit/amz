import { NextRequest, NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";
import aws4 from "aws4";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

/** === Helpers / Config === **/
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

const AWS_REGION = {
  eu: "eu-west-1",
  na: "us-east-1",
  fe: "us-west-2",
} as const;

/** FeeRow/ItemRow Types */
type FeeRow = { type: string; amount: number; currency: string };
type ItemRow = { key: string; orderItemId?: string | null; sellerSKU?: string | null; level: "order" | "item"; fees: FeeRow[]; feeTotal: number };

/** Normalisiert Felder (UpperCamel vs lowerCamel) */
function get<T = unknown>(o: any, ...paths: string[]): T | undefined {
  for (const p of paths) {
    const v = o?.[p];
    if (v !== undefined) return v as T;
  }
  return undefined;
}

function addFeeList(
  target: Map<string, ItemRow>,
  key: string,
  level: "order" | "item",
  list: any[] | undefined,
  currencyFallback: { value: string }
) {
  if (!Array.isArray(list) || list.length === 0) return;

  const row = target.get(key) ?? { key, level, orderItemId: null, sellerSKU: null, fees: [], feeTotal: 0 };

  for (const f of list) {
    // Fee-Typ (UpperCamel oder lowerCamel)
    const type = String(f?.FeeType ?? f?.feeType ?? "Fee");

    // FeeAmount kann UpperCamel oder lowerCamel sein
    const fa = f?.FeeAmount ?? f?.feeAmount;
    const amt = Number(fa?.Amount ?? fa?.amount ?? 0);
    const cur = String(fa?.CurrencyCode ?? fa?.currencyCode ?? currencyFallback.value);

    if (!Number.isFinite(amt) || amt === 0) continue;

    // Währung merken, falls später kein Code mitkommt
    currencyFallback.value = cur || currencyFallback.value;

    row.fees.push({ type, amount: amt, currency: cur });
    row.feeTotal += amt;
  }

  target.set(key, row);
}


/** Merged gleiche FeeTypes je Row (für hübschere Ausgabe) */
function mergeRowFees(row: ItemRow): ItemRow {
  const byKey: Record<string, FeeRow> = {};
  for (const f of row.fees) {
    const k = `${f.type}|${f.currency}`;
    byKey[k] = byKey[k] ? { ...byKey[k], amount: byKey[k].amount + f.amount } : f;
  }
  return { ...row, fees: Object.values(byKey) };
}

export async function GET(
  req: NextRequest,
  context: { params: Promise<{ orderId: string }> }
) {
  try {
    const { orderId } = await context.params;
    const url = new URL(req.url);
    const region = (url.searchParams.get("region") ?? process.env.NEXT_PUBLIC_DEFAULT_REGION ?? "eu").toLowerCase() as keyof typeof HOSTS;

    // 1) Refresh-Token aus DB
    const sb = createClient(must("SUPABASE_URL"), must("SUPABASE_SERVICE_ROLE_KEY"), { auth: { persistSession: false } });
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

    // 3) Finances-Order-Events
    const host = HOSTS[region] ?? HOSTS.eu;
    const path = `/finances/v0/orders/${encodeURIComponent(orderId)}/financialEvents`;

    const signed = aws4.sign(
      {
        host,
        path,
        method: "GET",
        service: "execute-api",
        region: AWS_REGION[region] ?? AWS_REGION.eu,
        headers: {
          "x-amz-access-token": access_token,
          accept: "application/json",
          "user-agent": "amz-profit/1.0",
        },
      },
      {
        accessKeyId: must("AWS_ACCESS_KEY_ID"),
        secretAccessKey: must("AWS_SECRET_ACCESS_KEY"),
      }
    );

    const resp = await fetch(`https://${host}${path}`, { method: "GET", headers: signed.headers as any, cache: "no-store" });
    const txt = await resp.text();
    if (!resp.ok) return NextResponse.json({ ok: false, status: resp.status, error: txt.slice(0, 2000) }, { status: resp.status });

    const data = JSON.parse(txt);
    const fe = get<any>(data?.payload ?? data, "FinancialEvents", "financialEvents") ?? {};
    const shipments: any[] = get<any[]>(fe, "ShipmentEventList", "shipmentEventList") ?? [];
    const adjustments: any[] = get<any[]>(fe, "ShipmentEventAdjustmentList", "shipmentEventAdjustmentList") ?? [];

    // === Aggregation ===
    const byKey = new Map<string, ItemRow>(); // key: "ORDER" oder OrderItemId
    const currency = { value: "EUR" };

    // A) ShipmentEvent (Order-EBENE & Item-EBENE)
    for (const se of shipments) {
      // --- Order-Level Fee Lists ---
      addFeeList(byKey, "ORDER", "order", get<any[]>(se, "OrderFeeList", "orderFeeList"), currency);
      addFeeList(byKey, "ORDER", "order", get<any[]>(se, "ShipmentFeeList", "shipmentFeeList"), currency);
      addFeeList(byKey, "ORDER", "order", get<any[]>(se, "OrderFeeAdjustmentList", "orderFeeAdjustmentList"), currency);
      addFeeList(byKey, "ORDER", "order", get<any[]>(se, "ShipmentFeeAdjustmentList", "shipmentFeeAdjustmentList"), currency);

      // --- Item-Level Lists ---
      const items = get<any[]>(se, "ShipmentItemList", "shipmentItemList") ?? [];
      for (const it of items) {
        const orderItemId = String(get<string>(it, "OrderItemId", "orderItemId") ?? "") || "UNKNOWN_ITEM";
        const sellerSKU = get<string>(it, "SellerSKU", "sellerSKU") ?? null;

        // Stelle sicher, dass Row existiert & Metadaten hängen
        const key = orderItemId;
        if (!byKey.has(key)) byKey.set(key, { key, level: "item", orderItemId, sellerSKU, fees: [], feeTotal: 0 });
        else {
          const ex = byKey.get(key)!;
          ex.orderItemId = ex.orderItemId ?? orderItemId;
          ex.sellerSKU = ex.sellerSKU ?? sellerSKU;
        }

        // Item Fees
        addFeeList(byKey, key, "item", get<any[]>(it, "ItemFeeList", "itemFeeList"), currency);
        addFeeList(byKey, key, "item", get<any[]>(it, "ItemFeeAdjustmentList", "itemFeeAdjustmentList"), currency);

        // Manche Gebühren tauchen als "ItemChargeList" (i. d. R. Charges, nicht Fees) auf – die ignorieren wir bewusst.
      }
    }

    // B) Adjustments (Refunds/Corrections) – Order & Item
    for (const adj of adjustments) {
      addFeeList(byKey, "ORDER", "order", get<any[]>(adj, "OrderFeeAdjustmentList", "orderFeeAdjustmentList"), currency);
      addFeeList(byKey, "ORDER", "order", get<any[]>(adj, "ShipmentFeeAdjustmentList", "shipmentFeeAdjustmentList"), currency);

      const items = get<any[]>(adj, "ShipmentItemAdjustmentList", "shipmentItemAdjustmentList") ?? [];
      for (const it of items) {
        const orderItemId = String(get<string>(it, "OrderItemId", "orderItemId") ?? "") || "UNKNOWN_ITEM";
        const sellerSKU = get<string>(it, "SellerSKU", "sellerSKU") ?? null;

        const key = orderItemId;
        if (!byKey.has(key)) byKey.set(key, { key, level: "item", orderItemId, sellerSKU, fees: [], feeTotal: 0 });
        else {
          const ex = byKey.get(key)!;
          ex.orderItemId = ex.orderItemId ?? orderItemId;
          ex.sellerSKU = ex.sellerSKU ?? sellerSKU;
        }

        addFeeList(byKey, key, "item", get<any[]>(it, "ItemFeeAdjustmentList", "itemFeeAdjustmentList"), currency);
      }
    }

    // (Optional) Weitere Event-Typen, falls nötig, kannst du ähnlich einhängen:
    // RefundEventList, GuaranteeClaimEventList, ChargebackEventList, ServiceFeeEventList, etc.
    // → Sag Bescheid, dann erweitere ich das gezielt.

    // Merge & Summen
    const items: ItemRow[] = Array.from(byKey.values()).map(mergeRowFees);
    const totalFee = items.reduce((s, r) => s + r.feeTotal, 0);

    return NextResponse.json({
      ok: true,
      orderId,
      currency: currency.value,
      totalFee,
      items: items.map(r => ({
        orderItemId: r.orderItemId ?? (r.level === "order" ? "ORDER" : null),
        sellerSKU: r.sellerSKU ?? null,
        level: r.level,
        fees: r.fees,
        feeTotal: r.feeTotal,
      })),
      note: "Order- und Item-Fees zusammengeführt. Events können je nach Marktplatz verzögert verbucht werden.",
    });
  } catch (e: any) {
    return NextResponse.json({ ok: false, error: e.message }, { status: 500 });
  }
}

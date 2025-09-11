import { NextRequest, NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function must(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env ${name}`);
  return v;
}
function supa() {
  return createClient(
    must("SUPABASE_URL"),
    must("SUPABASE_SERVICE_ROLE_KEY"),
    { auth: { persistSession: false } }
  );
}

export async function GET(req: NextRequest) {
  try {
    const url = new URL(req.url);
    const code = url.searchParams.get("spapi_oauth_code");
    const sellerId = url.searchParams.get("selling_partner_id");
    const returnedState = url.searchParams.get("state");
    const cookieState = req.cookies.get("amz_state")?.value;

    if (!code) return NextResponse.json({ ok: false, error: "Missing spapi_oauth_code" }, { status: 400 });
    if (!returnedState || !cookieState || returnedState !== cookieState)
      return NextResponse.json({ ok: false, error: "Invalid state" }, { status: 400 });

    const redirectUri = must("AMAZON_REDIRECT_URI");
    const body = new URLSearchParams({
      grant_type: "authorization_code",
      code,
      redirect_uri: redirectUri,
      client_id: must("LWA_CLIENT_ID"),
      client_secret: must("LWA_CLIENT_SECRET"),
    });

    const r = await fetch("https://api.amazon.com/auth/o2/token", {
      method: "POST",
      headers: { "content-type": "application/x-www-form-urlencoded" },
      body,
    });

    const tokens = await r.json() as { access_token?: string; refresh_token?: string; expires_in?: number };
    if (!r.ok || !tokens.refresh_token) {
      return NextResponse.json({ ok: false, error: "Token exchange failed", detail: tokens }, { status: 400 });
    }

    // In Supabase speichern (upsert auf seller_id+region)
    const region = (process.env.NEXT_PUBLIC_DEFAULT_REGION ?? "eu").toLowerCase();
    const sb = supa();
    const payload: Record<string, unknown> = {
      seller_id: sellerId,
      region,
      refresh_token: tokens.refresh_token,
    };

    // upsert nach seller_id+region (falls seller_id fehlt, legen wir trotzdem einen Datensatz an)
    const { error } = await sb
      .from("amazon_connections")
      .upsert(payload, { onConflict: "seller_id,region", ignoreDuplicates: false });

    if (error) {
      return NextResponse.json({ ok: false, error: "DB upsert failed", detail: error.message }, { status: 500 });
    }

    // State-Cookie löschen
    const res = NextResponse.json({ ok: true, seller_id: sellerId ?? null });
    res.cookies.set("amz_state", "", { path: "/", maxAge: 0 });
    return res;
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e);
    return NextResponse.json({ ok: false, error: msg }, { status: 500 });
  }
}

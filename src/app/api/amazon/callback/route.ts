// C:\Users\ridva\OneDrive\Desktop\Privat\Projekte\amz-connect\src\app\api\amazon\callback\route.ts
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
    const code = url.searchParams.get("spapi_oauth_code");       // Authorization Code
    const sellerId = url.searchParams.get("selling_partner_id"); // i.d.R. vorhanden
    const returnedState = url.searchParams.get("state");
    const cookieState = req.cookies.get("amz_state")?.value;

    if (!code) {
      return NextResponse.json({ ok: false, error: "Missing spapi_oauth_code" }, { status: 400 });
    }
    if (!returnedState || !cookieState || returnedState !== cookieState) {
      return NextResponse.json({ ok: false, error: "Invalid state" }, { status: 400 });
    }

    const redirectUri = must("REDIRECT_URI");

    // LWA: Code -> Tokens
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

    const tokens = (await r.json()) as {
      access_token?: string;
      refresh_token?: string;
      expires_in?: number;
    };
    if (!r.ok || !tokens.refresh_token) {
      return NextResponse.json(
        { ok: false, error: "Token exchange failed", detail: tokens },
        { status: 400 }
      );
    }

    if (!sellerId) {
      const res = NextResponse.redirect(new URL("/connect?auth=error&msg=missing_seller_id", url));
      res.cookies.set("amz_state", "", {
        path: "/",
        maxAge: 0,
        httpOnly: true,
        sameSite: "lax",
        secure: process.env.NODE_ENV === "production",
      });
      return res;
    }

    // Region aus Param/Env bestimmen (muss zu deiner DB-Check-Constraint passen: eu|na|fe)
    const regionParam = url.searchParams.get("region")?.toLowerCase();
    const region = (regionParam || process.env.NEXT_PUBLIC_DEFAULT_REGION || "eu").toLowerCase();

    // Konservativer Default der Marketplaces je Region (ohne AWS/SP-API Call)
    const DEFAULT_CODES: Record<string, string[]> = {
      eu: ["DE"],
      na: ["US"],
      fe: ["JP"],
    };
    let marketCodes = DEFAULT_CODES[region] ?? ["DE"];

    const sb = supa();

    // (Nice-to-have) bestehende marketplaces beibehalten/mergen statt zu überschreiben
    const { data: existingRow } = await sb
      .from("amazon_connections")
      .select("marketplaces")
      .eq("tenant_id", sellerId)
      .eq("region", region)
      .maybeSingle();

    if (existingRow?.marketplaces && Array.isArray(existingRow.marketplaces)) {
      const merged = new Set<string>([
        ...existingRow.marketplaces.map((x: unknown) => String(x).toUpperCase()),
        ...marketCodes.map((x) => x.toUpperCase()),
      ]);
      marketCodes = Array.from(merged);
    }

    // Upsert – WICHTIG: tenant_id setzen (hier = sellerId)
    const payload = {
      tenant_id: sellerId,        // NOT NULL + (tenant_id, region) unique key
      seller_id: sellerId,
      region,
      refresh_token: tokens.refresh_token,
      marketplaces: marketCodes,  // jsonb Array
    };

    const { error } = await sb
      .from("amazon_connections")
      .upsert(payload, { onConflict: "tenant_id,region", ignoreDuplicates: false });

    if (error) {
      return NextResponse.json(
        { ok: false, error: "DB upsert failed", detail: error.message },
        { status: 500 }
      );
    }

    // State-Cookie löschen + Redirect ins Dashboard
    const res = NextResponse.redirect(new URL("/dashboard?auth=ok", url));
    res.cookies.set("amz_state", "", {
      path: "/",
      maxAge: 0,
      httpOnly: true,
      sameSite: "lax",
      secure: process.env.NODE_ENV === "production",
    });
    return res;

  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e);
    return NextResponse.json({ ok: false, error: msg }, { status: 500 });
  }
}

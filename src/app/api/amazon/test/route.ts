import { NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";
import { SellingPartner } from "amazon-sp-api";

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

export async function GET() {
  try {
    const { data: row, error } = await supa()
      .from("amazon_connections")
      .select("id, refresh_token, region")
      .eq("region", "eu")
      .order("created_at", { ascending: false })
      .limit(1)
      .maybeSingle();

    if (error) throw new Error(`DB: ${error.message}`);
    if (!row) {
      return NextResponse.json(
        { error: "Kein Refresh-Token in DB gefunden (region=eu)." },
        { status: 400 }
      );
    }

    const sp = new SellingPartner({
      region: "eu",
      refresh_token: row.refresh_token,
      credentials: {
        SELLING_PARTNER_APP_CLIENT_ID: must("LWA_CLIENT_ID"),
        SELLING_PARTNER_APP_CLIENT_SECRET: must("LWA_CLIENT_SECRET"),
        AWS_ACCESS_KEY_ID: must("AWS_ACCESS_KEY_ID"),
        AWS_SECRET_ACCESS_KEY: must("AWS_SECRET_ACCESS_KEY"),
        AWS_SELLING_PARTNER_ROLE: must("AWS_SELLING_PARTNER_ROLE_ARN"),
      },
    });

    const res = await sp.callAPI({
      operation: "getMarketplaceParticipations",
      endpoint: "sellers",
    });

    return NextResponse.json({ ok: true, marketplaces: res });
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e);
    return NextResponse.json({ ok: false, error: msg }, { status: 500 });
  }
}

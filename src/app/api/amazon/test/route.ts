import { NextRequest, NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";
import { SellingPartner } from "amazon-sp-api";

function supa() {
  return createClient(
    process.env.SUPABASE_URL!,
    process.env.SUPABASE_SERVICE_ROLE_KEY!,
    { auth: { persistSession: false } }
  );
}

export async function GET(_req: NextRequest) {
  // 1) Refresh-Token aus DB holen (Region EU)
  const { data: row, error } = await supa()
    .from("amazon_connections")
    .select("id, region, refresh_token")
    .eq("region", "eu")
    .limit(1)
    .maybeSingle();

  if (error || !row) {
    return NextResponse.json({ error: "Kein Token in DB gefunden", detail: error?.message }, { status: 400 });
  }

  // 2) SP-API Client mit Refresh-Token bauen
  const sp = new SellingPartner({
    region: "eu",
    refresh_token: row.refresh_token,
    credentials: {
      SELLING_PARTNER_APP_CLIENT_ID: process.env.LWA_CLIENT_ID!,
      SELLING_PARTNER_APP_CLIENT_SECRET: process.env.LWA_CLIENT_SECRET!,
      AWS_ACCESS_KEY_ID: process.env.AWS_ACCESS_KEY_ID!,
      AWS_SECRET_ACCESS_KEY: process.env.AWS_SECRET_ACCESS_KEY!,
      AWS_SELLING_PARTNER_ROLE: process.env.AWS_SELLING_PARTNER_ROLE_ARN!,
    },
  });

  try {
    // 3) Leichter Test-Call
    const res = await sp.callAPI({
      operation: "getMarketplaceParticipations",
      endpoint: "sellers",
    });

    // Optional: sellerId + marketplaces in DB speichern
    await supa().from("amazon_connections")
      .update({ marketplaces: res })
      .eq("id", row.id);

    return NextResponse.json({ ok: true, marketplaces: res });
  } catch (e: any) {
    return NextResponse.json({ error: "API-Call fehlgeschlagen", detail: e?.message ?? String(e) }, { status: 500 });
  }
}

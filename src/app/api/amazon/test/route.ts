// src/app/api/amazon/test/route.ts
import { NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";

export const runtime = "nodejs";        // Node-Runtime (nicht Edge)
export const dynamic = "force-dynamic"; // immer serverseitig ausführen

// --- Helpers ---
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

// Die Typings von 'amazon-sp-api' sind nicht als konstruierbarer Default exportiert.
// Wir nutzen require + einen schmalen Konstruktor-Typ und deaktivieren NUR diese eine Lint-Regel.
type SPClient = {
  callAPI(args: {
    endpoint: string;
    operation: string;
    path?: string;
    query?: Record<string, unknown>;
    body?: unknown;
  }): Promise<unknown>;
};
type SPConstructor = new (opts: Record<string, unknown>) => SPClient;

// eslint-disable-next-line @typescript-eslint/no-require-imports
const SellingPartner = require("amazon-sp-api") as unknown as SPConstructor;

// GET /api/amazon/test
export async function GET() {
  try {
    // 1) Refresh-Token (EU) aus DB holen
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
        { ok: false, error: "Kein Refresh-Token in DB (region=eu) gefunden." },
        { status: 400 }
      );
    }

    // 2) Credentials zusammenstellen
    const credentials = {
      SELLING_PARTNER_APP_CLIENT_ID: must("LWA_CLIENT_ID"),
      SELLING_PARTNER_APP_CLIENT_SECRET: must("LWA_CLIENT_SECRET"),
      AWS_ACCESS_KEY_ID: must("AWS_ACCESS_KEY_ID"),
      AWS_SECRET_ACCESS_KEY: must("AWS_SECRET_ACCESS_KEY"),
      AWS_SELLING_PARTNER_ROLE: must("AWS_SELLING_PARTNER_ROLE_ARN"),
    } as const;

    // 3) SP-API Client erzeugen
    const sp = new SellingPartner({
      region: "eu",
      refresh_token: row.refresh_token,
      credentials: credentials as unknown as Record<string, string>,
    });

    // 4) Sanfter Test-Call
    const marketplaces = await sp.callAPI({
      endpoint: "sellers",
      operation: "getMarketplaceParticipations",
    });

    // Optional: Ergebnis cachen
    await supa()
      .from("amazon_connections")
      .update({ marketplaces })
      .eq("id", row.id);

    return NextResponse.json({ ok: true, marketplaces });
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e);
    return NextResponse.json({ ok: false, error: msg }, { status: 500 });
  }
}

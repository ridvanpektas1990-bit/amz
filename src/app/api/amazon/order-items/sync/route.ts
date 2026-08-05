import { NextRequest, NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";
import { tenantIdFromRequest } from "@/lib/amazon-tenant-cookie";
import { loadAmazonConnection } from "@/lib/amazon-connection";
import { syncRecentOrderItems } from "@/lib/amazon-order-items-sync";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export const maxDuration = 300;

function must(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing env ${name}`);
  return value;
}

export async function POST(req: NextRequest) {
  try {
    const tenantId = tenantIdFromRequest(req);
    if (!tenantId) {
      return NextResponse.json({ ok: false, error: "not_connected" }, { status: 401 });
    }

    const url = new URL(req.url);
    const days = Math.min(45, Math.max(1, Number(url.searchParams.get("days") || 21)));
    const maxOrders = Math.min(800, Math.max(1, Number(url.searchParams.get("maxOrders") || 400)));
    const marketplace = (url.searchParams.get("marketplace") || "DE").toUpperCase();
    const region = (url.searchParams.get("region") || "eu").toLowerCase() as "eu" | "na" | "fe";

    const connectionResult = await loadAmazonConnection(req, region);
    if (!connectionResult.connection) {
      return NextResponse.json({ ok: false, error: connectionResult.error }, { status: 401 });
    }

    const sb = createClient(must("SUPABASE_URL"), must("SUPABASE_SERVICE_ROLE_KEY"), {
      auth: { persistSession: false },
    });

    const result = await syncRecentOrderItems({
      sb,
      tenantId,
      marketplace,
      region,
      refreshToken: connectionResult.connection.refresh_token,
      days,
      maxOrders,
      paceMs: 700,
    });

    return NextResponse.json({ ok: true, days, ...result }, { headers: { "cache-control": "no-store" } });
  } catch (error: unknown) {
    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : String(error) },
      { status: 500 },
    );
  }
}

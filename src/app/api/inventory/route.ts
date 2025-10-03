import { NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// GET /api/inventory?sku=ABC123
export async function GET(req: Request) {
  try {
    const { searchParams } = new URL(req.url);
    const sku = (searchParams.get("sku") || "").trim();
    if (!sku) {
      return NextResponse.json({ ok: false, error: "sku required" }, { status: 400 });
    }

    const { data, error } = await supabase
      .from("vw_inventory_latest_per_asin_max")
      .select("inventory_left")
      .eq("seller_sku", sku)
      .maybeSingle();

    if (error) {
      return NextResponse.json({ ok: false, error: error.message }, { status: 500 });
    }

    const inventory_left = data?.inventory_left ?? null;
    return NextResponse.json({ ok: true, sku, inventory_left }, { headers: { "cache-control": "no-store" } });
  } catch (e: any) {
    return NextResponse.json({ ok: false, error: e?.message ?? "unknown error" }, { status: 500 });
  }
}

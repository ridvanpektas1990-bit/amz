import { NextResponse } from "next/server";
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

export async function GET(req: Request) {
  try {
    const url = new URL(req.url);

    const table   = (url.searchParams.get("table")    ?? "vw_amazon_fees_orders").toLowerCase();
    const skuCol  = (url.searchParams.get("sku_col")  ?? "seller_sku").toLowerCase();
    const dateCol = (url.searchParams.get("date_col") ?? "").toLowerCase();
    const startIso = url.searchParams.get("start") ?? null;
    const endIso   = url.searchParams.get("end")   ?? null;

    const sb = supa();

    const PAGE = 1000;
    let from = 0;
    let to = PAGE - 1;

    // Map: normalizedKey -> rawValue (erstes Vorkommen)
    const map = new Map<string, string>();

    for (;;) {
      let sel = skuCol;
      if (dateCol && startIso && endIso) sel = `${skuCol}, ${dateCol}`;

      let q = sb.from(table).select(sel);
      if (dateCol && startIso && endIso) q = q.gte(dateCol, startIso).lte(dateCol, endIso);

      const { data, error } = await q.range(from, to);
      if (error) throw new Error(`SKU page ${from}-${to}: ${error.message}`);
      if (!data || data.length === 0) break;

      for (const row of data as any[]) {
        const raw: string = String(row[skuCol] ?? "");
        if (!raw) continue;
        const norm = raw.trim().toUpperCase();
        if (!map.has(norm)) map.set(norm, raw); // rohen Wert behalten!
      }

      if (data.length < PAGE) break;
      from += PAGE;
      to += PAGE;
    }

    const skus = Array.from(map.values())
      .map(v => ({ value: v, label: v.trim() })) // value=roh, label=schön
      .sort((a, b) => a.label.localeCompare(b.label, "en"));

    return NextResponse.json({ ok: true, skus });
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e);
    return NextResponse.json({ ok: false, error: msg }, { status: 500 });
  }
}

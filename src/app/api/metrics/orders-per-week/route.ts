import { NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

/* === Helpers === */
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

/** Robustes UTC-Parsing für:
 *  - 2025-02-10T12:34:56Z
 *  - 2025-02-10 12:34:56
 *  - 2025-02-10
 */
function parseUTC(s: unknown): Date | null {
  if (!s) return null;
  let t = String(s).trim();
  if (!t) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(t)) t += "T00:00:00Z";
  else if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/.test(t)) {
    t = t.replace(" ", "T");
    if (!/[zZ]|[+-]\d{2}:\d{2}$/.test(t)) t += "Z";
  } else if (t.includes("T") && !/[zZ]|[+-]\d{2}:\d{2}$/.test(t)) {
    t += "Z";
  }
  const d = new Date(t);
  return isNaN(d.getTime()) ? null : d;
}

// ISO-Woche (UTC, Montag=1)
function getISOYearWeek(d: Date): { year: number; week: number } {
  const date = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
  const day = date.getUTCDay() || 7;            // 1..7 (Mo..So)
  date.setUTCDate(date.getUTCDate() + 4 - day); // Do der ISO-Woche
  const year = date.getUTCFullYear();
  const yearStart = new Date(Date.UTC(year, 0, 1));
  const week = Math.ceil((((date.getTime() - yearStart.getTime()) / 86400000) + 1) / 7);
  return { year, week };
}

// Montag/Sonntag (UTC) für ISO-Jahr/Woche
function isoWeekStartEndUTC(year: number, week: number): { start: Date; end: Date } {
  const jan4 = new Date(Date.UTC(year, 0, 4));
  const day = jan4.getUTCDay() || 7;
  const mondayW1 = new Date(jan4);
  mondayW1.setUTCDate(jan4.getUTCDate() - (day - 1));
  const start = new Date(mondayW1);
  start.setUTCDate(mondayW1.getUTCDate() + (week - 1) * 7);
  const end = new Date(start);
  end.setUTCDate(start.getUTCDate() + 6);
  return { start, end };
}

/* === Route === */
export async function GET(req: Request) {
  try {
    const url = new URL(req.url);
    const year = Number(url.searchParams.get("year") ?? "2025");
    const fixed = url.searchParams.get("fixed") === "1";
    const debug = url.searchParams.get("debug") === "1";
    const sku = (url.searchParams.get("sku") || "").trim();

    const sb = supa();

    // Fenster um das Jahr herum (Berlin-Wochenränder sicher drin)
    const startStr = `${year - 1}-12-29`; // 29.12.Vorjahr
    const endStr   = `${year + 1}-01-05`; // 05.01.Folgejahr

    // --- Pagination aus dem VIEW: purchase_date_berlin (date) + quantity ---
    const PAGE = 1000;
    let from = 0;
    let to = PAGE - 1;
    const rows: Array<{ purchase_date_berlin: string; quantity: number; seller_sku?: string }> = [];

    for (;;) {
      let q = sb
        .from("vw_amazon_fees_orders")
        .select("purchase_date_berlin, quantity, seller_sku")
        .gte("purchase_date_berlin", startStr)
        .lte("purchase_date_berlin", endStr);

      if (sku) q = q.eq("seller_sku", sku);

      const { data, error } = await q.range(from, to);
      if (error) throw new Error(`DB page ${from}-${to}: ${error.message}`);
      if (!data || data.length === 0) break;

      rows.push(...(data as any[]));
      if (data.length < PAGE) break;
      from += PAGE;
      to += PAGE;
    }

    // --- Aggregation (ISO-Wochenlogik) ---
    const bucket = new Map<number, number>(); // week -> sum
    const byMonth = Array.from({ length: 12 }, () => 0);
    let usedRows = 0;
    let sumTotal = 0;

    for (const row of rows) {
      const d = parseUTC(row.purchase_date_berlin);   // "YYYY-MM-DD" → Mitternacht UTC
      const qty = Number(row.quantity ?? 0);
      if (!d || !isFinite(qty)) continue;

      const { year: isoYear, week } = getISOYearWeek(d);
      if (isoYear !== year) continue;

      usedRows++;
      bucket.set(week, (bucket.get(week) ?? 0) + qty);
      sumTotal += qty;
      byMonth[d.getUTCMonth()] += qty; // 0..11
    }

    // fix 52 Wochen (KW1..KW52)
    const weeks = fixed ? 52 : 52;
    const points = Array.from({ length: weeks }, (_, i) => {
      const wk = i + 1;
      const { start, end } = isoWeekStartEndUTC(year, wk);
      const endEnd = new Date(Date.UTC(end.getUTCFullYear(), end.getUTCMonth(), end.getUTCDate(), 23, 59, 59, 999));
      return {
        key: `${year}-W${wk.toString().padStart(2, "0")}`,
        label: `KW ${wk}`,
        isoYear: year,
        isoWeek: wk,
        startUtc: start.toISOString(),
        endUtc: endEnd.toISOString(),
        total: bucket.get(wk) ?? 0,
      };
    });

    return NextResponse.json({
      ok: true,
      year,
      points,
      meta: debug ? {
        fetchedRows: rows.length,
        usedRows,
        sumTotal,
        monthTotals: {
          jan: byMonth[0], feb: byMonth[1], mar: byMonth[2], apr: byMonth[3],
          mai: byMonth[4], jun: byMonth[5], jul: byMonth[6], aug: byMonth[7],
          sep: byMonth[8], okt: byMonth[9], nov: byMonth[10], dez: byMonth[11],
        }
      } : undefined,
    });
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e);
    return NextResponse.json({ ok: false, error: msg }, { status: 500 });
  }
}

import { NextRequest, NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";
import { tenantIdFromRequest } from "@/lib/amazon-tenant-cookie";
import { loadOrderUnitRows } from "@/lib/amazon-order-units";
import {
  dailyUnitsSeriesFromMap,
  recentSalesTempoFromDaily,
  RECENT_TEMPO_LOOKBACK_DAYS,
} from "@/lib/recent-sales-tempo";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function must(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env ${name}`);
  return v;
}
function supa() {
  return createClient(must("SUPABASE_URL"), must("SUPABASE_SERVICE_ROLE_KEY"), {
    auth: { persistSession: false },
  });
}

function getISOYearWeek(d: Date): { year: number; week: number } {
  const date = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
  const day = date.getUTCDay() || 7;
  date.setUTCDate(date.getUTCDate() + 4 - day);
  const year = date.getUTCFullYear();
  const yearStart = new Date(Date.UTC(year, 0, 1));
  const week = Math.ceil((((date.getTime() - yearStart.getTime()) / 86400000) + 1) / 7);
  return { year, week };
}

function isoWeeksInYear(year: number): number {
  return getISOYearWeek(new Date(Date.UTC(year, 11, 28))).week;
}

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

export async function GET(req: NextRequest) {
  try {
    const tenantId = tenantIdFromRequest(req);
    if (!tenantId) {
      return NextResponse.json({ ok: false, error: "not_connected" }, { status: 401 });
    }
    const url = new URL(req.url);
    const year = Number(url.searchParams.get("year") ?? new Date().getUTCFullYear());
    const debug = url.searchParams.get("debug") === "1";
    const sku = (url.searchParams.get("sku") || "").trim();
    const todayISO = new Intl.DateTimeFormat("en-CA", {
      timeZone: "Europe/Berlin",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    }).format(new Date());
    const recentStartDate = new Date(`${todayISO}T12:00:00Z`);
    recentStartDate.setUTCDate(recentStartDate.getUTCDate() - (RECENT_TEMPO_LOOKBACK_DAYS - 1));
    const recentStart = recentStartDate.toISOString().slice(0, 10);

    const sb = supa();
    const startStr = `${year - 1}-12-29`;
    const endStr = `${year + 1}-01-05`;

    const unitRows = await loadOrderUnitRows({
      sb,
      tenantId,
      startISO: startStr,
      endISO: endStr,
      sellerSku: sku || null,
    });

    const bucket = new Map<number, number>();
    const byMonth = Array.from({ length: 12 }, () => 0);
    const recentByDate = new Map<string, number>();
    let usedRows = 0;
    let sumTotal = 0;

    for (const row of unitRows) {
      if (row.dateISO >= recentStart && row.dateISO <= todayISO) {
        recentByDate.set(row.dateISO, (recentByDate.get(row.dateISO) || 0) + row.quantity);
      }
      if (row.isoYear !== year) continue;
      usedRows += 1;
      bucket.set(row.isoWeek, (bucket.get(row.isoWeek) ?? 0) + row.quantity);
      sumTotal += row.quantity;
      const monthIdx = Number(row.dateISO.slice(5, 7)) - 1;
      if (monthIdx >= 0 && monthIdx < 12) byMonth[monthIdx] += row.quantity;
    }

    const recentSeries = dailyUnitsSeriesFromMap(recentStart, todayISO, recentByDate);
    const recentTempo = recentSalesTempoFromDaily(recentSeries);
    const recent30Units = recentTempo.units;

    const weeks = isoWeeksInYear(year);
    const points = Array.from({ length: weeks }, (_, i) => {
      const wk = i + 1;
      const { start, end } = isoWeekStartEndUTC(year, wk);
      const endEnd = new Date(
        Date.UTC(end.getUTCFullYear(), end.getUTCMonth(), end.getUTCDate(), 23, 59, 59, 999),
      );
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
      source: sku ? "amazon_order_items" : "amazon_orders",
      recent30Units,
      recentTempoDays: recentTempo.activeDays,
      recentTempoTruncated: recentTempo.truncated,
      recentDailyRate: Number(recentTempo.dailyRate.toFixed(4)),
      recent30Window: { start: recentStart, end: todayISO, days: RECENT_TEMPO_LOOKBACK_DAYS },
      meta: debug
        ? {
            fetchedRows: unitRows.length,
            usedRows,
            sumTotal,
            recentTempo,
            monthTotals: {
              jan: byMonth[0],
              feb: byMonth[1],
              mar: byMonth[2],
              apr: byMonth[3],
              mai: byMonth[4],
              jun: byMonth[5],
              jul: byMonth[6],
              aug: byMonth[7],
              sep: byMonth[8],
              okt: byMonth[9],
              nov: byMonth[10],
              dez: byMonth[11],
            },
          }
        : undefined,
    });
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e);
    return NextResponse.json({ ok: false, error: msg }, { status: 500 });
  }
}

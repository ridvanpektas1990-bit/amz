/**
 * Bulk backfill amazon_order_items via Amazon flat-file order reports.
 *
 * Usage:
 *   node --env-file=.env.local --experimental-strip-types scripts/backfill-order-items-via-report.mjs
 *
 * Optional:
 *   BACKFILL_START=2023-10-01
 *   BACKFILL_END=2026-08-05
 */
import { createClient } from "@supabase/supabase-js";
import { syncOrderItemsViaReports } from "../src/lib/amazon-order-items-report.ts";

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
if (!url || !key) throw new Error("Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY");

const sb = createClient(url, key, { auth: { persistSession: false } });

const { data: orderRow, error: tenantError } = await sb
  .from("amazon_orders")
  .select("tenant_id")
  .limit(1)
  .maybeSingle();
if (tenantError || !orderRow?.tenant_id) throw new Error("No tenant found");
const tenantId = orderRow.tenant_id;

const { data: conn, error: connError } = await sb
  .from("amazon_connections")
  .select("refresh_token, region")
  .eq("tenant_id", tenantId)
  .eq("region", "eu")
  .maybeSingle();
if (connError || !conn?.refresh_token) throw new Error("No amazon_connections refresh_token");

const startISO = (process.env.BACKFILL_START || "2023-10-01").slice(0, 10);
const endISO = (process.env.BACKFILL_END || new Date().toISOString().slice(0, 10)).slice(0, 10);

console.log(
  JSON.stringify({
    phase: "start",
    mode: "reports",
    reportType: "GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL",
    tenantId,
    startISO,
    endISO,
    at: new Date().toISOString(),
  }),
);

const result = await syncOrderItemsViaReports({
  sb,
  tenantId,
  marketplace: "DE",
  region: (conn.region || "eu").toLowerCase(),
  refreshToken: conn.refresh_token,
  startISO,
  endISO,
});

const { count } = await sb
  .from("amazon_order_items")
  .select("*", { count: "exact", head: true })
  .eq("tenant_id", tenantId);

console.log(
  JSON.stringify({
    phase: "done",
    ...result,
    errorCount: result.errors.length,
    sampleErrors: result.errors.slice(0, 5),
    itemsTotal: count,
    at: new Date().toISOString(),
  }),
);

if (result.errors.length) process.exitCode = 2;

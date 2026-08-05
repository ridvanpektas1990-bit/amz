/**
 * Bulk-load amazon_order_items via SP-API Reports
 * (GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL).
 * One report ≈ thousands of lines; Amazon allows ≤30-day windows.
 */
import { createClient, type SupabaseClient } from "@supabase/supabase-js";
import aws4 from "aws4";
import { gunzipSync } from "zlib";

const HOSTS = {
  eu: "sellingpartnerapi-eu.amazon.com",
  na: "sellingpartnerapi-na.amazon.com",
  fe: "sellingpartnerapi-fe.amazon.com",
} as const;

const AWS_REGION = {
  eu: "eu-west-1",
  na: "us-east-1",
  fe: "us-west-2",
} as const;

const MARKETPLACE_IDS: Record<string, string> = {
  DE: "A1PA6795UKMFR9",
  AT: "A2VIGQ35RCS4UG",
  FR: "A13V1IB3VIYZZH",
  IT: "APJ6JRA9NG5V4",
  ES: "A1RKKUPIHCS9HS",
  NL: "A1805IZSGTT6HS",
  BE: "AMEN7PMS3EDWL",
  PL: "A1C3SOZRARQ6R3",
  SE: "A2NODRKZP88ZB9",
  GB: "A1F83G8C2ARO7P",
};

const REPORT_TYPE = "GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL";

function must(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing env ${name}`);
  return value;
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function berlinDateFromTs(value: string): string {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: "Europe/Berlin",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(new Date(value));
}

/** ISO week + ISO week-year for a Berlin calendar date (YYYY-MM-DD). */
export function isoYearWeekFromBerlinDate(dateISO: string): { isoYear: number; isoWeek: number } {
  const [y, m, d] = dateISO.split("-").map(Number);
  const date = new Date(Date.UTC(y, m - 1, d));
  // Thursday-based ISO week year
  const day = date.getUTCDay() || 7;
  date.setUTCDate(date.getUTCDate() + 4 - day);
  const isoYear = date.getUTCFullYear();
  const yearStart = new Date(Date.UTC(isoYear, 0, 1));
  const isoWeek = Math.ceil(((date.getTime() - yearStart.getTime()) / 86_400_000 + 1) / 7);
  return { isoYear, isoWeek };
}

async function refreshAccessToken(refreshToken: string): Promise<string> {
  const body = new URLSearchParams({
    grant_type: "refresh_token",
    refresh_token: refreshToken,
    client_id: must("LWA_CLIENT_ID"),
    client_secret: must("LWA_CLIENT_SECRET"),
  });
  const response = await fetch("https://api.amazon.com/auth/o2/token", {
    method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded" },
    body,
  });
  if (!response.ok) throw new Error(`LWA refresh failed ${response.status}`);
  const json = (await response.json()) as { access_token: string };
  return json.access_token;
}

async function spApiFetch({
  region,
  accessToken,
  method,
  path,
  body,
}: {
  region: keyof typeof HOSTS;
  accessToken: string;
  method: "GET" | "POST";
  path: string;
  body?: unknown;
}): Promise<unknown> {
  const host = HOSTS[region] ?? HOSTS.eu;
  const headers: Record<string, string> = {
    "x-amz-access-token": accessToken,
    accept: "application/json",
    "user-agent": "amz-profit/1.0",
  };
  let payload: string | undefined;
  if (body !== undefined) {
    payload = JSON.stringify(body);
    headers["content-type"] = "application/json";
  }
  const signed = aws4.sign(
    {
      host,
      path,
      method,
      service: "execute-api",
      region: AWS_REGION[region] ?? AWS_REGION.eu,
      headers,
      body: payload,
    },
    {
      accessKeyId: must("AWS_ACCESS_KEY_ID"),
      secretAccessKey: must("AWS_SECRET_ACCESS_KEY"),
    },
  );
  const response = await fetch(`https://${host}${path}`, {
    method,
    headers: signed.headers as Record<string, string>,
    body: payload,
    cache: "no-store",
  });
  const text = await response.text();
  if (response.status === 429) {
    const err = new Error("throttled") as Error & { status: number };
    err.status = 429;
    throw err;
  }
  if (!response.ok) {
    throw new Error(`SP-API ${method} ${path}: ${response.status} ${text.slice(0, 400)}`);
  }
  return text ? JSON.parse(text) : {};
}

async function withThrottleRetry<T>(fn: () => Promise<T>, label: string): Promise<T> {
  for (let attempt = 0; attempt < 8; attempt++) {
    try {
      return await fn();
    } catch (err: unknown) {
      const status = (err as { status?: number }).status;
      const message = err instanceof Error ? err.message : String(err);
      if (status === 429 || /throttled|QuotaExceeded|429/i.test(message)) {
        const wait = Math.min(60_000, 2000 * 2 ** attempt);
        console.log(JSON.stringify({ phase: "retry", label, attempt: attempt + 1, waitMs: wait }));
        await sleep(wait);
        continue;
      }
      throw err;
    }
  }
  throw new Error(`${label}: too many retries`);
}

function parseTsv(text: string): Record<string, string>[] {
  const lines = text.replace(/^\uFEFF/, "").split(/\r?\n/).filter((line) => line.length > 0);
  if (lines.length < 2) return [];
  const headers = lines[0].split("\t").map((h) => h.trim());
  const rows: Record<string, string>[] = [];
  for (let i = 1; i < lines.length; i++) {
    const cols = lines[i].split("\t");
    const row: Record<string, string> = {};
    for (let c = 0; c < headers.length; c++) {
      row[headers[c]] = cols[c] ?? "";
    }
    rows.push(row);
  }
  return rows;
}

function addDaysISO(dateISO: string, days: number): string {
  const d = new Date(`${dateISO}T12:00:00Z`);
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}

/** Inclusive start/end Berlin calendar dates, split into ≤30-day windows (newest first). */
export function reportWindows(startISO: string, endISO: string, maxDays = 30): Array<[string, string]> {
  const windows: Array<[string, string]> = [];
  let end = endISO;
  while (end >= startISO) {
    let start = addDaysISO(end, -(maxDays - 1));
    if (start < startISO) start = startISO;
    windows.push([start, end]);
    end = addDaysISO(start, -1);
  }
  return windows;
}

export type ReportItemsSyncResult = {
  windows: number;
  reportRows: number;
  upserted: number;
  skipped: number;
  errors: string[];
};

export async function syncOrderItemsViaReports({
  sb,
  tenantId,
  marketplace = "DE",
  region = "eu",
  refreshToken,
  startISO,
  endISO,
  pollMs = 15_000,
  maxPolls = 80,
}: {
  sb: SupabaseClient;
  tenantId: string;
  marketplace?: string;
  region?: keyof typeof HOSTS;
  refreshToken: string;
  startISO: string;
  endISO: string;
  pollMs?: number;
  maxPolls?: number;
}): Promise<ReportItemsSyncResult> {
  const marketplaceId = MARKETPLACE_IDS[marketplace.toUpperCase()];
  if (!marketplaceId) throw new Error(`Unsupported marketplace ${marketplace}`);

  const windows = reportWindows(startISO, endISO, 30);
  const result: ReportItemsSyncResult = {
    windows: windows.length,
    reportRows: 0,
    upserted: 0,
    skipped: 0,
    errors: [],
  };

  let accessToken = await refreshAccessToken(refreshToken);

  for (const [winStart, winEnd] of windows) {
    console.log(
      JSON.stringify({
        phase: "window_start",
        range: [winStart, winEnd],
        at: new Date().toISOString(),
      }),
    );

    try {
      accessToken = await refreshAccessToken(refreshToken);
      const createJson = (await withThrottleRetry(
        () =>
          spApiFetch({
            region,
            accessToken,
            method: "POST",
            path: "/reports/2021-06-30/reports",
            body: {
              reportType: REPORT_TYPE,
              marketplaceIds: [marketplaceId],
              dataStartTime: `${winStart}T00:00:00.000Z`,
              dataEndTime: `${winEnd}T23:59:59.999Z`,
            },
          }),
        `createReport ${winStart}`,
      )) as { reportId?: string };

      const reportId = createJson.reportId;
      if (!reportId) throw new Error(`No reportId for ${winStart}..${winEnd}`);

      let documentId: string | null = null;
      for (let poll = 0; poll < maxPolls; poll++) {
        const statusJson = (await withThrottleRetry(
          () =>
            spApiFetch({
              region,
              accessToken,
              method: "GET",
              path: `/reports/2021-06-30/reports/${encodeURIComponent(reportId)}`,
            }),
          `getReport ${reportId}`,
        )) as {
          processingStatus?: string;
          reportDocumentId?: string;
        };
        const status = String(statusJson.processingStatus || "");
        if (status === "DONE") {
          documentId = statusJson.reportDocumentId || null;
          break;
        }
        if (status === "CANCELLED" || status === "FATAL") {
          throw new Error(`Report ${reportId} ${status}`);
        }
        await sleep(pollMs);
      }
      if (!documentId) throw new Error(`Report ${reportId} timed out`);

      const docJson = (await withThrottleRetry(
        () =>
          spApiFetch({
            region,
            accessToken,
            method: "GET",
            path: `/reports/2021-06-30/documents/${encodeURIComponent(documentId)}`,
          }),
        `getDocument ${documentId}`,
      )) as { url?: string; compressionAlgorithm?: string };

      if (!docJson.url) throw new Error(`No document URL for ${documentId}`);
      const docRes = await fetch(docJson.url);
      if (!docRes.ok) throw new Error(`Download failed ${docRes.status}`);
      const buf = Buffer.from(await docRes.arrayBuffer());
      const text =
        String(docJson.compressionAlgorithm || "").toUpperCase() === "GZIP"
          ? gunzipSync(buf).toString("utf8")
          : buf.toString("utf8");

      const rows = parseTsv(text);
      result.reportRows += rows.length;
      const upserts: Record<string, unknown>[] = [];

      for (const row of rows) {
        const orderId = String(row["amazon-order-id"] || "").trim();
        const orderItemId = String(row["order-item-id"] || "").trim();
        const orderStatus = String(row["order-status"] || "").trim().toLowerCase();
        if (!orderId || !orderItemId) {
          result.skipped += 1;
          continue;
        }
        if (orderStatus === "cancelled" || orderStatus === "canceled") {
          result.skipped += 1;
          continue;
        }

        const purchaseRaw = String(row["purchase-date"] || "").trim();
        if (!purchaseRaw) {
          result.skipped += 1;
          continue;
        }
        const purchaseDate = new Date(purchaseRaw);
        if (!Number.isFinite(purchaseDate.getTime())) {
          result.skipped += 1;
          continue;
        }
        const berlin = berlinDateFromTs(purchaseDate.toISOString());
        const { isoYear, isoWeek } = isoYearWeekFromBerlinDate(berlin);
        const qty = Math.max(0, Number(row.quantity) || 0);
        const itemStatus = String(row["item-status"] || "").trim().toLowerCase();
        // Logistics cares about units sold; treat report quantity as ordered.
        const shipped = itemStatus === "unshipped" ? 0 : qty;

        upserts.push({
          tenant_id: tenantId,
          marketplace: marketplace.toUpperCase(),
          amazon_order_id: orderId,
          order_item_id: orderItemId,
          seller_sku: row.sku ? String(row.sku).trim() : null,
          asin: row.asin ? String(row.asin).trim() : null,
          title: row["product-name"] ? String(row["product-name"]).trim() : null,
          quantity_ordered: qty,
          quantity_shipped: shipped,
          purchase_date_local: purchaseDate.toISOString(),
          purchase_date_berlin: berlin,
          iso_year: isoYear,
          iso_week: isoWeek,
          updated_at: new Date().toISOString(),
        });
      }

      for (let i = 0; i < upserts.length; i += 200) {
        const chunk = upserts.slice(i, i + 200);
        const { error } = await sb.from("amazon_order_items").upsert(chunk, {
          onConflict: "tenant_id,marketplace,amazon_order_id,order_item_id",
        });
        if (error) throw new Error(`Upsert: ${error.message}`);
        result.upserted += chunk.length;
      }

      console.log(
        JSON.stringify({
          phase: "window_done",
          range: [winStart, winEnd],
          reportRows: rows.length,
          upserted: upserts.length,
          at: new Date().toISOString(),
        }),
      );
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      result.errors.push(`${winStart}..${winEnd}: ${message}`);
      console.log(
        JSON.stringify({
          phase: "window_error",
          range: [winStart, winEnd],
          error: message,
          at: new Date().toISOString(),
        }),
      );
    }

    // Gentle pacing between report creates
    await sleep(2000);
  }

  return result;
}

export function createServiceSupabase() {
  return createClient(must("SUPABASE_URL"), must("SUPABASE_SERVICE_ROLE_KEY"), {
    auth: { persistSession: false },
  });
}

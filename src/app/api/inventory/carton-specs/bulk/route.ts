import { NextRequest, NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";
import { tenantIdFromRequest } from "@/lib/amazon-tenant-cookie";
import {
  DEFAULT_AMAZON_TARGET_COVER_DAYS,
  DEFAULT_TRANSFER_LEAD_DAYS,
} from "@/lib/local-stock";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function must(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing env ${name}`);
  return value;
}

function supabase() {
  return createClient(must("SUPABASE_URL"), must("SUPABASE_SERVICE_ROLE_KEY"), {
    auth: { persistSession: false },
  });
}

function positiveIntOrNull(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  const i = Math.round(n);
  return i >= 0 ? i : null;
}

async function ensureProductStubs(
  sb: ReturnType<typeof supabase>,
  tenantId: string,
  skus: string[],
) {
  if (!skus.length) return;
  const { data: existing } = await sb
    .from("inventory_products")
    .select("seller_sku")
    .eq("tenant_id", tenantId)
    .in("seller_sku", skus);
  const have = new Set((existing || []).map((row) => String((row as { seller_sku: string }).seller_sku)));
  const missing = skus.filter((sku) => !have.has(sku));
  if (!missing.length) return;
  const { error } = await sb.from("inventory_products").insert(
    missing.map((seller_sku) => ({ tenant_id: tenantId, seller_sku })),
  );
  if (error) throw new Error(`Products bulk: ${error.message}`);
}

/**
 * Apply universal Stammdaten defaults to inventory SKUs.
 * Body (all fields optional; at least one required):
 * {
 *   transferLeadDays?: number,
 *   amazonTargetCoverDays?: number,
 *   productionTimeDays?: number,
 *   shippingTimeDays?: number,
 *   bufferTimeDays?: number,
 *   sellerSkus?: string[]
 * }
 */
export async function POST(req: NextRequest) {
  try {
    const tenantId = tenantIdFromRequest(req);
    if (!tenantId) {
      return NextResponse.json({ ok: false, error: "not_connected" }, { status: 401 });
    }

    const body = await req.json().catch(() => ({}));
    const transferLeadDays = positiveIntOrNull(body?.transferLeadDays ?? body?.transfer_lead_days);
    const amazonTargetRaw = positiveIntOrNull(
      body?.amazonTargetCoverDays ?? body?.amazon_target_cover_days,
    );
    const amazonTargetCoverDays =
      amazonTargetRaw != null ? Math.max(1, amazonTargetRaw) : null;
    const productionTimeDays = positiveIntOrNull(
      body?.productionTimeDays ?? body?.production_time_days,
    );
    const shippingTimeDays = positiveIntOrNull(body?.shippingTimeDays ?? body?.shipping_time_days);
    const bufferTimeDays = positiveIntOrNull(body?.bufferTimeDays ?? body?.buffer_time_days);

    const applyLocal =
      transferLeadDays != null || amazonTargetCoverDays != null;
    const applyLead =
      productionTimeDays != null || shippingTimeDays != null || bufferTimeDays != null;

    if (!applyLocal && !applyLead) {
      return NextResponse.json(
        {
          ok: false,
          error:
            "Mindestens eines von transferLeadDays, amazonTargetCoverDays, productionTimeDays, shippingTimeDays, bufferTimeDays angeben",
        },
        { status: 400 },
      );
    }

    const sb = supabase();
    const { data: inventoryData, error: inventoryError } = await sb
      .from("vw_inventory_latest_per_asin_max")
      .select("seller_sku")
      .eq("tenant_id", tenantId)
      .eq("marketplace", "DE");
    if (inventoryError) throw new Error(`Inventory: ${inventoryError.message}`);

    const inventorySkus = [
      ...new Set(
        (inventoryData || [])
          .map((row) => String((row as { seller_sku: string | null }).seller_sku || "").trim())
          .filter(Boolean),
      ),
    ];

    const requestedSkus = Array.isArray(body?.sellerSkus)
      ? [
          ...new Set(
            (body.sellerSkus as unknown[])
              .map((sku) => String(sku || "").trim())
              .filter(Boolean),
          ),
        ]
      : null;

    const inventorySet = new Set(inventorySkus);
    const skus = (requestedSkus ?? inventorySkus).filter((sku) => inventorySet.has(sku));

    if (!skus.length) {
      return NextResponse.json({ ok: true, updated: 0, skus: 0 });
    }

    await ensureProductStubs(sb, tenantId, skus);
    const now = new Date().toISOString();
    let updatedLocal = 0;
    let updatedSpecs = 0;

    if (applyLocal) {
      const { data: localRows } = await sb
        .from("inventory_local_stock")
        .select(
          "seller_sku,local_qty,on_order_units,on_order_ordered_at,last_inbound_seen,transfer_lead_days,amazon_target_cover_days",
        )
        .eq("tenant_id", tenantId)
        .in("seller_sku", skus);

      const localBySku = new Map(
        (localRows || []).map((row) => [
          String((row as { seller_sku: string }).seller_sku),
          row as {
            seller_sku: string;
            local_qty: number | null;
            on_order_units: number | null;
            on_order_ordered_at: string | null;
            last_inbound_seen: number | null;
            transfer_lead_days: number | null;
            amazon_target_cover_days: number | null;
          },
        ]),
      );

      const payloads = skus.map((seller_sku) => {
        const existing = localBySku.get(seller_sku);
        return {
          tenant_id: tenantId,
          seller_sku,
          local_qty: Math.max(0, Math.floor(Number(existing?.local_qty) || 0)),
          on_order_units: Math.max(0, Math.floor(Number(existing?.on_order_units) || 0)),
          transfer_lead_days:
            transferLeadDays != null
              ? Math.max(0, transferLeadDays)
              : Math.max(
                  0,
                  Math.round(Number(existing?.transfer_lead_days) || DEFAULT_TRANSFER_LEAD_DAYS) ||
                    DEFAULT_TRANSFER_LEAD_DAYS,
                ),
          amazon_target_cover_days:
            amazonTargetCoverDays != null
              ? amazonTargetCoverDays
              : Math.max(
                  1,
                  Math.round(
                    Number(existing?.amazon_target_cover_days) || DEFAULT_AMAZON_TARGET_COVER_DAYS,
                  ) || DEFAULT_AMAZON_TARGET_COVER_DAYS,
                ),
          on_order_ordered_at: existing?.on_order_ordered_at ?? null,
          last_inbound_seen: existing?.last_inbound_seen ?? null,
          updated_at: now,
        };
      });

      for (let i = 0; i < payloads.length; i += 100) {
        const chunk = payloads.slice(i, i + 100);
        const { error } = await sb
          .from("inventory_local_stock")
          .upsert(chunk, { onConflict: "tenant_id,seller_sku" });
        if (error) throw new Error(`Local stock bulk: ${error.message}`);
        updatedLocal += chunk.length;
      }
    }

    if (applyLead) {
      const { data: specRows } = await sb
        .from("inventory_carton_specs")
        .select(
          "seller_sku,units_per_carton,carton_len_cm,carton_w_cm,carton_h_cm,carton_weight_kg,production_time_days,shipping_time_days,buffer_time_days",
        )
        .eq("tenant_id", tenantId)
        .in("seller_sku", skus);

      type SpecExisting = {
        seller_sku: string;
        units_per_carton: number | null;
        carton_len_cm: number | null;
        carton_w_cm: number | null;
        carton_h_cm: number | null;
        carton_weight_kg: number | null;
        production_time_days: number | null;
        shipping_time_days: number | null;
        buffer_time_days: number | null;
      };

      const specBySku = new Map(
        (specRows || []).map((row) => [String((row as SpecExisting).seller_sku), row as SpecExisting]),
      );

      const payloads = skus.map((seller_sku) => {
        const existing = specBySku.get(seller_sku);
        return {
          tenant_id: tenantId,
          seller_sku,
          units_per_carton: existing?.units_per_carton != null && existing.units_per_carton > 0
            ? existing.units_per_carton
            : 1,
          carton_len_cm: existing?.carton_len_cm ?? null,
          carton_w_cm: existing?.carton_w_cm ?? null,
          carton_h_cm: existing?.carton_h_cm ?? null,
          carton_weight_kg: existing?.carton_weight_kg ?? null,
          production_time_days:
            productionTimeDays != null
              ? productionTimeDays
              : (existing?.production_time_days ?? null),
          shipping_time_days:
            shippingTimeDays != null ? shippingTimeDays : (existing?.shipping_time_days ?? null),
          buffer_time_days:
            bufferTimeDays != null ? bufferTimeDays : (existing?.buffer_time_days ?? null),
          updated_at: now,
        };
      });

      for (let i = 0; i < payloads.length; i += 100) {
        const chunk = payloads.slice(i, i + 100);
        const { error } = await sb
          .from("inventory_carton_specs")
          .upsert(chunk, { onConflict: "tenant_id,seller_sku" });
        if (error) throw new Error(`Carton specs bulk: ${error.message}`);
        updatedSpecs += chunk.length;
      }
    }

    return NextResponse.json(
      {
        ok: true,
        skus: skus.length,
        updatedLocal,
        updatedSpecs,
        applied: {
          transferLeadDays: transferLeadDays,
          amazonTargetCoverDays: amazonTargetCoverDays,
          productionTimeDays: productionTimeDays,
          shippingTimeDays: shippingTimeDays,
          bufferTimeDays: bufferTimeDays,
        },
      },
      { headers: { "cache-control": "no-store" } },
    );
  } catch (error: unknown) {
    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 },
    );
  }
}

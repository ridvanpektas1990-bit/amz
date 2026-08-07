import { NextRequest, NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";
import { tenantIdFromRequest } from "@/lib/amazon-tenant-cookie";
import { loadCatalogMetadata } from "@/lib/amazon-catalog";
import type { CartonSpecRow } from "@/lib/carton-specs";
import {
  DEFAULT_AMAZON_TARGET_COVER_DAYS,
  DEFAULT_TRANSFER_LEAD_DAYS,
} from "@/lib/local-stock";
import { applyInboundLocalDeductions, loadLocalStockBySku } from "@/lib/local-stock-db";

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

function numOrNull(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function positiveIntOrNull(value: unknown): number | null {
  const n = numOrNull(value);
  if (n === null) return null;
  const i = Math.round(n);
  return i >= 0 ? i : null;
}

type InventoryRow = {
  asin: string | null;
  seller_sku: string | null;
  inventory_left: number | null;
  inbound_total: number | null;
};

type SpecRow = {
  seller_sku: string;
  units_per_carton: number | null;
  carton_len_cm: number | null;
  carton_w_cm: number | null;
  carton_h_cm: number | null;
  carton_weight_kg: number | null;
  production_time_days: number | null;
  shipping_time_days: number | null;
  buffer_time_days: number | null;
  updated_at: string | null;
};

async function ensureProductStub(
  sb: ReturnType<typeof supabase>,
  tenantId: string,
  sellerSku: string,
) {
  const { data: product, error: productLookupError } = await sb
    .from("inventory_products")
    .select("seller_sku")
    .eq("tenant_id", tenantId)
    .eq("seller_sku", sellerSku)
    .maybeSingle();
  if (productLookupError) throw new Error(`Products: ${productLookupError.message}`);

  if (!product) {
    const { error: insertProductError } = await sb.from("inventory_products").insert({
      tenant_id: tenantId,
      seller_sku: sellerSku,
    });
    if (insertProductError) {
      throw new Error(
        `SKU fehlt in inventory_products und konnte nicht angelegt werden: ${insertProductError.message}`,
      );
    }
  }
}

export async function GET(req: NextRequest) {
  try {
    const tenantId = tenantIdFromRequest(req);
    if (!tenantId) {
      return NextResponse.json({ ok: false, error: "not_connected" }, { status: 401 });
    }

    const skuFilter = (req.nextUrl.searchParams.get("sku") || "").trim();
    const sb = supabase();

    let inventoryQuery = sb
      .from("vw_inventory_latest_per_asin_max")
      .select("asin,seller_sku,inventory_left,inbound_total")
      .eq("tenant_id", tenantId)
      .eq("marketplace", "DE")
      .order("seller_sku", { ascending: true });

    if (skuFilter) inventoryQuery = inventoryQuery.eq("seller_sku", skuFilter);

    const { data: inventoryData, error: inventoryError } = await inventoryQuery;
    if (inventoryError) throw new Error(`Inventory: ${inventoryError.message}`);

    const inventoryRows = (inventoryData || []) as InventoryRow[];
    const skus = inventoryRows
      .map((row) => String(row.seller_sku || "").trim())
      .filter(Boolean);

    const inboundBySku = new Map<string, number>();
    for (const row of inventoryRows) {
      const sku = String(row.seller_sku || "").trim();
      if (!sku) continue;
      inboundBySku.set(sku, Math.max(0, Number(row.inbound_total) || 0));
    }
    await applyInboundLocalDeductions(sb, tenantId, inboundBySku).catch(() => null);

    let specsQuery = sb
      .from("inventory_carton_specs")
      .select(
        "seller_sku,units_per_carton,carton_len_cm,carton_w_cm,carton_h_cm,carton_weight_kg,production_time_days,shipping_time_days,buffer_time_days,updated_at",
      )
      .eq("tenant_id", tenantId);

    if (skuFilter) specsQuery = specsQuery.eq("seller_sku", skuFilter);
    else if (skus.length) specsQuery = specsQuery.in("seller_sku", skus);

    const { data: specsData, error: specsError } = await specsQuery;
    if (specsError) throw new Error(`Carton specs: ${specsError.message}`);

    const specsBySku = new Map<string, SpecRow>();
    for (const row of (specsData || []) as SpecRow[]) {
      specsBySku.set(String(row.seller_sku), row);
    }

    const localBySku = await loadLocalStockBySku(sb, tenantId, skus.length ? skus : undefined).catch(
      () => new Map(),
    );

    const asins = inventoryRows.map((row) => String(row.asin || "").trim()).filter(Boolean);
    const catalog = await loadCatalogMetadata(req, asins, "A1PA6795UKMFR9").catch(() => new Map());

    const items: CartonSpecRow[] = inventoryRows
      .filter((row) => row.seller_sku)
      .map((row) => {
        const sku = String(row.seller_sku);
        const asin = row.asin ? String(row.asin) : null;
        const spec = specsBySku.get(sku);
        const local = localBySku.get(sku);
        const meta = asin ? catalog.get(asin) : null;
        return {
          sellerSku: sku,
          asin,
          productName: meta?.productName || null,
          imageUrl: meta?.imageUrl || null,
          available: Math.max(0, Number(row.inventory_left) || 0),
          inbound: Math.max(0, Number(row.inbound_total) || 0),
          unitsPerCarton: spec?.units_per_carton ?? null,
          cartonLenCm: spec?.carton_len_cm != null ? Number(spec.carton_len_cm) : null,
          cartonWCm: spec?.carton_w_cm != null ? Number(spec.carton_w_cm) : null,
          cartonHCm: spec?.carton_h_cm != null ? Number(spec.carton_h_cm) : null,
          cartonWeightKg: spec?.carton_weight_kg != null ? Number(spec.carton_weight_kg) : null,
          productionTimeDays: spec?.production_time_days ?? null,
          shippingTimeDays: spec?.shipping_time_days ?? null,
          bufferTimeDays: spec?.buffer_time_days ?? null,
          updatedAt: spec?.updated_at ?? local?.updatedAt ?? null,
          hasSpec: Boolean(spec),
          localQty: local?.localQty ?? 0,
          onOrderUnits: local?.onOrderUnits ?? 0,
          transferLeadDays: local?.transferLeadDays ?? DEFAULT_TRANSFER_LEAD_DAYS,
          amazonTargetCoverDays:
            local?.amazonTargetCoverDays ?? DEFAULT_AMAZON_TARGET_COVER_DAYS,
          onOrderOrderedAt: local?.onOrderOrderedAt ?? null,
        };
      });

    if (skuFilter && items.length === 1) {
      return NextResponse.json(
        { ok: true, item: items[0], items },
        { headers: { "cache-control": "no-store" } },
      );
    }

    return NextResponse.json({ ok: true, items }, { headers: { "cache-control": "no-store" } });
  } catch (error: unknown) {
    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 },
    );
  }
}

export async function PUT(req: NextRequest) {
  try {
    const tenantId = tenantIdFromRequest(req);
    if (!tenantId) {
      return NextResponse.json({ ok: false, error: "not_connected" }, { status: 401 });
    }

    const body = await req.json();
    const sellerSku = String(body?.sellerSku || body?.seller_sku || "").trim();
    if (!sellerSku) {
      return NextResponse.json({ ok: false, error: "sellerSku required" }, { status: 400 });
    }

    const unitsPerCarton = positiveIntOrNull(body?.unitsPerCarton ?? body?.units_per_carton);
    if (unitsPerCarton === null || unitsPerCarton <= 0) {
      return NextResponse.json({ ok: false, error: "unitsPerCarton must be > 0" }, { status: 400 });
    }

    const productionTimeDays = positiveIntOrNull(body?.productionTimeDays ?? body?.production_time_days);
    const shippingTimeDays = positiveIntOrNull(body?.shippingTimeDays ?? body?.shipping_time_days);
    const bufferTimeDays = positiveIntOrNull(body?.bufferTimeDays ?? body?.buffer_time_days);
    const cartonLenCm = numOrNull(body?.cartonLenCm ?? body?.carton_len_cm);
    const cartonWCm = numOrNull(body?.cartonWCm ?? body?.carton_w_cm);
    const cartonHCm = numOrNull(body?.cartonHCm ?? body?.carton_h_cm);
    const cartonWeightKg = numOrNull(body?.cartonWeightKg ?? body?.carton_weight_kg);
    const localQty = positiveIntOrNull(body?.localQty ?? body?.local_qty) ?? 0;
    const onOrderUnits = positiveIntOrNull(body?.onOrderUnits ?? body?.on_order_units) ?? 0;
    const transferLeadDays =
      positiveIntOrNull(body?.transferLeadDays ?? body?.transfer_lead_days) ??
      DEFAULT_TRANSFER_LEAD_DAYS;
    const amazonTargetCoverDays = Math.max(
      1,
      positiveIntOrNull(body?.amazonTargetCoverDays ?? body?.amazon_target_cover_days) ??
        DEFAULT_AMAZON_TARGET_COVER_DAYS,
    );
    const rawOrderedAt = String(body?.onOrderOrderedAt ?? body?.on_order_ordered_at ?? "").trim();
    const onOrderOrderedAt =
      onOrderUnits > 0 && /^\d{4}-\d{2}-\d{2}$/.test(rawOrderedAt) ? rawOrderedAt : null;

    for (const [label, value] of [
      ["cartonLenCm", cartonLenCm],
      ["cartonWCm", cartonWCm],
      ["cartonHCm", cartonHCm],
      ["cartonWeightKg", cartonWeightKg],
    ] as const) {
      if (value !== null && value <= 0) {
        return NextResponse.json({ ok: false, error: `${label} must be > 0` }, { status: 400 });
      }
    }

    const sb = supabase();
    await ensureProductStub(sb, tenantId, sellerSku);

    const payload = {
      tenant_id: tenantId,
      seller_sku: sellerSku,
      units_per_carton: unitsPerCarton,
      carton_len_cm: cartonLenCm,
      carton_w_cm: cartonWCm,
      carton_h_cm: cartonHCm,
      carton_weight_kg: cartonWeightKg,
      production_time_days: productionTimeDays,
      shipping_time_days: shippingTimeDays,
      buffer_time_days: bufferTimeDays,
      updated_at: new Date().toISOString(),
    };

    const { data, error } = await sb
      .from("inventory_carton_specs")
      .upsert(payload, { onConflict: "tenant_id,seller_sku" })
      .select(
        "seller_sku,units_per_carton,carton_len_cm,carton_w_cm,carton_h_cm,carton_weight_kg,production_time_days,shipping_time_days,buffer_time_days,updated_at",
      )
      .single();

    if (error) throw new Error(error.message);

    let lastInboundSeen: number | null = null;
    const { data: invRow } = await sb
      .from("vw_inventory_latest_per_asin_max")
      .select("inbound_total")
      .eq("tenant_id", tenantId)
      .eq("marketplace", "DE")
      .eq("seller_sku", sellerSku)
      .maybeSingle();
    if (invRow) {
      lastInboundSeen = Math.max(
        0,
        Number((invRow as { inbound_total: number | null }).inbound_total) || 0,
      );
    }

    const { data: existingLocal } = await sb
      .from("inventory_local_stock")
      .select("last_inbound_seen")
      .eq("tenant_id", tenantId)
      .eq("seller_sku", sellerSku)
      .maybeSingle();

    const localPayload = {
      tenant_id: tenantId,
      seller_sku: sellerSku,
      local_qty: localQty,
      on_order_units: onOrderUnits,
      transfer_lead_days: Math.max(0, transferLeadDays),
      amazon_target_cover_days: amazonTargetCoverDays,
      on_order_ordered_at: onOrderOrderedAt,
      last_inbound_seen:
        existingLocal?.last_inbound_seen != null
          ? existingLocal.last_inbound_seen
          : lastInboundSeen,
      updated_at: new Date().toISOString(),
    };

    const { data: localData, error: localError } = await sb
      .from("inventory_local_stock")
      .upsert(localPayload, { onConflict: "tenant_id,seller_sku" })
      .select(
        "local_qty,on_order_units,transfer_lead_days,amazon_target_cover_days,on_order_ordered_at,updated_at",
      )
      .single();

    if (localError) throw new Error(`Local stock: ${localError.message}`);

    return NextResponse.json(
      {
        ok: true,
        item: {
          sellerSku: data.seller_sku,
          unitsPerCarton: data.units_per_carton,
          cartonLenCm: data.carton_len_cm != null ? Number(data.carton_len_cm) : null,
          cartonWCm: data.carton_w_cm != null ? Number(data.carton_w_cm) : null,
          cartonHCm: data.carton_h_cm != null ? Number(data.carton_h_cm) : null,
          cartonWeightKg: data.carton_weight_kg != null ? Number(data.carton_weight_kg) : null,
          productionTimeDays: data.production_time_days,
          shippingTimeDays: data.shipping_time_days,
          bufferTimeDays: data.buffer_time_days,
          updatedAt: data.updated_at,
          hasSpec: true,
          localQty: Math.max(0, Number(localData.local_qty) || 0),
          onOrderUnits: Math.max(0, Number(localData.on_order_units) || 0),
          transferLeadDays:
            Math.max(0, Number(localData.transfer_lead_days) || DEFAULT_TRANSFER_LEAD_DAYS) ||
            DEFAULT_TRANSFER_LEAD_DAYS,
          amazonTargetCoverDays: Math.max(
            1,
            Number(localData.amazon_target_cover_days) || DEFAULT_AMAZON_TARGET_COVER_DAYS,
          ),
          onOrderOrderedAt: localData.on_order_ordered_at
            ? String(localData.on_order_ordered_at).slice(0, 10)
            : null,
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

import type { SupabaseClient } from "@supabase/supabase-js";
import {
  computeInboundLocalDeduction,
  DEFAULT_AMAZON_TARGET_COVER_DAYS,
  DEFAULT_TRANSFER_LEAD_DAYS,
} from "./local-stock.ts";

type LocalStockDbRow = {
  seller_sku: string;
  local_qty: number | null;
  on_order_units: number | null;
  transfer_lead_days: number | null;
  amazon_target_cover_days: number | null;
  last_inbound_seen: number | null;
  on_order_ordered_at: string | null;
  updated_at: string | null;
};

export type LocalStockMapped = {
  localQty: number;
  onOrderUnits: number;
  transferLeadDays: number;
  amazonTargetCoverDays: number;
  lastInboundSeen: number | null;
  onOrderOrderedAt: string | null;
  updatedAt: string | null;
};

/**
 * For each local-stock row, compare current Amazon inbound to last_inbound_seen
 * and deduct rising deltas from local_qty.
 */
export async function applyInboundLocalDeductions(
  sb: SupabaseClient,
  tenantId: string,
  inboundBySku: Map<string, number>,
): Promise<{ updated: number; deductedUnits: number }> {
  if (inboundBySku.size === 0) return { updated: 0, deductedUnits: 0 };

  const skus = [...inboundBySku.keys()];
  const { data, error } = await sb
    .from("inventory_local_stock")
    .select(
      "seller_sku,local_qty,on_order_units,transfer_lead_days,amazon_target_cover_days,last_inbound_seen,on_order_ordered_at,updated_at",
    )
    .eq("tenant_id", tenantId)
    .in("seller_sku", skus);

  if (error) throw new Error(`Local stock read: ${error.message}`);

  let updated = 0;
  let deductedUnits = 0;
  const now = new Date().toISOString();

  for (const row of (data || []) as LocalStockDbRow[]) {
    const sku = String(row.seller_sku);
    if (!inboundBySku.has(sku)) continue;
    const currentInbound = inboundBySku.get(sku) || 0;
    const localQty = Math.max(0, Math.floor(Number(row.local_qty) || 0));
    const prevSeen =
      row.last_inbound_seen == null
        ? null
        : Math.max(0, Math.floor(Number(row.last_inbound_seen) || 0));
    const result = computeInboundLocalDeduction(localQty, prevSeen, currentInbound);

    if (
      prevSeen !== null &&
      result.nextLocalQty === localQty &&
      result.nextLastInboundSeen === prevSeen
    ) {
      continue;
    }

    const { error: updateError } = await sb
      .from("inventory_local_stock")
      .update({
        local_qty: result.nextLocalQty,
        last_inbound_seen: result.nextLastInboundSeen,
        updated_at: now,
      })
      .eq("tenant_id", tenantId)
      .eq("seller_sku", sku);

    if (updateError) throw new Error(`Local stock update (${sku}): ${updateError.message}`);
    updated += 1;
    deductedUnits += result.deducted;
  }

  return { updated, deductedUnits };
}

export async function loadLocalStockBySku(
  sb: SupabaseClient,
  tenantId: string,
  skus?: string[],
): Promise<Map<string, LocalStockMapped>> {
  let query = sb
    .from("inventory_local_stock")
    .select(
      "seller_sku,local_qty,on_order_units,transfer_lead_days,amazon_target_cover_days,last_inbound_seen,on_order_ordered_at,updated_at",
    )
    .eq("tenant_id", tenantId);

  if (skus && skus.length) query = query.in("seller_sku", skus);

  const { data, error } = await query;
  if (error) {
    // Graceful if migration not applied yet (column missing).
    if (/amazon_target_cover_days/i.test(error.message)) {
      let fallback = sb
        .from("inventory_local_stock")
        .select(
          "seller_sku,local_qty,on_order_units,transfer_lead_days,last_inbound_seen,on_order_ordered_at,updated_at",
        )
        .eq("tenant_id", tenantId);
      if (skus && skus.length) fallback = fallback.in("seller_sku", skus);
      const fb = await fallback;
      if (fb.error) throw new Error(`Local stock: ${fb.error.message}`);
      const map = new Map<string, LocalStockMapped>();
      for (const row of (fb.data || []) as LocalStockDbRow[]) {
        map.set(String(row.seller_sku), mapLocalRow(row));
      }
      return map;
    }
    throw new Error(`Local stock: ${error.message}`);
  }

  const map = new Map<string, LocalStockMapped>();
  for (const row of (data || []) as LocalStockDbRow[]) {
    map.set(String(row.seller_sku), mapLocalRow(row));
  }
  return map;
}

function mapLocalRow(row: LocalStockDbRow): LocalStockMapped {
  return {
    localQty: Math.max(0, Math.floor(Number(row.local_qty) || 0)),
    onOrderUnits: Math.max(0, Math.floor(Number(row.on_order_units) || 0)),
    transferLeadDays: Math.max(
      0,
      Math.round(Number(row.transfer_lead_days) || DEFAULT_TRANSFER_LEAD_DAYS) ||
        DEFAULT_TRANSFER_LEAD_DAYS,
    ),
    amazonTargetCoverDays: Math.max(
      1,
      Math.round(Number(row.amazon_target_cover_days) || DEFAULT_AMAZON_TARGET_COVER_DAYS) ||
        DEFAULT_AMAZON_TARGET_COVER_DAYS,
    ),
    lastInboundSeen:
      row.last_inbound_seen == null
        ? null
        : Math.max(0, Math.floor(Number(row.last_inbound_seen) || 0)),
    onOrderOrderedAt: row.on_order_ordered_at
      ? String(row.on_order_ordered_at).slice(0, 10)
      : null,
    updatedAt: row.updated_at ?? null,
  };
}

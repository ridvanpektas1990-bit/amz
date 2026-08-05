import type { SupabaseClient } from "@supabase/supabase-js";

export type OrderUnitRow = {
  amazonOrderId: string;
  dateISO: string;
  isoYear: number;
  isoWeek: number;
  quantity: number;
};

function berlinDateFromTs(value: string): string {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: "Europe/Berlin",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(new Date(value));
}

function orderQuantity(row: {
  number_of_items_shipped?: number | null;
  number_of_items_unshipped?: number | null;
}): number {
  return (
    Math.max(0, Number(row.number_of_items_shipped) || 0) +
    Math.max(0, Number(row.number_of_items_unshipped) || 0)
  );
}

async function paginate<T>(
  fetchPage: (
    from: number,
    to: number,
  ) => PromiseLike<{ data: T[] | null; error: { message: string } | null }>,
): Promise<T[]> {
  const PAGE = 1000;
  const rows: T[] = [];
  for (let from = 0; ; from += PAGE) {
    const to = from + PAGE - 1;
    const { data, error } = await fetchPage(from, to);
    if (error) throw new Error(error.message);
    if (!data?.length) break;
    rows.push(...data);
    if (data.length < PAGE) break;
  }
  return rows;
}

type OrderRow = {
  amazon_order_id: string;
  purchase_date_local: string | null;
  number_of_items_shipped: number | null;
  number_of_items_unshipped: number | null;
  iso_year: number | null;
  iso_week: number | null;
  order_status: string | null;
};

/**
 * Unit sales from amazon_orders (no fee lag). Quantity = shipped + unshipped.
 * Excludes Canceled. With sellerSku, uses amazon_order_items only.
 */
export async function loadOrderUnitRows({
  sb,
  tenantId,
  marketplace = "DE",
  startISO,
  endISO,
  sellerSku,
}: {
  sb: SupabaseClient;
  tenantId: string;
  marketplace?: string;
  startISO: string;
  endISO: string;
  sellerSku?: string | null;
}): Promise<OrderUnitRow[]> {
  const sku = (sellerSku || "").trim();

  if (!sku) {
    const rows = await paginate<OrderRow>((from, to) =>
      sb
        .from("amazon_orders")
        .select(
          "amazon_order_id,purchase_date_local,number_of_items_shipped,number_of_items_unshipped,iso_year,iso_week,order_status",
        )
        .eq("tenant_id", tenantId)
        .eq("marketplace", marketplace)
        .gte("purchase_date_local", `${startISO}T00:00:00+00:00`)
        .lte("purchase_date_local", `${endISO}T23:59:59.999+00:00`)
        .range(from, to)
        .then((res) => ({ data: res.data as OrderRow[] | null, error: res.error })),
    );

    return rows
      .filter((row) => String(row.order_status || "").toLowerCase() !== "canceled")
      .map((row) => {
        const qty = orderQuantity(row);
        const local = row.purchase_date_local ? String(row.purchase_date_local) : "";
        const dateISO = local ? berlinDateFromTs(local) : "";
        return {
          amazonOrderId: String(row.amazon_order_id),
          dateISO,
          isoYear: Number(row.iso_year) || 0,
          isoWeek: Number(row.iso_week) || 0,
          quantity: qty,
        };
      })
      .filter((row) => row.dateISO && row.quantity > 0 && row.isoYear > 0 && row.isoWeek > 0);
  }

  type ItemRow = {
    amazon_order_id: string;
    quantity_ordered: number | null;
    quantity_shipped: number | null;
    purchase_date_berlin: string | null;
    iso_year: number | null;
    iso_week: number | null;
  };

  let itemRows: ItemRow[] = [];
  try {
    itemRows = await paginate<ItemRow>((from, to) =>
      sb
        .from("amazon_order_items")
        .select(
          "amazon_order_id,quantity_ordered,quantity_shipped,purchase_date_berlin,iso_year,iso_week",
        )
        .eq("tenant_id", tenantId)
        .eq("marketplace", marketplace)
        .eq("seller_sku", sku)
        .gte("purchase_date_berlin", startISO)
        .lte("purchase_date_berlin", endISO)
        .range(from, to)
        .then((res) => ({ data: res.data as ItemRow[] | null, error: res.error })),
    );
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    if (!/amazon_order_items|schema cache|does not exist/i.test(message)) throw error;
  }

  const out: OrderUnitRow[] = [];
  for (const row of itemRows) {
    const orderId = String(row.amazon_order_id || "").trim();
    const qty = Math.max(
      Math.max(0, Number(row.quantity_ordered) || 0),
      Math.max(0, Number(row.quantity_shipped) || 0),
    );
    const dateISO = String(row.purchase_date_berlin || "").slice(0, 10);
    if (!orderId || !dateISO || qty <= 0) continue;
    out.push({
      amazonOrderId: orderId,
      dateISO,
      isoYear: Number(row.iso_year) || 0,
      isoWeek: Number(row.iso_week) || 0,
      quantity: qty,
    });
  }

  return out.filter((row) => row.isoYear > 0 && row.isoWeek > 0 && row.quantity > 0);
}

export type SkuSaleRow = {
  sellerSku: string;
  dateISO: string;
  quantity: number;
};

/**
 * SKU sales from amazon_order_items only (logistics path, no fee lag).
 */
export async function loadSkuSalesLinkedToOrders({
  sb,
  tenantId,
  marketplace = "DE",
  startISO,
  endISO,
  sellerSkus,
}: {
  sb: SupabaseClient;
  tenantId: string;
  marketplace?: string;
  startISO: string;
  endISO: string;
  sellerSkus: string[];
}): Promise<SkuSaleRow[]> {
  const skus = [...new Set(sellerSkus.map((s) => s.trim()).filter(Boolean))];
  if (!skus.length) return [];

  const out: SkuSaleRow[] = [];

  type ItemRow = {
    amazon_order_id: string;
    seller_sku: string | null;
    quantity_ordered: number | null;
    quantity_shipped: number | null;
    purchase_date_berlin: string | null;
  };

  try {
    const SKU_CHUNK = 100;
    for (let i = 0; i < skus.length; i += SKU_CHUNK) {
      const skuChunk = skus.slice(i, i + SKU_CHUNK);
      const itemRows = await paginate<ItemRow>((from, to) =>
        sb
          .from("amazon_order_items")
          .select(
            "amazon_order_id,seller_sku,quantity_ordered,quantity_shipped,purchase_date_berlin",
          )
          .eq("tenant_id", tenantId)
          .eq("marketplace", marketplace)
          .in("seller_sku", skuChunk)
          .gte("purchase_date_berlin", startISO)
          .lte("purchase_date_berlin", endISO)
          .range(from, to)
          .then((res) => ({ data: res.data as ItemRow[] | null, error: res.error })),
      );
      for (const row of itemRows) {
        const orderId = String(row.amazon_order_id || "").trim();
        const sellerSku = String(row.seller_sku || "").trim();
        const dateISO = String(row.purchase_date_berlin || "").slice(0, 10);
        const quantity = Math.max(
          Math.max(0, Number(row.quantity_ordered) || 0),
          Math.max(0, Number(row.quantity_shipped) || 0),
        );
        if (!orderId || !sellerSku || !dateISO || quantity <= 0) continue;
        out.push({ sellerSku, dateISO, quantity });
      }
    }
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    if (!/amazon_order_items|schema cache|does not exist/i.test(message)) throw error;
  }

  return out;
}

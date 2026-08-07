export type ListingActivityInput = {
  available?: number | null;
  inbound?: number | null;
  units90?: number | null;
  units30?: number | null;
  /** Local / 3PL warehouse units. */
  localQty?: number | null;
  /** Open supplier PO units. */
  onOrderUnits?: number | null;
};

/**
 * Long-dead catalog rows: no Amazon stock/inbound, no local/PO pipeline, no sales in 90 days.
 * OOS products that still sell (or have local/open PO) stay active.
 */
export function isActiveListing(input: ListingActivityInput): boolean {
  const available = Math.max(0, Number(input.available) || 0);
  const inbound = Math.max(0, Number(input.inbound) || 0);
  const localQty = Math.max(0, Number(input.localQty) || 0);
  const onOrderUnits = Math.max(0, Number(input.onOrderUnits) || 0);
  const units90 = Math.max(0, Number(input.units90) || 0);
  const units30 = Math.max(0, Number(input.units30) || 0);
  if (available > 0 || inbound > 0) return true;
  if (localQty > 0 || onOrderUnits > 0) return true;
  if (units90 > 0 || units30 > 0) return true;
  return false;
}

export function dailySalesAverage(units30: number | null | undefined): number {
  return Math.max(0, Number(units30) || 0) / 30;
}

export function sortByDailySalesDesc<T extends { units30?: number | null; dailySales30?: number | null; label?: string }>(
  items: T[],
): T[] {
  return [...items].sort((a, b) => {
    const aDaily = Number.isFinite(Number(a.dailySales30))
      ? Number(a.dailySales30)
      : dailySalesAverage(a.units30);
    const bDaily = Number.isFinite(Number(b.dailySales30))
      ? Number(b.dailySales30)
      : dailySalesAverage(b.units30);
    if (bDaily !== aDaily) return bDaily - aDaily;
    return String(a.label || "").localeCompare(String(b.label || ""), "de");
  });
}

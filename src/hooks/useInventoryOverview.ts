"use client";

import { useEffect, useMemo, useState } from "react";
import type { InventoryOverviewResponse } from "@/lib/inventory-overview";
import { isActiveListing, sortByDailySalesDesc } from "@/lib/listing-activity";

export function useInventoryOverview(showInactiveListings = false) {
  const [data, setData] = useState<InventoryOverviewResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [reloadKey, setReloadKey] = useState(0);

  useEffect(() => {
    let active = true;
    (async () => {
      setLoading(true);
      setError(null);
      try {
        const response = await fetch("/api/inventory/overview", { cache: "no-store" });
        const json = (await response.json()) as InventoryOverviewResponse;
        if (!response.ok || !json.ok) {
          throw new Error(json.error || "Bestandsübersicht konnte nicht geladen werden");
        }
        if (active) setData(json);
      } catch (caught) {
        if (active) setError(caught instanceof Error ? caught.message : String(caught));
      } finally {
        if (active) setLoading(false);
      }
    })();
    return () => {
      active = false;
    };
  }, [reloadKey]);

  const visibleData = useMemo(() => {
    if (!data) return null;
    const items = sortByDailySalesDesc(
      showInactiveListings
        ? data.items
        : data.items.filter((item) =>
            isActiveListing({
              available: item.available,
              inbound: item.inbound,
              units30: item.units30,
              units90: item.units90,
              localQty: item.localQty,
              onOrderUnits: item.onOrderUnits,
            }),
          ),
    );
    return { ...data, items };
  }, [data, showInactiveListings]);

  const hiddenCount = useMemo(() => {
    if (!data || showInactiveListings) return 0;
    return data.items.filter(
      (item) =>
        !isActiveListing({
          available: item.available,
          inbound: item.inbound,
          units30: item.units30,
          units90: item.units90,
          localQty: item.localQty,
          onOrderUnits: item.onOrderUnits,
        }),
    ).length;
  }, [data, showInactiveListings]);

  return {
    data: visibleData,
    rawData: data,
    loading,
    error,
    hiddenCount,
    reload: () => setReloadKey((value) => value + 1),
  };
}

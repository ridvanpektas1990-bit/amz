"use client";

import InventoryOverviewTable from "@/components/InventoryOverviewTable";
import type { InventoryOverviewResponse } from "@/lib/inventory-overview";

export function InventoryTableSection({
  data,
  loading,
  error,
  onReload,
  selectedSku,
  onSelectSku,
}: {
  data: InventoryOverviewResponse | null;
  loading: boolean;
  error: string | null;
  onReload: () => void;
  selectedSku?: string;
  onSelectSku?: (sku: string) => void;
}) {
  return (
    <InventoryOverviewTable
      data={data}
      loading={loading}
      error={error}
      onReload={onReload}
      selectedSku={selectedSku}
      onSelectSku={onSelectSku}
    />
  );
}

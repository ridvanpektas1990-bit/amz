import {
  buildSupplierOrderMessage,
  classifyReorderTiming,
  leadTimeDaysFromSpec,
  roundUpToCartons,
  type CartonSpec,
  type ReorderTiming,
  type ReorderTimingStatus,
} from "./carton-specs.ts";
import type { InventoryOverviewItem } from "./inventory-overview.ts";
import { suggestedSupplierOrderQty } from "./inventory-overview.ts";
import { isActiveListing } from "./listing-activity.ts";
import {
  classifyLocalStockAction,
  DEFAULT_TRANSFER_LEAD_DAYS,
  onOrderArrivalDelayDays,
  type LocalStockAction,
} from "./local-stock.ts";

export type ReorderBoardRow = {
  sku: string;
  asin: string;
  productName: string | null;
  imageUrl: string | null;
  available: number;
  inbound: number;
  localQty: number;
  onOrderUnits: number;
  transferLeadDays: number;
  daysOfCover: number | null;
  status: InventoryOverviewItem["status"];
  leadDays: number | null;
  bufferDays: number;
  coverDays: number | null;
  timing: ReorderTiming | null;
  action: LocalStockAction;
  rawQty: number;
  orderQty: number;
  cartons: number | null;
  /** Qty used in supplier copy text (may be gross charge when net is 0 due to open POs). */
  messageOrderQty: number;
  messageCartons: number | null;
  unitsPerCarton: number | null;
  rounded: boolean;
  productionDays: number | null;
  shippingDays: number | null;
  cartonLenCm: number | null;
  cartonWCm: number | null;
  cartonHCm: number | null;
  supplierMessage: string | null;
  missingLeadTime: boolean;
  urgency: number;
};

const timingUrgency: Record<ReorderTimingStatus, number> = {
  already_oos: 0,
  too_late: 1,
  order_now: 2,
  ok: 3,
  no_demand: 9,
};

const actionUrgency: Record<LocalStockAction, number> = {
  order_supplier: 0,
  replenish_amazon: 1,
  awaiting_supplier: 2,
  missing_lead: 3,
  ok: 8,
};

/** Horizon: show rows that must be ordered within this many days (or are already late). */
export const REORDER_BOARD_HORIZON_DAYS = 21;

export function buildReorderBoardRows(
  items: InventoryOverviewItem[],
  specsBySku: Map<
    string,
    Pick<
      CartonSpec,
      | "productionTimeDays"
      | "shippingTimeDays"
      | "bufferTimeDays"
      | "unitsPerCarton"
      | "cartonLenCm"
      | "cartonWCm"
      | "cartonHCm"
    >
  >,
  options?: { includeInactive?: boolean; horizonDays?: number; includeAllActive?: boolean },
): ReorderBoardRow[] {
  const horizon = options?.horizonDays ?? REORDER_BOARD_HORIZON_DAYS;
  const includeAll = options?.includeAllActive === true;
  const rows: ReorderBoardRow[] = [];

  for (const item of items) {
    if (!options?.includeInactive && !isActiveListing(item)) continue;

    const spec = specsBySku.get(item.sku) || null;
    const leadDays = leadTimeDaysFromSpec(spec);
    const bufferDays = Math.max(0, Math.round(Number(spec?.bufferTimeDays) || 0));
    const missingLeadTime = leadDays == null;
    const localQty = Math.max(0, Math.floor(Number(item.localQty) || 0));
    const onOrderUnits = Math.max(0, Math.floor(Number(item.onOrderUnits) || 0));
    const transferLeadDays = Math.max(
      0,
      Math.round(Number(item.transferLeadDays) || DEFAULT_TRANSFER_LEAD_DAYS) ||
        DEFAULT_TRANSFER_LEAD_DAYS,
    );

    const timing =
      leadDays != null
        ? classifyReorderTiming(
            item.daysOfCoverAmazonAndLocal ?? item.daysOfCover,
            leadDays,
          )
        : null;

    const dailyRate =
      item.forecastDailySales > 0
        ? item.forecastDailySales
        : Math.max(0, Number(item.dailySales30) || 0);

    let rawQty = 0;
    let orderQty = 0;
    let cartons: number | null = null;
    let unitsPerCarton: number | null = spec?.unitsPerCarton ?? null;
    let rounded = false;
    let coverDays: number | null = null;
    let supplierMessage: string | null = null;
    const productionDays =
      spec?.productionTimeDays != null ? Math.max(0, Number(spec.productionTimeDays) || 0) : null;
    const shippingDays =
      spec?.shippingTimeDays != null ? Math.max(0, Number(spec.shippingTimeDays) || 0) : null;
    const cartonLenCm =
      spec?.cartonLenCm != null && Number(spec.cartonLenCm) > 0 ? Number(spec.cartonLenCm) : null;
    const cartonWCm =
      spec?.cartonWCm != null && Number(spec.cartonWCm) > 0 ? Number(spec.cartonWCm) : null;
    const cartonHCm =
      spec?.cartonHCm != null && Number(spec.cartonHCm) > 0 ? Number(spec.cartonHCm) : null;

    let messageOrderQty = 0;
    let messageCartons: number | null = null;

    if (leadDays != null) {
      coverDays = leadDays + bufferDays;
      const qtyItem = {
        ...item,
        supplierLeadDays: leadDays,
        bufferTimeDays: bufferDays,
        unitsPerCarton: unitsPerCarton ?? item.unitsPerCarton ?? null,
      };
      // Same LY+growth / tempo engine as Dashboard Nachbestellung + overview API.
      const suggested = suggestedSupplierOrderQty(qtyItem);
      rawQty = suggested;
      const roundedResult = roundUpToCartons(suggested, unitsPerCarton);
      orderQty = roundedResult.orderQty;
      cartons = roundedResult.cartons;
      unitsPerCarton = roundedResult.unitsPerCarton;
      rounded = roundedResult.rounded;

      // Open POs can zero the net qty (e.g. UI-JKHV-J3CU). Still offer a copyable
      // charge so users can place an extra / early PO when they want.
      messageOrderQty = orderQty;
      messageCartons = cartons;
      if (messageOrderQty <= 0) {
        const gross = suggestedSupplierOrderQty(qtyItem, {
          deductOpenPurchaseOrders: false,
        });
        if (gross > 0) {
          const grossRounded = roundUpToCartons(gross, unitsPerCarton);
          messageOrderQty = grossRounded.orderQty;
          messageCartons = grossRounded.cartons;
          unitsPerCarton = grossRounded.unitsPerCarton;
        }
      }
    }

    const action = classifyLocalStockAction({
      amazonDaysOfCover: item.daysOfCover,
      transferLeadDays,
      localQty,
      onOrderUnits,
      supplierLeadDays: leadDays,
      dailyRate,
      chargeCoverDays: coverDays,
      amazonAndLocalDaysOfCover: item.daysOfCoverAmazonAndLocal ?? null,
      pipelineDaysOfCover: item.daysOfCoverWithLocal ?? null,
      onOrderArrivesInDays: onOrderArrivalDelayDays({
        orderedAtISO: item.onOrderOrderedAt,
        supplierLeadDays: leadDays,
      }),
    });

    // Supplier text whenever we have a usable qty (net or gross charge).
    if (
      messageOrderQty > 0 &&
      (action === "order_supplier" ||
        action === "awaiting_supplier" ||
        action === "ok" ||
        includeAll)
    ) {
      supplierMessage = buildSupplierOrderMessage({
        productName: item.productName,
        sku: item.sku,
        asin: item.asin,
        orderQty: messageOrderQty,
        cartons: messageCartons,
        unitsPerCarton,
        cartonLenCm,
        cartonWCm,
        cartonHCm,
        lang: "de",
      });
    }

    const dueSoon =
      timing != null &&
      (timing.status === "already_oos" ||
        timing.status === "too_late" ||
        timing.status === "order_now" ||
        (timing.status === "ok" &&
          timing.daysUntilMustOrder != null &&
          timing.daysUntilMustOrder <= horizon));

    const criticalWithoutSpec =
      missingLeadTime && (item.status === "out" || item.status === "critical");

    const actionable =
      action === "order_supplier" ||
      action === "replenish_amazon" ||
      action === "awaiting_supplier" ||
      action === "missing_lead";

    if (!includeAll) {
      if (!dueSoon && !criticalWithoutSpec && !actionable) continue;
      if (!missingLeadTime && orderQty <= 0 && timing?.status === "ok" && action === "ok") continue;
      // Hide quiet "ok" rows that only appeared via dueSoon but pipeline covers them.
      if (action === "ok" && !criticalWithoutSpec && timing?.status === "ok") continue;
    }

    let urgency = 5;
    if (missingLeadTime) urgency = 4;
    else urgency = actionUrgency[action];
    if (action === "order_supplier" && timing) {
      urgency = Math.min(urgency, timingUrgency[timing.status]);
    }
    if (timing?.status === "ok" && timing.daysUntilMustOrder != null) {
      // Later deadlines sort further down; still listed when includeAll.
      urgency = Math.max(urgency, 3 + timing.daysUntilMustOrder / 100);
    }
    if (includeAll && action === "ok" && !dueSoon) {
      urgency = Math.max(urgency, 8);
    }

    rows.push({
      sku: item.sku,
      asin: item.asin,
      productName: item.productName,
      imageUrl: item.imageUrl,
      available: item.available,
      inbound: item.inbound,
      localQty,
      onOrderUnits,
      transferLeadDays,
      daysOfCover: item.daysOfCover,
      status: item.status,
      leadDays,
      bufferDays,
      coverDays,
      timing,
      action,
      rawQty,
      orderQty,
      cartons,
      messageOrderQty,
      messageCartons,
      unitsPerCarton,
      rounded,
      productionDays,
      shippingDays,
      cartonLenCm,
      cartonWCm,
      cartonHCm,
      supplierMessage,
      missingLeadTime,
      urgency,
    });
  }

  return rows.sort((a, b) => {
    if (a.urgency !== b.urgency) return a.urgency - b.urgency;
    return b.orderQty - a.orderQty;
  });
}

export function timingLabel(timing: ReorderTiming | null, missingLeadTime: boolean): string {
  if (missingLeadTime) return "Stammdaten fehlen";
  if (!timing) return "–";
  switch (timing.status) {
    case "already_oos":
      return "Bereits OOS";
    case "too_late":
      return "Zu spät";
    case "order_now":
      return "Jetzt bestellen";
    case "ok":
      return timing.daysUntilMustOrder != null
        ? `Spätestens in ${timing.daysUntilMustOrder} Tagen`
        : "Ok";
    case "no_demand":
      return "Kein Tempo";
    default:
      return "–";
  }
}

export function actionLabel(action: LocalStockAction): string {
  switch (action) {
    case "replenish_amazon":
      return "Amazon nachfüllen";
    case "order_supplier":
      return "Beim Lieferanten bestellen";
    case "awaiting_supplier":
      return "Bestellung unterwegs";
    case "missing_lead":
      return "Stammdaten fehlen";
    case "ok":
      return "Abgedeckt";
    default:
      return "–";
  }
}

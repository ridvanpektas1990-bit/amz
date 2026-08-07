"use client";

import { useMemo, useState } from "react";
import {
  CartesianGrid,
  Line,
  LineChart,
  ReferenceDot,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { InventoryOverviewItem } from "@/lib/inventory-overview";
import {
  buildInventoryStockForecast,
  formatStockForecastDate,
  type StockForecastHorizonDays,
  type StockForecastPoint,
} from "@/lib/inventory-stock-forecast";

const nf = new Intl.NumberFormat("de-DE");

const HORIZONS: StockForecastHorizonDays[] = [90, 180, 365];

/** Only supplier deliveries are annotated in the chart. */
const PRIMARY_EVENT_KINDS = new Set(["supplier_delivery"]);

type Props = {
  sku: string;
  item: InventoryOverviewItem | null;
  previousYearWeekTotals?: Map<number, number> | null;
  currentYearWeekTotals?: Map<number, number> | null;
  todayISO?: string;
  /** Compact mini chart for dashboard under KPI cards. */
  compact?: boolean;
};

type ChartRow = StockForecastPoint & {
  label: string;
  monthKey: string;
  oosFloor: number | null;
};

function monthLabel(dateISO: string): string {
  const date = new Date(`${dateISO}T12:00:00Z`);
  return new Intl.DateTimeFormat("de-DE", {
    month: "short",
    year: "2-digit",
    timeZone: "UTC",
  }).format(date);
}

function TooltipContent({
  active,
  payload,
}: {
  active?: boolean;
  payload?: Array<{ payload: ChartRow }>;
}) {
  if (!active || !payload?.length) return null;
  const row = payload[0].payload;
  const primaryEvents = row.events.filter((event) => PRIMARY_EVENT_KINDS.has(event.kind));
  return (
    <div className="rounded-xl border border-slate-200 bg-white px-3 py-2 text-xs shadow-lg">
      <div className="font-semibold text-slate-900">{formatStockForecastDate(row.dateISO)}</div>
      <div className="mt-1 space-y-0.5 text-slate-600">
        <div>Gesamt: {nf.format(row.total)} Stk.</div>
        <div>Lokal: {nf.format(row.local)} Stk.</div>
        {row.total <= 0 && (
          <div className="font-semibold text-rose-700">Out of Stock · kein Verkauf</div>
        )}
      </div>
      {primaryEvents.length > 0 && (
        <div className="mt-1.5 space-y-0.5 border-t border-slate-100 pt-1.5">
          {primaryEvents.map((event) => (
            <div key={`${event.kind}-${event.label}`} className="font-medium text-slate-800">
              {event.label}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

export default function InventoryStockForecastSection({
  sku,
  item,
  previousYearWeekTotals,
  currentYearWeekTotals,
  todayISO,
  compact = false,
}: Props) {
  const [horizon, setHorizon] = useState<StockForecastHorizonDays>(compact ? 90 : 180);

  const forecast = useMemo(() => {
    if (!item || !sku) return null;
    try {
      return buildInventoryStockForecast({
        available: item.available,
        inbound: item.inbound,
        localQty: item.localQty ?? 0,
        onOrderUnits: item.onOrderUnits,
        onOrderOrderedAt: item.onOrderOrderedAt,
        supplierLeadDays: item.supplierLeadDays,
        transferLeadDays: item.transferLeadDays,
        recommendedShipQty: item.recommendedShipQty,
        amazonTargetCoverDays: item.amazonTargetCoverDays,
        forecastDailySales: item.forecastDailySales,
        dailySales30: item.dailySales30,
        previousYearWeekTotals,
        currentYearWeekTotals,
        todayISO,
        horizonDays: horizon,
      });
    } catch {
      return null;
    }
  }, [
    item,
    sku,
    horizon,
    previousYearWeekTotals,
    currentYearWeekTotals,
    todayISO,
  ]);

  const chartData = useMemo<ChartRow[]>(() => {
    if (!forecast) return [];
    return forecast.points.map((point) => ({
      ...point,
      label: formatStockForecastDate(point.dateISO),
      monthKey: monthLabel(point.dateISO),
      oosFloor: point.total <= 0 ? 0 : null,
    }));
  }, [forecast]);

  const deliveryEvents = useMemo(
    () => (forecast?.events || []).filter((event) => event.kind === "supplier_delivery"),
    [forecast],
  );

  const yMax = useMemo(() => {
    if (!chartData.length) return 10;
    const max = Math.max(...chartData.map((row) => row.total), 1);
    return Math.ceil(max * 1.1);
  }, [chartData]);

  const monthTicks = useMemo(() => {
    const seen = new Set<string>();
    const ticks: string[] = [];
    for (const row of chartData) {
      if (seen.has(row.monthKey)) continue;
      seen.add(row.monthKey);
      ticks.push(row.dateISO);
    }
    return ticks;
  }, [chartData]);

  const shellClass = compact
    ? "mb-4 rounded-2xl border border-slate-200 bg-white px-3 py-2 shadow-sm"
    : "mb-4 rounded-2xl border border-slate-200 bg-white px-3 py-4 shadow-sm md:px-4";

  if (!sku) {
    return (
      <section className={shellClass}>
        <div className="flex items-baseline justify-between gap-2">
          <h2 className="text-sm font-semibold text-slate-950">Bestandsprognose</h2>
          <p className="text-[11px] text-slate-500">SKU wählen</p>
        </div>
      </section>
    );
  }

  if (!item || !forecast || chartData.length === 0) {
    return (
      <section className={shellClass}>
        <div className="flex items-baseline justify-between gap-2">
          <h2 className="text-sm font-semibold text-slate-950">Bestandsprognose</h2>
          <p className="text-[11px] text-slate-500">
            {!item ? "Keine Bestandsdaten" : "Nicht berechenbar"}
          </p>
        </div>
      </section>
    );
  }

  const chartHeight = compact ? 148 : 360;

  return (
    <section className={shellClass}>
      <div className="flex items-center justify-between gap-2">
        <div className="min-w-0">
          <h2 className="text-sm font-semibold text-slate-950">Bestandsprognose</h2>
          {!compact && (
            <p className="mt-0.5 text-[11px] text-slate-500">Voraussichtliche Bestandsentwicklung</p>
          )}
        </div>
        <div className="flex shrink-0 rounded-full border border-slate-200 bg-slate-50 p-0.5 text-[11px]">
          {HORIZONS.map((days) => (
            <button
              key={days}
              type="button"
              onClick={() => setHorizon(days)}
              className={`rounded-full px-2 py-0.5 font-medium transition ${
                horizon === days
                  ? "bg-slate-900 text-white shadow-sm"
                  : "text-slate-600 hover:text-slate-900"
              }`}
            >
              {days}T
            </button>
          ))}
        </div>
      </div>

      <div className={`mt-1.5 w-full min-w-0 ${compact ? "h-[148px]" : "h-[360px]"}`}>
        <ResponsiveContainer width="100%" height={chartHeight}>
          <LineChart
            data={chartData}
            margin={
              compact
                ? { top: 16, right: 8, left: 2, bottom: 0 }
                : { top: 28, right: 16, left: 0, bottom: 8 }
            }
          >
            {!compact && <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" vertical={false} />}
            <XAxis
              dataKey="dateISO"
              ticks={monthTicks}
              tickFormatter={(value: string) => monthLabel(value)}
              tick={{ fontSize: compact ? 10 : 11, fill: "#64748b" }}
              axisLine={{ stroke: "#cbd5e1" }}
              tickLine={false}
              minTickGap={compact ? 36 : 24}
              height={compact ? 20 : 30}
            />
            <YAxis
              domain={[0, yMax]}
              width={compact ? 0 : 48}
              tick={compact ? false : { fontSize: 11, fill: "#64748b" }}
              axisLine={false}
              tickLine={false}
              tickFormatter={(value: number) => nf.format(value)}
              tickCount={compact ? 2 : 5}
              hide={compact}
            />
            <Tooltip content={<TooltipContent />} />

            <Line
              type="monotone"
              dataKey="oosFloor"
              name="Out of Stock"
              stroke="#dc2626"
              strokeWidth={compact ? 4 : 6}
              strokeLinecap="round"
              dot={false}
              connectNulls={false}
              isAnimationActive={false}
              legendType="none"
              activeDot={false}
            />

            <Line
              type="monotone"
              dataKey="total"
              name="Gesamt"
              stroke="#1e3a5f"
              strokeWidth={compact ? 2 : 2.5}
              dot={false}
              isAnimationActive={false}
              activeDot={{ r: 3, strokeWidth: 0, fill: "#1e3a5f" }}
            />

            {deliveryEvents.map((event) => {
              const point = chartData.find((row) => row.dateISO === event.dateISO);
              if (!point) return null;
              const units = event.units ?? 0;
              const label =
                units > 0 ? `+${nf.format(units)}` : event.shortLabel;
              return (
                <ReferenceDot
                  key={`refdot-${event.dateISO}`}
                  x={event.dateISO}
                  y={point.total}
                  r={compact ? 4 : 6}
                  fill="#059669"
                  stroke="#ffffff"
                  strokeWidth={compact ? 1.5 : 2}
                  label={{
                    value: label,
                    position: "top",
                    fill: "#047857",
                    fontSize: compact ? 10 : 11,
                    fontWeight: 700,
                    offset: compact ? 6 : 10,
                  }}
                />
              );
            })}
          </LineChart>
        </ResponsiveContainer>
      </div>
    </section>
  );
}

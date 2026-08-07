"use client";

import { Fragment, useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import {
  Bar,
  BarChart,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
  Cell,
  LabelList,
  Customized, // <— wichtig für den Hover-Hotspot
} from "recharts";
import {
  InventorySummarySection,
  InventoryTableSection,
} from "@/components/InventoryMvpSection";
import ShowInactiveListingsToggle from "@/components/ShowInactiveListingsToggle";
import SyncStatusBanner from "@/components/SyncStatusBanner";
import { useInventoryOverview } from "@/hooks/useInventoryOverview";
import {
  classifyReorderTiming,
  leadTimeDaysFromSpec,
  roundUpToCartons,
  type CartonSpecRow,
} from "@/lib/carton-specs";
import {
  classifyLocalStockAction,
  DEFAULT_TRANSFER_LEAD_DAYS,
  onOrderArrivalDelayDays,
  openOrderCoverDays,
  supplierDeliveryGap,
  supplierOrderQtyAfterPipeline,
} from "@/lib/local-stock";
import {
  classifyCoverageHealth,
  coverageHealthTextClass,
} from "@/lib/coverage-health";
import { sortByDailySalesDesc } from "@/lib/listing-activity";
import Link from "next/link";
import {
  chartWeekFromOosDate,
  planArrivalShipmentReorder,
} from "@/lib/inventory-forecast";

/* ===== Types ===== */
type Point = {
  key: string;
  label: string; // X (z. B. "KW 01")
  isoYear: number;
  isoWeek: number;
  startUtc: string;
  endUtc: string;
  total: number;
};

type SkuOption = {
  value: string;
  label: string;
  asin?: string | null;
  imageUrl?: string | null;
  productName?: string | null;
  units30?: number;
  units90?: number;
  dailySales30?: number;
  available?: number;
  inbound?: number;
  active?: boolean;
};

type RawEvent = { event_name: string; event_date: string }; // YYYY-MM-DD

type EventsForYear = {
  all: { name: string; dateISO: string; week: number }[];
  pastWeeks: Set<number>;
  futureLines: { week: number; name: string; dateISO: string }[];
};

/* ===== Helpers ===== */
function fmt(dIso: string) {
  const d = new Date(dIso);
  const dd = String(d.getUTCDate()).padStart(2, "0");
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const yyyy = d.getUTCFullYear();
  return `${dd}.${mm}.${yyyy}`;
}

// ISO-Woche
function isoWeekFromDateISO(dateISO: string): { isoYear: number; isoWeek: number } {
  const d = new Date(dateISO + "T00:00:00Z");
  const target = new Date(d.valueOf());
  const dayNr = (d.getUTCDay() + 6) % 7; // 0=Mo … 6=So
  target.setUTCDate(target.getUTCDate() - dayNr + 3);
  const firstThursday = new Date(Date.UTC(target.getUTCFullYear(), 0, 4));
  const firstDayNr = (firstThursday.getUTCDay() + 6) % 7;
  firstThursday.setUTCDate(firstThursday.getUTCDate() - firstDayNr + 3);
  const week = 1 + Math.round((target.getTime() - firstThursday.getTime()) / 604800000);
  return { isoYear: target.getUTCFullYear(), isoWeek: week };
}

/* ===== Events mappen ===== */
function buildEventMappings(points: Point[], rawEvents: RawEvent[], todayISO: string): EventsForYear {
  const pastWeeks = new Set<number>();
  const futureLines: { week: number; name: string; dateISO: string }[] = [];
  const all: { name: string; dateISO: string; week: number }[] = [];

  const today = new Date(todayISO + "T00:00:00Z");
  const weeksInData = new Set(points.map((p) => p.isoWeek));

  for (const ev of rawEvents) {
    const { isoWeek } = isoWeekFromDateISO(ev.event_date);
    if (!weeksInData.has(isoWeek)) continue;
    all.push({ name: ev.event_name, dateISO: ev.event_date, week: isoWeek });
    const evDate = new Date(ev.event_date + "T00:00:00Z");
    if (evDate <= today) pastWeeks.add(isoWeek);
    else futureLines.push({ week: isoWeek, name: ev.event_name, dateISO: ev.event_date });
  }
  return { all, pastWeeks, futureLines };
}

/* ===== Tooltip: Quick-Facts (Bars) ===== */
function YearTooltip({ active, payload, year, prevYearWeekTotals, events }: any) {
  if (!active || !payload || !payload.length) return null;
  const p = payload[0].payload as Point;
  const sales = p?.total ?? 0;
  const prev =
    prevYearWeekTotals && typeof prevYearWeekTotals.get === "function"
      ? prevYearWeekTotals.get(p.isoWeek) ?? null
      : null;

  let deltaJSX: any = null;
  if (prev !== null) {
    if (prev === 0) {
      if (sales === 0) {
        deltaJSX = <div className="text-gray-600">±0,0&nbsp;% vs. {year - 1}</div>;
      } else {
        deltaJSX = (
          <div>
            <span className="text-green-600">▲ +100,0&nbsp;%</span> vs. {year - 1}
          </div>
        );
      }
    } else {
      const pct = ((sales - prev) / prev) * 100;
      const val = Math.abs(pct).toFixed(1).replace(".", ",") + " %";
      if (pct > 0) {
        deltaJSX = (
          <div>
            <span className="text-green-600">▲ +{val}</span> vs. {year - 1}
          </div>
        );
      } else if (pct < 0) {
        deltaJSX = (
          <div>
            <span className="text-red-600">▼ −{val}</span> vs. {year - 1}
          </div>
        );
      } else {
        deltaJSX = <div className="text-gray-600">±0,0&nbsp;% vs. {year - 1}</div>;
      }
    }
  }

  const weekEvents = (events?.all || []).filter((e: any) => e.week === p.isoWeek);

  return (
    <div className="rounded-md border bg-white p-2 shadow text-sm">
      <div className="font-medium">KW {p.isoWeek}/{year}</div>
      {(p.startUtc || p.endUtc) && (
        <div className="text-xs text-slate-500">
          {fmt(p.startUtc)} – {fmt(p.endUtc)}
        </div>
      )}
      <div>🛍️ {sales} Verkäufe</div>
      {deltaJSX}
      {weekEvents.length > 0 && (
        <div className="mt-1 text-xs text-gray-600">
          {weekEvents.map((e: any, i: number) => (
            <div key={i}>• {e.name} ({fmt(e.dateISO)})</div>
          ))}
        </div>
      )}
    </div>
  );
}



/* ===== Einzeljahres-Chart ===== */
const CHART_LABEL_STEP = -12;
/** Weeks this close share the top-label lane and get stacked. */
const CHART_LABEL_NEAR_WEEKS = 1;

function ChartTopLabel(props: any) {
  const { viewBox, value = "", fill = "#111827", dy = 0 } = props || {};
  const x = viewBox?.x ?? 0;
  const y = (viewBox?.y ?? 0) - 8 + dy;
  return (
    <text
      x={x}
      y={y}
      textAnchor="middle"
      dominantBaseline="auto"
      fontSize={10}
      fill={fill}
    >
      {value}
    </text>
  );
}

function YearChart({
  data,
  year,
  yMax,
  sku,
  events,
  prevYearWeekTotals,
  currentIso,
  inventoryLeft,
  inventoryOnHand,
  inventoryInbound = 0,
  /** Same daily LY OOS dates as the cards (overview). */
  oosDateAmazonISO = null,
  oosDateGesamtISO = null,
}: {
  data: Point[];
  year: number;
  yMax: number;
  sku: string;
  events: EventsForYear | null;
  prevYearWeekTotals?: Map<number, number> | null;
  currentIso?: { year: number; week: number } | null;
  inventoryLeft?: number | null;
  inventoryOnHand?: number | null;
  inventoryInbound?: number;
  oosDateAmazonISO?: string | null;
  oosDateGesamtISO?: string | null;
}) {
  const eventsByWeek = useMemo(() => {
    const m = new Map<number, { name: string; dateISO: string }[]>();
    (events?.all || []).forEach((e) => {
      const arr = m.get(e.week) || [];
      arr.push({ name: e.name, dateISO: e.dateISO });
      m.set(e.week, arr);
    });
    return m;
  }, [events]);

  // isoWeek -> label (für ReferenceLine auf kategorialer X-Achse)
  const labelByWeek = useMemo(() => {
    const m = new Map<number, string>();
    data.forEach((p) => m.set(p.isoWeek, p.label));
    return m;
  }, [data]);

  const nf = useMemo(() => new Intl.NumberFormat("de-DE"), []);

  // Cutoff-Woche für YTD
  const cutoffWeekCurrent = useMemo(() => {
    if (year !== currentIso?.year) return null;
    const wkNow = currentIso?.week ?? 53;
    const maxWithData = data.reduce((m, p) => (p.total > 0 ? Math.max(m, p.isoWeek) : m), 0);
    const cutoff = Math.min(wkNow, Math.max(maxWithData, 0));
    return cutoff > 0 ? cutoff : null;
  }, [year, currentIso, data]);

  // YTD 2025 vs 2024
  const ytd = useMemo(() => {
    if (year !== currentIso?.year) return null;
    if (!cutoffWeekCurrent) return null;

    const currentTotal = data
      .filter((p) => p.isoWeek <= cutoffWeekCurrent)
      .reduce((acc, p) => acc + (p.total || 0), 0);

    let previousTotal: number | null = null;
    if (prevYearWeekTotals) {
      let s = 0;
      for (let w = 1; w <= cutoffWeekCurrent; w++) {
        s += Math.max(0, prevYearWeekTotals.get?.(w) ?? 0);
      }
      previousTotal = s;
    }

    let pct: number | null = null;
    if (previousTotal !== null) {
      pct = previousTotal === 0
        ? (currentTotal === 0 ? 0 : 100)
        : ((currentTotal - previousTotal) / previousTotal) * 100;
    }
    return { cutoff: cutoffWeekCurrent, currentTotal, previousTotal, pct };
  }, [year, currentIso, cutoffWeekCurrent, data, prevYearWeekTotals]);

  const ytdColorClass = useMemo(() => {
    if (!ytd || ytd.pct === null) return "text-gray-600";
    return ytd.pct > 0 ? "text-green-600" : ytd.pct < 0 ? "text-red-600" : "text-gray-600";
  }, [ytd]);

  // Farb-Logik für Bars:
  const colorForBar = (p: Point): string => {
    if (year === currentIso?.year && prevYearWeekTotals) {
      const prev = prevYearWeekTotals.get?.(p.isoWeek);
      const curr = p.total || 0;
      if (prev === undefined) return "#8884d8";
      if (prev === 0 && curr > 0) return "#16a34a";
      if (prev > 0) {
        if (curr > prev) return "#16a34a";
        if (curr < prev) return "#dc2626";
        return "#9ca3af";
      }
      return "#9ca3af";
    }
    return "#82ca9d";
  };

  // OOS-Linien aus denselben Tages-Daten wie die Cards (overview), nicht Wochen-Walk
  const oosLines = useMemo(() => {
    const lines: Array<{
      key: "amazon" | "gesamt";
      weekKw: number;
      stroke: string;
      code: string;
      name: string;
      dateISO: string;
    }> = [];

    if (year !== currentIso?.year) return lines;

    const amzDate = oosDateAmazonISO?.slice(0, 10) || null;
    const gesamtDate = oosDateGesamtISO?.slice(0, 10) || null;
    const amzKw = chartWeekFromOosDate(amzDate, year);
    const gesamtKw = chartWeekFromOosDate(gesamtDate, year);
    const sameDate = Boolean(amzDate && gesamtDate && amzDate === gesamtDate);

    // Gleiches OOS-Datum → nur rote OOS2; sonst beide (wenn vorhanden)
    if (!sameDate && amzKw != null && amzDate) {
      lines.push({
        key: "amazon",
        weekKw: amzKw,
        stroke: "#ea580c",
        code: "OOS1",
        name: "Amazon Lager",
        dateISO: amzDate,
      });
    }
    if (gesamtKw != null && gesamtDate) {
      lines.push({
        key: "gesamt",
        weekKw: gesamtKw,
        stroke: "#dc2626",
        code: "OOS2",
        name: "Gesamtlager",
        dateISO: gesamtDate,
      });
    } else if (lines.length === 0 && amzKw != null && amzDate) {
      lines.push({
        key: "amazon",
        weekKw: amzKw,
        stroke: "#ea580c",
        code: "OOS1",
        name: "Amazon Lager",
        dateISO: amzDate,
      });
    }
    return lines;
  }, [year, currentIso, oosDateAmazonISO, oosDateGesamtISO]);

  /**
   * Stack top labels when weeks are close:
   * - Events stay at base
   * - Heute moves up if near an event
   * - OOS always moves up if near anything (OOS2 above OOS1)
   */
  const chartLabelDy = useMemo(() => {
    const dy = new Map<string, number>();
    type Placed = { key: string; week: number; level: number };
    const placed: Placed[] = [];
    const near = (week: number) =>
      placed.filter((p) => Math.abs(p.week - week) <= CHART_LABEL_NEAR_WEEKS);

    for (const [i, f] of (events?.futureLines || []).entries()) {
      const key = `event-${f.week}-${i}`;
      placed.push({ key, week: f.week, level: 0 });
      dy.set(key, 0);
    }

    if (currentIso && currentIso.year === year) {
      const nearby = near(currentIso.week);
      const level = nearby.some((p) => p.key.startsWith("event-"))
        ? Math.max(...nearby.map((p) => p.level)) + 1
        : 0;
      placed.push({ key: "heute", week: currentIso.week, level });
      dy.set("heute", level * CHART_LABEL_STEP);
    }

    // OOS1 first, then OOS2 → OOS2 stacks higher when both collide
    for (const line of oosLines) {
      const key = `oos-${line.key}`;
      const nearby = near(line.weekKw);
      const level = nearby.length ? Math.max(...nearby.map((p) => p.level)) + 1 : 0;
      placed.push({ key, week: line.weekKw, level });
      dy.set(key, level * CHART_LABEL_STEP);
    }

    return dy;
  }, [events?.futureLines, currentIso, year, oosLines]);

  const chartTopMargin = useMemo(() => {
    const levels = [...chartLabelDy.values()].map((v) => Math.abs(v / CHART_LABEL_STEP));
    const maxLevel = levels.length ? Math.max(0, ...levels) : 0;
    return Math.max(32, 28 + maxLevel * 12);
  }, [chartLabelDy]);

  const [oosTipKey, setOosTipKey] = useState<"amazon" | "gesamt" | null>(null);

  return (
    <section className="mb-4">
      {/* HEADER: Titel zentriert, links YTD, rechts Lager/OOS */}
      <div className="mb-1 grid grid-cols-3 items-start">
        {/* LINKS: YTD */}
        <div className="flex flex-col">
          {year === currentIso?.year && ytd && (
            <div className="mt-1 text-sm">
              <div className="text-[11px] uppercase tracking-wide text-gray-500">
                YTD Jahresvergleich bis KW {ytd.cutoff}
              </div>
              <div className="leading-tight">
                <span className="text-base font-semibold">{nf.format(ytd.currentTotal)} Stk</span>
                {ytd.pct !== null && (
                  <span className="ml-2 text-sm">
                    <span className={ytdColorClass}>
                      {ytd.pct > 0 ? "▲ +" : ytd.pct < 0 ? "▼ " : "±"}
                      {Math.abs(ytd.pct).toFixed(1).replace(".", ",")}% 
                    </span>{" "}
                    vs. {year - 1} {ytd.previousTotal !== null ? `(${nf.format(ytd.previousTotal)} Stk)` : ""}
                  </span>
                )}
              </div>
            </div>
          )}
        </div>

        {/* MITTE: Titel + Legende */}
        <div className="justify-self-center text-center">
          <h2 className="text-lg font-semibold">
            {year}{sku ? ` · ${sku}` : ""}
          </h2>
          {year === currentIso?.year && sku && typeof inventoryLeft === "number" && (
            <div className="mt-0.5 flex flex-wrap items-center justify-center gap-x-3 gap-y-0.5 text-[11px] leading-tight">
              <span className="font-medium text-orange-600">OOS1: Amazon Lager</span>
              <span className="font-medium text-red-600">OOS2: Gesamtlager</span>
            </div>
          )}
        </div>

        {/* RECHTS: Lager/OOS (inkl. Inbound) */}
        {year === currentIso?.year && sku && typeof inventoryLeft === "number" && (
          <div className="justify-self-end text-right">
            <div className="text-[11px] uppercase tracking-wide text-gray-500">
              {inventoryInbound > 0 ? "Bestand inkl. Zulauf" : "Auf Lager"}
            </div>
            <div className="leading-none">
              <span className="text-3xl font-extrabold">{nf.format(inventoryLeft)}</span>
              <span className="ml-1 text-sm font-semibold text-gray-500">Stk</span>
            </div>
            {inventoryInbound > 0 && typeof inventoryOnHand === "number" && (
              <div className="mt-0.5 text-[11px] text-sky-700">
                {nf.format(inventoryOnHand)} verfügbar · +{nf.format(inventoryInbound)} Inbound
              </div>
            )}
          </div>
        )}
      </div>

      {/* Chart */}
      <div className="w-full h-[280px]">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart
            data={data}
            syncId="kw-sync"
            margin={{ top: chartTopMargin, right: 16, left: 16, bottom: 8 }}
            barCategoryGap={2}
            barSize={10}
          >
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="label" tick={{ fontSize: 10 }} interval={3} />
            <YAxis
              domain={[0, yMax]}
              tick={{ fontSize: 12 }}
              label={{ value: `Stückzahl (${year})`, angle: -90, position: "insideLeft" }}
              allowDecimals={false}
            />

            <Tooltip
              content={
                <YearTooltip
                  year={year}
                  prevYearWeekTotals={prevYearWeekTotals || null}
                  events={events}
                />
              }
            />

            {/* Bars */}
            <Bar dataKey="total" isAnimationActive={false}>
              {data.map((p) => (
                <Cell key={`cell-${year}-${p.isoWeek}`} fill={colorForBar(p)} />
              ))}

              {/* ★ Stern für Event-Wochen */}
              <LabelList
                dataKey="total"
                content={({ x, y, width, index }: any) => {
                  const pt = data[index] as Point | undefined;
                  if (!pt) return null;
                  const hasEvent = (eventsByWeek.get(pt.isoWeek) || []).length > 0;
                  if (!hasEvent) return null;
                  const cx = (x ?? 0) + (width ?? 0) / 2;
                  const cy = typeof y === "number" ? y - 6 : 0;
                  return (
                    <text x={cx} y={cy} textAnchor="middle" fontSize={16} fill="#f59e0b">
                      ★
                    </text>
                  );
                }}
              />
            </Bar>

            {/* Aktuelle Woche */}
            {currentIso && currentIso.year === year
              ? (() => {
                  const xLabel = labelByWeek.get(currentIso.week);
                  if (!xLabel) return null;
                  return (
                    <ReferenceLine
                      x={xLabel}
                      xAxisId={0}
                      ifOverflow="extendDomain"
                      stroke="#16a34a"
                      strokeWidth={2}
                      strokeDasharray="3 3"
                      label={
                        <ChartTopLabel
                          value="Heute"
                          fill="#16a34a"
                          dy={chartLabelDy.get("heute") ?? 0}
                        />
                      }
                    />
                  );
                })()
              : null}

            {/* OOS-Linien: kleines „OOS“-Label, Name per Hover */}
            {year === currentIso?.year &&
              oosLines.map((line) => {
                const xLabel = labelByWeek.get(line.weekKw);
                if (!xLabel) return null;
                const tipOpen = oosTipKey === line.key;

                return (
                  <Fragment key={`oos-${line.key}-${line.weekKw}`}>
                    <ReferenceLine
                      x={xLabel}
                      xAxisId={0}
                      ifOverflow="extendDomain"
                      stroke={line.stroke}
                      strokeWidth={2}
                      strokeDasharray="2 2"
                      label={
                        <ChartTopLabel
                          value={line.code}
                          fill={line.stroke}
                          dy={chartLabelDy.get(`oos-${line.key}`) ?? 0}
                        />
                      }
                    />
                    <Customized
                      key={`oos-hotspot-${line.key}`}
                      component={(props: any) => {
                        const axisMap = props?.xAxisMap || {};
                        const axis = (Object.values(axisMap) as any[])[0];
                        if (!axis || !axis.scale) return null;

                        const xLocal = axis.scale(xLabel);
                        if (typeof xLocal !== "number") return null;

                        const left = props?.offset?.left ?? 0;
                        const top = props?.offset?.top ?? 0;
                        const bottom = props?.offset?.bottom ?? 0;
                        const height = props?.height ?? 0;
                        const plotHeight = height - top - bottom;
                        const x = left + xLocal;
                        const tooltipW = 150;
                        const tooltipH = 40;
                        const dateLabel = new Intl.DateTimeFormat("de-DE", {
                          day: "2-digit",
                          month: "2-digit",
                          year: "numeric",
                        }).format(new Date(`${line.dateISO}T12:00:00Z`));

                        return (
                          <g>
                            <rect
                              x={x - 10}
                              y={top}
                              width={20}
                              height={plotHeight}
                              fill="transparent"
                              onMouseEnter={() => setOosTipKey(line.key)}
                              onMouseLeave={() => setOosTipKey(null)}
                              style={{ cursor: "help" }}
                            />
                            {tipOpen && (
                              <g pointerEvents="none">
                                <rect
                                  x={Math.min(x + 10, (props?.width ?? 0) - tooltipW - 4)}
                                  y={top + 6}
                                  width={tooltipW}
                                  height={tooltipH}
                                  rx={6}
                                  ry={6}
                                  fill="white"
                                  stroke={line.stroke}
                                  opacity={0.98}
                                />
                                <text
                                  x={Math.min(x + 18, (props?.width ?? 0) - tooltipW + 4)}
                                  y={top + 22}
                                  fontSize={12}
                                  fontWeight={600}
                                  fill={line.stroke}
                                >
                                  <tspan
                                    x={Math.min(x + 18, (props?.width ?? 0) - tooltipW + 4)}
                                    dy={0}
                                  >
                                    {line.name}
                                  </tspan>
                                  <tspan
                                    x={Math.min(x + 18, (props?.width ?? 0) - tooltipW + 4)}
                                    dy={14}
                                    fontSize={11}
                                    fontWeight={500}
                                    fill="#475569"
                                  >
                                    ca. {dateLabel}
                                  </tspan>
                                </text>
                              </g>
                            )}
                          </g>
                        );
                      }}
                    />
                  </Fragment>
                );
              })}

            {/* Zukünftige Events */}
            {(events?.futureLines || []).map((f, i) => {
              const xLabel = labelByWeek.get(f.week);
              if (!xLabel) return null;
              const eventKey = `event-${f.week}-${i}`;
              return (
                <ReferenceLine
                  key={eventKey}
                  x={xLabel}
                  xAxisId={0}
                  ifOverflow="extendDomain"
                  stroke="#3809a7ff"
                  strokeWidth={2}
                  strokeDasharray="4 2"
                  label={
                    <ChartTopLabel
                      value={f.name}
                      fill="#3809a7ff"
                      dy={chartLabelDy.get(eventKey) ?? 0}
                    />
                  }
                />
              );
            })}
          </BarChart>
        </ResponsiveContainer>
      </div>
    </section>
  );
}

function SkuProductSelect({
  options,
  value,
  onChange,
  disabled,
}: {
  options: SkuOption[];
  value: string;
  onChange: (value: string) => void;
  disabled: boolean;
}) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [menuBox, setMenuBox] = useState<{ top: number; left: number; width: number; maxHeight: number } | null>(
    null,
  );
  const rootRef = useRef<HTMLDivElement>(null);
  const buttonRef = useRef<HTMLButtonElement>(null);
  const menuRef = useRef<HTMLDivElement>(null);
  const selected = options.find((option) => option.value === value) || null;
  const filtered = useMemo(() => {
    const term = query.trim().toLowerCase();
    if (!term) return options;
    return options.filter((option) =>
      [option.productName, option.asin, option.label]
        .filter(Boolean)
        .some((entry) => String(entry).toLowerCase().includes(term)),
    );
  }, [options, query]);

  useEffect(() => {
    if (!open) {
      setMenuBox(null);
      return;
    }

    const updatePosition = () => {
      const button = buttonRef.current;
      if (!button) return;
      const rect = button.getBoundingClientRect();
      const gap = 8;
      const width = Math.min(Math.max(rect.width, 360), window.innerWidth - 24);
      const left = Math.min(Math.max(12, rect.left), window.innerWidth - width - 12);
      const spaceBelow = window.innerHeight - rect.bottom - gap - 12;
      const spaceAbove = rect.top - gap - 12;
      const preferBelow = spaceBelow >= 240 || spaceBelow >= spaceAbove;
      const maxHeight = Math.max(180, Math.min(420, preferBelow ? spaceBelow : spaceAbove));
      const top = preferBelow ? rect.bottom + gap : Math.max(12, rect.top - gap - maxHeight);
      setMenuBox({ top, left, width, maxHeight });
    };

    updatePosition();
    window.addEventListener("resize", updatePosition);
    window.addEventListener("scroll", updatePosition, true);
    return () => {
      window.removeEventListener("resize", updatePosition);
      window.removeEventListener("scroll", updatePosition, true);
    };
  }, [open]);

  useEffect(() => {
    if (!open) return;
    const close = (event: MouseEvent) => {
      const target = event.target as Node;
      if (rootRef.current?.contains(target) || menuRef.current?.contains(target)) return;
      setOpen(false);
    };
    const onKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") setOpen(false);
    };
    document.addEventListener("mousedown", close);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", close);
      document.removeEventListener("keydown", onKey);
    };
  }, [open]);

  function choose(nextValue: string) {
    onChange(nextValue);
    setQuery("");
    setOpen(false);
  }

  const menu =
    open && !disabled && menuBox
      ? createPortal(
          <div
            ref={menuRef}
            style={{
              position: "fixed",
              top: menuBox.top,
              left: menuBox.left,
              width: menuBox.width,
              maxHeight: menuBox.maxHeight,
              zIndex: 200,
            }}
            className="flex flex-col overflow-hidden rounded-xl border border-slate-200 bg-white shadow-2xl"
          >
            <div className="shrink-0 border-b border-slate-200 p-2">
              <input
                autoFocus
                value={query}
                onChange={(event) => setQuery(event.target.value)}
                onKeyDown={(event) => {
                  if (event.key === "Escape") setOpen(false);
                }}
                placeholder="Produkt, ASIN oder SKU suchen …"
                className="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm outline-none focus:border-sky-400 focus:ring-2 focus:ring-sky-100"
              />
            </div>
            <div role="listbox" className="min-h-0 flex-1 overflow-y-auto p-1">
              <button
                type="button"
                role="option"
                aria-selected={!value}
                onClick={() => choose("")}
                className={`flex w-full items-center gap-3 rounded-lg px-3 py-2 text-left hover:bg-slate-50 ${!value ? "bg-sky-50" : ""}`}
              >
                <div className="flex h-12 w-12 shrink-0 items-center justify-center rounded-lg border border-slate-200 bg-slate-50 text-[9px] font-semibold uppercase text-slate-400">
                  Alle
                </div>
                <div>
                  <div className="text-sm font-semibold">Alle Produkte</div>
                  <div className="text-xs text-slate-500">Gesamten Verkauf anzeigen</div>
                </div>
              </button>
              {filtered.map((option) => (
                <button
                  key={option.value}
                  type="button"
                  role="option"
                  aria-selected={option.value === value}
                  onClick={() => choose(option.value)}
                  className={`flex w-full items-center gap-3 rounded-lg px-3 py-2 text-left hover:bg-slate-50 ${
                    option.value === value ? "bg-sky-50" : ""
                  }`}
                >
                  <div className="flex h-12 w-12 shrink-0 items-center justify-center overflow-hidden rounded-lg border border-slate-200 bg-white">
                    {option.imageUrl ? (
                      <img
                        src={option.imageUrl}
                        alt=""
                        loading="lazy"
                        referrerPolicy="no-referrer"
                        className="h-full w-full object-contain p-0.5"
                        onError={(event) => {
                          event.currentTarget.style.display = "none";
                        }}
                      />
                    ) : (
                      <span className="text-[9px] font-semibold uppercase text-slate-400">Kein Bild</span>
                    )}
                  </div>
                  <span className="min-w-0 flex-1">
                    <span className="block truncate text-sm font-semibold text-slate-900">
                      {option.productName || option.label}
                    </span>
                    <span className="block truncate text-xs text-slate-500">
                      {[
                        option.asin,
                        option.label,
                        typeof option.dailySales30 === "number"
                          ? `Ø ${option.dailySales30.toFixed(1).replace(".", ",")} / Tag`
                          : null,
                        option.active === false ? "inaktiv" : null,
                      ]
                        .filter(Boolean)
                        .join(" · ")}
                    </span>
                  </span>
                </button>
              ))}
              {filtered.length === 0 && (
                <div className="px-3 py-8 text-center text-sm text-slate-500">Kein Produkt gefunden.</div>
              )}
            </div>
          </div>,
          document.body,
        )
      : null;

  return (
    <div ref={rootRef} className="relative w-full max-w-xl">
      <button
        ref={buttonRef}
        type="button"
        disabled={disabled}
        aria-haspopup="listbox"
        aria-expanded={open}
        onClick={() => setOpen((current) => !current)}
        className="flex min-h-14 w-full items-center gap-3 rounded-xl border-2 border-sky-300/80 bg-gradient-to-r from-sky-50 via-white to-teal-50/60 px-3 py-2 text-left shadow-md shadow-sky-100/60 transition hover:border-sky-400 hover:shadow-lg hover:shadow-sky-100/80 focus:outline-none focus:ring-2 focus:ring-sky-200 disabled:cursor-not-allowed disabled:border-slate-200 disabled:bg-slate-100 disabled:shadow-none"
      >
        <div className="flex h-10 w-10 shrink-0 items-center justify-center overflow-hidden rounded-lg border border-slate-200 bg-white">
          {selected?.imageUrl ? (
            <img
              src={selected.imageUrl}
              alt=""
              className="h-full w-full object-contain p-0.5"
              referrerPolicy="no-referrer"
            />
          ) : (
            <span className="text-[9px] font-semibold uppercase text-slate-400">Alle</span>
          )}
        </div>
        <span className="min-w-0 flex-1">
          <span className="block truncate text-sm font-semibold text-slate-900">
            {selected?.productName || selected?.label || (disabled ? "Produkte werden geladen …" : "Alle Produkte")}
          </span>
          <span className="block truncate text-xs text-slate-500">
            {selected
              ? [
                  selected.asin,
                  selected.label,
                  typeof selected.dailySales30 === "number"
                    ? `Ø ${selected.dailySales30.toFixed(1).replace(".", ",")} / Tag`
                    : null,
                ]
                  .filter(Boolean)
                  .join(" · ")
              : "Gesamten Verkauf anzeigen · sortiert nach Absatz"}
          </span>
        </span>
        <svg
          viewBox="0 0 20 20"
          aria-hidden="true"
          className={`h-4 w-4 shrink-0 text-slate-500 transition ${open ? "rotate-180" : ""}`}
        >
          <path
            d="m5 7.5 5 5 5-5"
            fill="none"
            stroke="currentColor"
            strokeWidth="1.8"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
      </button>
      {menu}
    </div>
  );
}

/* ===== Page ===== */
export default function DashboardPage() {
  const currentYear = new Date().getUTCFullYear();
  const previousYear = currentYear - 1;
  const olderYear = currentYear - 2;
  const [currentYearData, setCurrentYearData] = useState<Point[] | null>(null);
  const [previousYearData, setPreviousYearData] = useState<Point[] | null>(null);
  const [olderYearData, setOlderYearData] = useState<Point[] | null>(null);
  /** SKU the loaded week maps belong to ("" = all SKUs). Prevents cross-SKU reorder math. */
  const [chartDataSku, setChartDataSku] = useState<string | null>(null);
  const [currentRecent30Units, setCurrentRecent30Units] = useState(0);
  const [currentRecentTempoDays, setCurrentRecentTempoDays] = useState(14);
  const [orderItemsSyncKey, setOrderItemsSyncKey] = useState(0);

  const [err, setErr] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  const [skus, setSkus] = useState<SkuOption[] | null>(null);
  const [sku, setSku] = useState<string>("");
  const [skuLoadErr, setSkuLoadErr] = useState<string | null>(null);
  const [skuLoading, setSkuLoading] = useState<boolean>(true);
  const [showInactiveListings, setShowInactiveListings] = useState(false);
  const [skuMeta, setSkuMeta] = useState<{ activeCount: number; inactiveCount: number } | null>(null);
  const [showCurrentYearChart, setShowCurrentYearChart] = useState(true);
  const [showPreviousYearChart, setShowPreviousYearChart] = useState(true);
  const [showOlderYearChart, setShowOlderYearChart] = useState(false);
  const [cartonSpec, setCartonSpec] = useState<CartonSpecRow | null>(null);
  const [reorderDetailsOpen, setReorderDetailsOpen] = useState(false);

  useEffect(() => {
    try {
      const fromQuery = new URLSearchParams(window.location.search).get("sku")?.trim();
      if (fromQuery) setSku(fromQuery);
    } catch {
      // ignore
    }
  }, []);

  useEffect(() => {
    setReorderDetailsOpen(false);
  }, [sku]);

  useEffect(() => {
    if (!reorderDetailsOpen) return;
    const close = (event: MouseEvent) => {
      const target = event.target as HTMLElement | null;
      if (target?.closest?.("[data-reorder-info]")) return;
      setReorderDetailsOpen(false);
    };
    document.addEventListener("mousedown", close);
    return () => document.removeEventListener("mousedown", close);
  }, [reorderDetailsOpen]);

  const inventory = useInventoryOverview(showInactiveListings);

  useEffect(() => {
    try {
      const stored = window.localStorage.getItem("amz_show_inactive_listings");
      if (stored === "1") setShowInactiveListings(true);
    } catch {
      // ignore
    }
  }, []);

  function updateShowInactiveListings(next: boolean) {
    setShowInactiveListings(next);
    try {
      window.localStorage.setItem("amz_show_inactive_listings", next ? "1" : "0");
    } catch {
      // ignore
    }
  }

  const [currentYearEvents, setCurrentYearEvents] = useState<RawEvent[] | null>(null);
  const [previousYearEvents, setPreviousYearEvents] = useState<RawEvent[] | null>(null);
  const [olderYearEvents, setOlderYearEvents] = useState<RawEvent[] | null>(null);

  // Inventory
  const [inventoryLeft, setInventoryLeft] = useState<number | null>(null);
  const [inventoryInbound, setInventoryInbound] = useState(0);
  const [inventoryOnHand, setInventoryOnHand] = useState<number | null>(null);
  const [inventoryErr, setInventoryErr] = useState<string | null>(null);

  // Heute als UTC-ISO
  const todayISO = useMemo(() => {
    const now = new Date();
    const yyyy = now.getUTCFullYear();
    const mm = String(now.getUTCMonth() + 1).padStart(2, "0");
    const dd = String(now.getUTCDate()).padStart(2, "0");
    return `${yyyy}-${mm}-${dd}`;
  }, []);

  // SKU-Liste
  useEffect(() => {
    (async () => {
      setSkuLoading(true);
      setSkuLoadErr(null);
      try {
        const r = await fetch("/api/metrics/skus", { cache: "no-store" });
        const j = await r.json();
        if (!r.ok || !j.ok) throw new Error(j?.error || "SKU-Fehler");
        const list: SkuOption[] = sortByDailySalesDesc(
          (j.skus as any[]).map((v) => (typeof v === "string" ? { value: v, label: v.trim() } : v)),
        );
        setSkus(list);
        setSkuMeta({
          activeCount: Number(j?.meta?.activeCount ?? list.filter((row) => row.active !== false).length),
          inactiveCount: Number(j?.meta?.inactiveCount ?? list.filter((row) => row.active === false).length),
        });
      } catch (e: any) {
        setSkus([]);
        setSkuLoadErr(e?.message ?? "Unbekannter Fehler");
        console.warn("SKU-Liste:", e?.message);
      } finally {
        setSkuLoading(false);
      }
    })();
  }, []);

  // Jahresdaten laden
  useEffect(() => {
    let cancelled = false;
    (async () => {
      setLoading(true);
      setErr(null);
      // Drop stale maps immediately so reorder cannot use another SKU's (or all-SKU) demand.
      setCurrentYearData(null);
      setPreviousYearData(null);
      setOlderYearData(null);
      setChartDataSku(null);
      setCurrentRecent30Units(0);
      try {
        const qs = sku ? `&sku=${encodeURIComponent(sku)}` : "";
        const [currentResponse, previousResponse, olderResponse] = await Promise.all([
          fetch(`/api/metrics/orders-per-week?year=${currentYear}&fixed=1${qs}`, { cache: "no-store" }),
          fetch(`/api/metrics/orders-per-week?year=${previousYear}&fixed=1${qs}`, { cache: "no-store" }),
          fetch(`/api/metrics/orders-per-week?year=${olderYear}&fixed=1${qs}`, { cache: "no-store" }),
        ]);
        const currentJson = await currentResponse.json();
        const previousJson = await previousResponse.json();
        const olderJson = await olderResponse.json();
        if (cancelled) return;
        if (!currentResponse.ok || !currentJson.ok) throw new Error(currentJson?.error || `Fehler ${currentYear}`);
        if (!previousResponse.ok || !previousJson.ok) throw new Error(previousJson?.error || `Fehler ${previousYear}`);
        if (!olderResponse.ok || !olderJson.ok) throw new Error(olderJson?.error || `Fehler ${olderYear}`);
        setCurrentYearData(currentJson.points as Point[]);
        setCurrentRecent30Units(Math.max(0, Number(currentJson.recent30Units || 0)));
        setCurrentRecentTempoDays(
          Math.max(1, Math.round(Number(currentJson.recentTempoDays || 14)) || 14),
        );
        setPreviousYearData(previousJson.points as Point[]);
        setOlderYearData(olderJson.points as Point[]);
        setChartDataSku(sku);
      } catch (e: any) {
        if (!cancelled) setErr(e.message);
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [sku, currentYear, previousYear, olderYear, orderItemsSyncKey]);

  // Bei SKU: Order-Items nachladen (ohne Fee-Lag), danach Chart neu
  useEffect(() => {
    if (!sku) return;
    let cancelled = false;
    (async () => {
      try {
        const response = await fetch("/api/amazon/order-items/sync?days=18&maxOrders=220", {
          method: "POST",
          cache: "no-store",
        });
        const json = await response.json();
        if (cancelled || !response.ok || !json.ok) return;
        if ((json.upserted || 0) > 0 || (json.fetched || 0) > 0) {
          setOrderItemsSyncKey((value) => value + 1);
        }
      } catch {
        // Sync ist best-effort; Fee-Fallback bleibt aktiv
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [sku]);

  // Events je Jahr laden
  useEffect(() => {
    (async () => {
      try {
        const [currentResponse, previousResponse, olderResponse] = await Promise.all([
          fetch(`/api/events?year=${currentYear}`, { cache: "no-store" }),
          fetch(`/api/events?year=${previousYear}`, { cache: "no-store" }),
          fetch(`/api/events?year=${olderYear}`, { cache: "no-store" }),
        ]);
        const currentJson = await currentResponse.json();
        const previousJson = await previousResponse.json();
        const olderJson = await olderResponse.json();
        if (!currentResponse.ok || currentJson.error) throw new Error(currentJson?.error || `Events ${currentYear} Fehler`);
        if (!previousResponse.ok || previousJson.error) throw new Error(previousJson?.error || `Events ${previousYear} Fehler`);
        if (!olderResponse.ok || olderJson.error) throw new Error(olderJson?.error || `Events ${olderYear} Fehler`);
        setCurrentYearEvents(currentJson.events as RawEvent[]);
        setPreviousYearEvents(previousJson.events as RawEvent[]);
        setOlderYearEvents(olderJson.events as RawEvent[]);
      } catch (e) {
        console.warn("Events laden:", e);
        setCurrentYearEvents([]);
        setPreviousYearEvents([]);
        setOlderYearEvents([]);
      }
    })();
  }, [currentYear, previousYear, olderYear]);

  // Inventory laden wenn SKU gesetzt (Available + Inbound für OOS/Nachbestellung)
  useEffect(() => {
    (async () => {
      setInventoryLeft(null);
      setInventoryOnHand(null);
      setInventoryInbound(0);
      setInventoryErr(null);
      setCartonSpec(null);
      if (!sku) return;
      try {
        const [invRes, specRes] = await Promise.all([
          fetch(`/api/inventory?sku=${encodeURIComponent(sku)}`, { cache: "no-store" }),
          fetch(`/api/inventory/carton-specs?sku=${encodeURIComponent(sku)}`, { cache: "no-store" }),
        ]);
        const j = await invRes.json();
        if (!invRes.ok || !j.ok) throw new Error(j?.error || "Inventory-Fehler");
        const onHand =
          typeof j.inventory_left === "number"
            ? j.inventory_left
            : j.inventory_left != null
            ? Number(j.inventory_left)
            : null;
        const inbound =
          typeof j.inbound_total === "number"
            ? Math.max(0, j.inbound_total)
            : j.inbound_total != null
            ? Math.max(0, Number(j.inbound_total) || 0)
            : 0;
        const effective =
          typeof j.inventory_effective === "number"
            ? j.inventory_effective
            : onHand != null && Number.isFinite(onHand)
            ? Number(onHand) + inbound
            : null;
        setInventoryOnHand(onHand != null && Number.isFinite(onHand) ? Number(onHand) : 0);
        setInventoryInbound(inbound);
        setInventoryLeft(effective != null && Number.isFinite(effective) ? Number(effective) : 0);

        if (specRes.ok) {
          const specJson = await specRes.json();
          if (specJson.ok && specJson.item) {
            setCartonSpec(specJson.item as CartonSpecRow);
          }
        }
      } catch (e: any) {
        setInventoryErr(e?.message ?? "Unbekannter Fehler");
      }
    })();
  }, [sku]);

  const yMax = useMemo(() => {
    const totals: number[] = [];
    if (showCurrentYearChart && currentYearData) {
      totals.push(...currentYearData.map((point) => point.total));
    }
    if (showPreviousYearChart && previousYearData) {
      totals.push(...previousYearData.map((point) => point.total));
    }
    if (showOlderYearChart && olderYearData) {
      totals.push(...olderYearData.map((point) => point.total));
    }
    // Fallback: keep comparison scale ready even while toggles load
    if (!totals.length) {
      if (currentYearData) totals.push(...currentYearData.map((point) => point.total));
      if (previousYearData) totals.push(...previousYearData.map((point) => point.total));
    }
    return Math.max(1, ...totals.map((value) => Math.max(0, value)));
  }, [
    currentYearData,
    previousYearData,
    olderYearData,
    showCurrentYearChart,
    showPreviousYearChart,
    showOlderYearChart,
  ]);

  const currentEventMap = useMemo<EventsForYear | null>(
    () => (currentYearData && currentYearEvents ? buildEventMappings(currentYearData, currentYearEvents, todayISO) : null),
    [currentYearData, currentYearEvents, todayISO]
  );

  const previousEventMap = useMemo<EventsForYear | null>(
    () => (previousYearData && previousYearEvents ? buildEventMappings(previousYearData, previousYearEvents, todayISO) : null),
    [previousYearData, previousYearEvents, todayISO]
  );

  const olderEventMap = useMemo<EventsForYear | null>(
    () => (olderYearData && olderYearEvents ? buildEventMappings(olderYearData, olderYearEvents, todayISO) : null),
    [olderYearData, olderYearEvents, todayISO]
  );

  // Aktuelle ISO-Woche/Jahr
  const currentIso = useMemo(() => {
    const { isoWeek } = isoWeekFromDateISO(todayISO);
    const y = new Date(todayISO + "T00:00:00Z").getUTCFullYear();
    return { year: y, week: isoWeek };
  }, [todayISO]);

  // Vorjahres-Map
  const previousYearMap = useMemo(() => {
    return previousYearData ? new Map<number, number>(previousYearData.map((p) => [p.isoWeek, p.total])) : null;
  }, [previousYearData]);

  // Header-Zahlenformat
  const nfTop = useMemo(() => new Intl.NumberFormat("de-DE"), []);

  const visibleSkus = useMemo(() => {
    const list = skus || [];
    if (showInactiveListings) return list;
    return list.filter((option) => option.active !== false || option.value === sku);
  }, [skus, showInactiveListings, sku]);

  // 2025 Wochen-Map
  const currentYearMap = useMemo(() => {
    return currentYearData ? new Map<number, number>(currentYearData.map((p) => [p.isoWeek, p.total || 0])) : null;
  }, [currentYearData]);

  const leadTimeDays = useMemo(() => leadTimeDaysFromSpec(cartonSpec), [cartonSpec]);

  // Reorder: Timing aus Lieferzeit; Menge = Charge über Gesamtdauer + Puffer ab Ankunft (LY)
  const reorderPlanTop = useMemo(() => {
    if (!sku || inventoryLeft == null || leadTimeDays == null) return null;
    if (!currentIso || !previousYearMap || !currentYearMap) return null;
    // Only use week maps that were fetched for this exact SKU (never all-SKU totals).
    if (chartDataSku !== sku) return null;

    return planArrivalShipmentReorder({
      inventory: inventoryLeft,
      currentIsoYear: currentIso.year,
      currentIsoWeek: currentIso.week,
      previousYearWeekTotals: previousYearMap,
      currentYearWeekTotals: currentYearMap,
      recent30Units: currentRecent30Units,
      recentTempoDays: currentRecentTempoDays,
      leadTimeDays,
      bufferDays: Math.max(0, Number(cartonSpec?.bufferTimeDays) || 0),
    });
  }, [
    sku,
    chartDataSku,
    inventoryLeft,
    currentIso,
    previousYearMap,
    currentYearMap,
    currentRecent30Units,
    currentRecentTempoDays,
    leadTimeDays,
    cartonSpec?.bufferTimeDays,
  ]);

  const daysUntilOos = useMemo(() => {
    if (!sku) return null;
    const fromOverview = inventory.data?.items.find((item) => item.sku === sku)?.daysOfCover;
    if (typeof fromOverview === "number") return fromOverview;
    if (!reorderPlanTop) return null;
    if (reorderPlanTop.weeksUntilOos < 0) return 400;
    return Math.max(0, reorderPlanTop.weeksUntilOos * 7);
  }, [sku, inventory.data, reorderPlanTop]);

  const overviewSku = useMemo(
    () => inventory.data?.items.find((item) => item.sku === sku) || null,
    [inventory.data, sku],
  );

  const localQty = overviewSku?.localQty ?? cartonSpec?.localQty ?? 0;
  const onOrderUnits = overviewSku?.onOrderUnits ?? cartonSpec?.onOrderUnits ?? 0;
  const transferLeadDays =
    overviewSku?.transferLeadDays ?? cartonSpec?.transferLeadDays ?? DEFAULT_TRANSFER_LEAD_DAYS;

  const daysUntilOosPipeline = useMemo(() => {
    if (!sku) return null;
    const fromOverview = overviewSku?.daysOfCoverWithLocal;
    if (typeof fromOverview === "number") return fromOverview;
    return daysUntilOos;
  }, [sku, overviewSku, daysUntilOos]);

  const daysUntilOosAmazonAndLocal = useMemo(() => {
    if (!sku) return null;
    const fromOverview = overviewSku?.daysOfCoverAmazonAndLocal;
    if (typeof fromOverview === "number") return fromOverview;
    return daysUntilOos;
  }, [sku, overviewSku, daysUntilOos]);

  const onOrderOrderedAt =
    overviewSku?.onOrderOrderedAt ?? cartonSpec?.onOrderOrderedAt ?? null;

  const cartonOrder = useMemo(() => {
    if (!reorderPlanTop) return null;
    const adjusted = supplierOrderQtyAfterPipeline({
      rawChargeQty: reorderPlanTop.reorderQty,
      onOrderUnits,
    });
    return roundUpToCartons(adjusted, cartonSpec?.unitsPerCarton ?? null);
  }, [reorderPlanTop, cartonSpec, onOrderUnits]);

  const reorderTiming = useMemo(() => {
    if (leadTimeDays == null) return null;
    // Same horizon as Lieferverzug Bestellfrist (Amazon + Eigenlager).
    return classifyReorderTiming(daysUntilOosAmazonAndLocal, leadTimeDays);
  }, [daysUntilOosAmazonAndLocal, leadTimeDays]);

  const stockAction = useMemo(() => {
    if (leadTimeDays == null) return null;
    return classifyLocalStockAction({
      amazonDaysOfCover: daysUntilOos,
      transferLeadDays,
      localQty,
      onOrderUnits,
      supplierLeadDays: leadTimeDays,
      dailyRate:
        overviewSku?.forecastDailySales ||
        overviewSku?.dailySales30 ||
        (currentRecentTempoDays > 0 ? currentRecent30Units / currentRecentTempoDays : 0),
      chargeCoverDays: reorderPlanTop?.coverDays ?? null,
      amazonAndLocalDaysOfCover: daysUntilOosAmazonAndLocal,
      pipelineDaysOfCover: daysUntilOosPipeline,
      onOrderArrivesInDays: onOrderArrivalDelayDays({
        orderedAtISO: onOrderUnits > 0 ? onOrderOrderedAt : null,
        supplierLeadDays: leadTimeDays,
      }),
    });
  }, [
    leadTimeDays,
    daysUntilOos,
    transferLeadDays,
    localQty,
    onOrderUnits,
    overviewSku,
    currentRecent30Units,
    currentRecentTempoDays,
    reorderPlanTop?.coverDays,
    daysUntilOosAmazonAndLocal,
    daysUntilOosPipeline,
    onOrderOrderedAt,
  ]);

  const dailyRateForCover = useMemo(() => {
    return (
      overviewSku?.forecastDailySales ||
      overviewSku?.dailySales30 ||
      (currentRecentTempoDays > 0 ? currentRecent30Units / currentRecentTempoDays : 0) ||
      0
    );
  }, [overviewSku, currentRecent30Units, currentRecentTempoDays]);

  const onOrderCoverDays = useMemo(
    () => openOrderCoverDays(onOrderUnits, dailyRateForCover),
    [onOrderUnits, dailyRateForCover],
  );

  /** Order qty even before chart LY maps are ready (same charge idea as board). */
  const plannedSupplierOrderQty = useMemo(() => {
    if (cartonOrder && cartonOrder.orderQty > 0) return cartonOrder.orderQty;
    if (leadTimeDays == null || dailyRateForCover <= 0) return null;
    const buffer = Math.max(0, Number(cartonSpec?.bufferTimeDays) || 0);
    const coverDays = Math.max(1, leadTimeDays + buffer);
    const raw = Math.max(0, Math.ceil(dailyRateForCover * coverDays));
    const adjusted = supplierOrderQtyAfterPipeline({
      rawChargeQty: raw,
      onOrderUnits,
    });
    const rounded = roundUpToCartons(adjusted, cartonSpec?.unitsPerCarton ?? null);
    return rounded.orderQty > 0 ? rounded.orderQty : null;
  }, [cartonOrder, leadTimeDays, dailyRateForCover, cartonSpec, onOrderUnits]);

  const deliveryGap = useMemo(() => {
    if (leadTimeDays == null) return null;
    const gap = supplierDeliveryGap({
      oosDaysAmazonAndLocal: daysUntilOosAmazonAndLocal,
      orderedAtISO: onOrderUnits > 0 ? onOrderOrderedAt : null,
      supplierLeadDays: leadTimeDays,
    });
    if (!gap) return null;
    // Same calendar date as chart OOS2 / overview (no re-derive drift)
    const oosDateISO =
      overviewSku?.estimatedOosDateAmazonAndLocal?.slice(0, 10) || gap.oosDateISO;
    if (!oosDateISO || !gap.arrivalDateISO) {
      return { ...gap, oosDateISO };
    }
    const oosMs = Date.parse(`${oosDateISO}T12:00:00Z`);
    const arrivalMs = Date.parse(`${gap.arrivalDateISO}T12:00:00Z`);
    if (!Number.isFinite(oosMs) || !Number.isFinite(arrivalMs)) {
      return { ...gap, oosDateISO };
    }
    return {
      ...gap,
      oosDateISO,
      gapDays: Math.round((arrivalMs - oosMs) / 86_400_000),
    };
  }, [
    leadTimeDays,
    daysUntilOosAmazonAndLocal,
    onOrderUnits,
    onOrderOrderedAt,
    overviewSku?.estimatedOosDateAmazonAndLocal,
  ]);

  const deDate = (iso: string | null | undefined) => {
    if (!iso) return "–";
    return new Intl.DateTimeFormat("de-DE", {
      day: "2-digit",
      month: "2-digit",
      year: "numeric",
    }).format(new Date(`${iso.slice(0, 10)}T12:00:00Z`));
  };

  const inDaysLabel = (days: number) =>
    days === 1 ? "in 1 Tag" : `in ${nfTop.format(Math.max(0, Math.round(days)))} Tagen`;

  /** Same “Bestellfrist” number as Lieferverzug card. */
  const daysUntilSupplierOrder = useMemo(() => {
    if (onOrderUnits > 0 && deliveryGap?.hasOpenOrder) return null;
    if (deliveryGap?.gapDays != null && !deliveryGap.hasOpenOrder) {
      // gapDays = arrival − OOS; negative → days of buffer / Bestellfrist
      return Math.round(-deliveryGap.gapDays);
    }
    if (reorderTiming?.daysUntilMustOrder != null) {
      return Math.round(reorderTiming.daysUntilMustOrder);
    }
    return null;
  }, [onOrderUnits, deliveryGap, reorderTiming]);

  const daysUntilAmazonShipAction = useMemo(() => {
    if (localQty <= 0 || daysUntilOos == null) return null;
    return Math.round(daysUntilOos) - Math.round(transferLeadDays);
  }, [localQty, daysUntilOos, transferLeadDays]);

  const coverageHealth = useMemo(() => {
    if (!sku || leadTimeDays == null) return null;
    return classifyCoverageHealth({
      amazonAvailable: inventoryLeft ?? 0,
      amazonInbound: 0,
      localQty,
      onOrderUnits,
      amazonDaysOfCover: daysUntilOos,
      amazonAndLocalDaysOfCover: daysUntilOosAmazonAndLocal,
      stockAction,
      reorderTiming,
      deliveryGapDays: deliveryGap?.gapDays ?? null,
      daysUntilShip: daysUntilAmazonShipAction,
      daysUntilOrder: daysUntilSupplierOrder,
    });
  }, [
    sku,
    leadTimeDays,
    inventoryLeft,
    localQty,
    onOrderUnits,
    daysUntilOos,
    daysUntilOosAmazonAndLocal,
    stockAction,
    reorderTiming,
    deliveryGap?.gapDays,
    daysUntilAmazonShipAction,
    daysUntilSupplierOrder,
  ]);

  const eigenesLagerLabel = (() => {
    if (localQty <= 0) return "Leer";
    if (daysUntilOos == null) return "nicht nötig";
    const daysLeft = daysUntilAmazonShipAction ?? 0;
    if (daysLeft <= 0) return "Amazon nachfüllen";
    return `${inDaysLabel(daysLeft)} schicken`;
  })();

  const lieferantLabel = (() => {
    if (onOrderUnits > 0) {
      return `${nfTop.format(onOrderUnits)} Stück bereits bestellt`;
    }
    const qty =
      plannedSupplierOrderQty != null && plannedSupplierOrderQty > 0
        ? nfTop.format(plannedSupplierOrderQty)
        : null;
    const daysLeft = daysUntilSupplierOrder;

    if (daysLeft == null) {
      if (qty) return `${qty} Stück bestellen`;
      return "nicht nötig";
    }
    if (daysLeft <= 0) {
      return qty ? `${qty} Stück jetzt bestellen` : "jetzt bestellen";
    }
    return qty
      ? `${inDaysLabel(daysLeft)} ${qty} Stück bestellen`
      : `${inDaysLabel(daysLeft)} bestellen`;
  })();

  // Countdown events remain available for charts via event maps
  // (header text intentionally minimal)

  function focusSku(nextSku: string) {
    setSku(nextSku);
    // Nachbestellblock mounts after SKU state update — scroll on next ticks.
    window.setTimeout(() => {
      const target =
        document.getElementById("nachbestellung") || document.getElementById("product-filter");
      target?.scrollIntoView({ behavior: "smooth", block: "start" });
    }, 80);
  }

  const chartToggleClass = (active: boolean) =>
    `rounded-full border px-3 py-1.5 text-xs font-medium transition ${
      active
        ? "border-slate-900 bg-slate-900 text-white"
        : "border-slate-300 bg-white text-slate-600 hover:border-slate-400"
    }`;

  return (
    <div className="mx-auto max-w-6xl px-4 py-5 md:py-6">
        <div className="mb-4 flex items-center justify-between gap-3">
          <h1 className="shrink-0 text-lg font-semibold tracking-tight text-slate-950 md:text-xl">Dashboard</h1>
          <SyncStatusBanner />
        </div>

        <section
          id="product-filter"
          className="mb-4 scroll-mt-4 rounded-2xl border border-slate-200/90 bg-gradient-to-br from-slate-50 via-white to-sky-50/40 shadow-sm"
        >
          <div className="px-3 py-3 md:px-4 md:py-4">
            <div className="flex min-w-0 flex-col gap-2 sm:flex-row sm:items-center sm:flex-wrap">
              <SkuProductSelect
                options={visibleSkus}
                value={sku}
                onChange={setSku}
                disabled={skuLoading || !skus}
              />
              {!!sku && (
                <button
                  type="button"
                  className="shrink-0 rounded-full border border-slate-200 bg-white px-3 py-1.5 text-xs font-medium text-slate-600 shadow-sm transition hover:border-slate-300 hover:bg-slate-50 hover:text-slate-900"
                  onClick={() => setSku("")}
                >
                  Alle Produkte
                </button>
              )}
              <ShowInactiveListingsToggle
                checked={showInactiveListings}
                onChange={updateShowInactiveListings}
                activeCount={skuMeta?.activeCount}
                inactiveCount={skuMeta?.inactiveCount}
              />
            </div>

          {!sku && !skuLoading && !skuLoadErr && (
            <p className="mt-3 rounded-xl border border-slate-200/80 bg-white/70 px-3 py-2.5 text-sm text-slate-600 shadow-sm backdrop-blur-[2px]">
              Noch kein Produkt gewählt. Nutze den Überblick unten oder wähle eine SKU, um die Nachbestellung zu sehen.
            </p>
          )}

          {sku && leadTimeDays == null && (
            <div
              id="nachbestellung"
              className="mt-3 scroll-mt-4 rounded-xl border border-slate-200 bg-white px-3 py-2.5 text-sm text-slate-700 shadow-sm"
            >
              Für die Nachbestellung fehlen Produktions- und Lieferdauer.{" "}
              <Link href="/sku-stammdaten" className="font-semibold text-slate-900 underline">
                In SKU-Stammdaten hinterlegen
              </Link>
            </div>
          )}

          {sku && leadTimeDays != null && reorderTiming && stockAction && (
            <div
              id="nachbestellung"
              className="mt-3 scroll-mt-4 rounded-xl border border-slate-200 bg-white px-4 py-3 shadow-sm"
            >
              <div className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">
                Wann handeln?{" "}
                <span className="font-normal normal-case tracking-normal text-slate-400">
                  (Lieferzeit {leadTimeDays} Tage
                  {cartonSpec
                    ? `: ${cartonSpec.productionTimeDays || 0} Prod. + ${cartonSpec.shippingTimeDays || 0} Versand`
                    : ""}
                  {localQty > 0 || onOrderUnits > 0
                    ? ` · Transfer lokal ${transferLeadDays}T`
                    : ""}
                  )
                </span>
              </div>

              <div className="mt-1">
                <div
                  className={`text-lg font-semibold ${
                    coverageHealth
                      ? coverageHealthTextClass[coverageHealth.tone]
                      : stockAction === "order_supplier" &&
                          (reorderTiming.status === "already_oos" ||
                            reorderTiming.status === "too_late")
                        ? "text-red-700"
                        : stockAction === "order_supplier"
                          ? "text-amber-800"
                          : stockAction === "replenish_amazon"
                            ? "text-sky-800"
                            : stockAction === "awaiting_supplier"
                              ? "text-violet-800"
                              : "text-slate-950"
                  }`}
                >
                  {coverageHealth?.label ?? "–"}
                </div>
                {stockAction === "replenish_amazon" && (
                  <p className="mt-1 text-sm leading-relaxed text-slate-600">
                    Amazon-Cover ca. {daysUntilOos == null ? "–" : nfTop.format(daysUntilOos)} Tage,
                    lokaler Bestand {nfTop.format(localQty)} Stück (Transfer {transferLeadDays} Tage).
                    Kein neues Lieferanten-PO nötig – aus lokalem Lager nach Amazon schicken.
                  </p>
                )}
                {stockAction === "awaiting_supplier" && (
                  <p className="mt-1 text-sm leading-relaxed text-slate-600">
                    Offene Bestellung {nfTop.format(onOrderUnits)} Stück deckt die Lücke.
                    Pipeline-Cover ca.{" "}
                    {daysUntilOosPipeline == null ? "–" : `${nfTop.format(daysUntilOosPipeline)} Tage`}.
                  </p>
                )}
                {stockAction === "order_supplier" && reorderTiming.status === "order_now" && (
                  <p className="mt-1 text-sm leading-relaxed text-slate-600">
                    Pipeline-OOS in ca. {nfTop.format(reorderTiming.daysUntilOos!)} Tagen – genau die Lieferzeit.
                    Heute ist der letzte sinnvolle Bestelltag beim Lieferanten.
                  </p>
                )}
                {stockAction === "order_supplier" && reorderTiming.status === "ok" && (
                  <p className="mt-1 text-sm leading-relaxed text-slate-600">
                    Spätestens in {nfTop.format(reorderTiming.daysUntilMustOrder!)} Tagen beim Lieferanten
                    bestellen. Pipeline-OOS in {nfTop.format(reorderTiming.daysUntilOos!)} Tagen
                    {reorderPlanTop?.oosWeek != null
                      ? ` (KW ${reorderPlanTop.oosWeek}/${reorderPlanTop.oosYear})`
                      : ""}
                    .
                  </p>
                )}
                {stockAction === "ok" && (
                  <p className="mt-1 text-sm leading-relaxed text-slate-600">
                    {daysUntilOos != null && daysUntilOos <= 30
                      ? `Amazon-Reichweite nur ca. ${nfTop.format(daysUntilOos)} Tage – trotzdem abgedeckt, weil Eigenlager/Lieferant die nächste Aktion rechtzeitig ermöglichen.`
                      : "Pipeline reicht voraussichtlich über die Lieferzeit."}
                  </p>
                )}
              </div>

              <div className="mt-3 grid gap-2 border-t border-slate-100 pt-3 text-sm sm:grid-cols-3">
                <div className="rounded-lg border border-sky-100 bg-sky-50/80 px-3 py-2.5">
                  <div className="text-[11px] font-medium text-sky-700/80">Aktueller Stand</div>
                  <div className="mt-0.5 font-semibold text-sky-950">
                    {daysUntilOos === null
                      ? "Kein Verkaufstempo"
                      : daysUntilOos === 0
                        ? "Amazon-Lager leer"
                        : `Amazon-Reichweite ca. ${nfTop.format(daysUntilOos)} Tage`}
                  </div>
                  <div className="mt-1 space-y-1 text-xs leading-snug text-sky-900/75">
                    <div>
                      <span className="font-semibold tabular-nums text-sky-950">
                        {inventoryLeft != null ? `${nfTop.format(Math.round(inventoryLeft))} Stück` : "–"}
                      </span>
                      <span className="text-sky-800/70">
                        {" "}
                        {inventoryInbound > 0
                          ? "(Amazon-Lager + Zulauf unterwegs)"
                          : "(Amazon-Lager)"}
                      </span>
                    </div>
                    {localQty > 0 && (
                      <div>
                        <span className="font-semibold tabular-nums text-violet-900">
                          {nfTop.format(localQty)} Stück
                        </span>
                        <span className="text-violet-800/75"> (Eigenes / externes Lager)</span>
                      </div>
                    )}
                    {onOrderUnits > 0 && (
                      <div>
                        <span className="font-semibold tabular-nums text-violet-900">
                          {nfTop.format(onOrderUnits)} Stück
                        </span>
                        <span className="text-violet-800/75">
                          {" "}
                          (beim Lieferanten bestellt
                          {onOrderOrderedAt ? ` am ${deDate(onOrderOrderedAt)}` : ""}
                          )
                        </span>
                      </div>
                    )}
                  </div>
                </div>
                <div
                  className={`rounded-lg border px-3 py-2.5 ${
                    deliveryGap?.gapDays != null && deliveryGap.gapDays > 0
                      ? "border-rose-100 bg-rose-50/80"
                      : "border-emerald-100 bg-emerald-50/80"
                  }`}
                >
                  <div
                    className={`text-[11px] font-medium ${
                      deliveryGap?.gapDays != null && deliveryGap.gapDays > 0
                        ? "text-rose-700/80"
                        : "text-emerald-700/80"
                    }`}
                  >
                    Lieferverzug
                  </div>
                  <div
                    className={`mt-0.5 font-semibold tabular-nums ${
                      deliveryGap?.gapDays != null && deliveryGap.gapDays > 0
                        ? "text-rose-800"
                        : "text-emerald-950"
                    }`}
                  >
                    {deliveryGap?.gapDays == null
                      ? "–"
                      : deliveryGap.gapDays > 0
                        ? `− ${nfTop.format(deliveryGap.gapDays)} Tage`
                        : "kein Verzug"}
                  </div>
                  {deliveryGap?.arrivalDateISO && deliveryGap?.oosDateISO && (
                    <div
                      className={`mt-1.5 grid grid-cols-[auto_1fr] gap-x-3 gap-y-0.5 text-xs leading-snug ${
                        deliveryGap.gapDays != null && deliveryGap.gapDays > 0
                          ? "text-rose-800/75"
                          : "text-emerald-800/75"
                      }`}
                    >
                      <span>Ankunft:</span>
                      <span>
                        ~ {deDate(deliveryGap.arrivalDateISO)}
                        {!deliveryGap.hasOpenOrder ? " (falls heute bestellt)" : ""}
                      </span>
                      <span>OOS:</span>
                      <span>~ {deDate(deliveryGap.oosDateISO)}</span>
                    </div>
                  )}
                  {deliveryGap?.gapDays != null && deliveryGap.gapDays > 0 && (
                    <div className="mt-1.5 text-xs leading-snug text-rose-800/80">
                      {nfTop.format(deliveryGap.gapDays)} Tage eventuell keine Sales, da die
                      Lieferung nach OOS ankommt
                    </div>
                  )}
                  {deliveryGap?.gapDays != null &&
                    deliveryGap.gapDays < 0 &&
                    !deliveryGap.hasOpenOrder && (
                      <div className="mt-1.5 text-xs leading-snug text-emerald-800/70">
                        noch {nfTop.format(Math.abs(deliveryGap.gapDays))} Tage bis Bestellfrist
                      </div>
                    )}
                </div>
                <div className="relative rounded-lg border border-teal-100 bg-teal-50/80 px-3 py-2.5">
                  <div className="flex items-start justify-between gap-2">
                    <div className="text-[11px] font-medium text-teal-700/80">Nachbestellung</div>
                    <div className="relative shrink-0" data-reorder-info>
                      <button
                        type="button"
                        aria-label="Rechnung und Nachbestellmenge erklären"
                        aria-expanded={reorderDetailsOpen}
                        onClick={() => setReorderDetailsOpen((open) => !open)}
                        className="inline-flex h-5 w-5 items-center justify-center rounded-full border border-teal-300/80 bg-white text-[11px] font-semibold text-teal-700 transition hover:border-teal-400 hover:bg-teal-50"
                      >
                        i
                      </button>
                      {reorderDetailsOpen && (
                        <div className="absolute right-0 top-full z-40 mt-2 w-80 max-w-[min(20rem,calc(100vw-2rem))] space-y-2 rounded-xl border border-slate-200 bg-white p-3 text-left text-xs leading-relaxed text-slate-600 shadow-xl">
                          <p className="font-semibold text-slate-800">Zwei getrennte Entscheidungen</p>
                          <p>
                            <strong className="text-slate-800">Eigenes Lager:</strong> Amazon aus dem externen /
                            eigenen Lager nachfüllen (kurze Transferzeit). Nur nötig, wenn Amazon-Reichweite die
                            Transferzeit unterschreitet und dort noch Bestand liegt.
                          </p>
                          <p>
                            <strong className="text-slate-800">Lieferant:</strong> Neue Bestellung mit langer
                            Lieferzeit. Nur nötig, wenn Amazon + eigenes Lager (+ offene Bestellung) die
                            Lieferanten-Leadzeit nicht mehr decken.
                          </p>
                          <p>
                            <strong className="text-slate-800">Menge:</strong> Vorjahresbedarf über{" "}
                            {reorderPlanTop ? `${reorderPlanTop.coverDays} Tage` : "Charge-Zeitraum"}, abzüglich
                            lokalem Bestand und offener Bestellung
                            {cartonOrder && cartonOrder.orderQty > 0
                              ? ` → ${nfTop.format(cartonOrder.orderQty)} Stück`
                              : ""}
                            .
                          </p>
                          <Link href="/sku-stammdaten" className="inline-block text-slate-600 underline">
                            Stammdaten anpassen
                          </Link>
                        </div>
                      )}
                    </div>
                  </div>
                  <div className="mt-1.5 space-y-1 text-xs leading-snug text-teal-950">
                    <div className="flex items-baseline justify-between gap-2">
                      <span className="text-teal-800/75">Eigenes Lager</span>
                      <span className="font-semibold tabular-nums">{eigenesLagerLabel}</span>
                    </div>
                    <div className="flex items-baseline justify-between gap-2">
                      <span className="text-teal-800/75">Lieferant</span>
                      <span className="font-semibold tabular-nums">{lieferantLabel}</span>
                    </div>
                  </div>
                  {onOrderUnits > 0 && (
                    <div className="mt-1 space-y-0.5 text-[11px] leading-snug text-teal-800/75">
                      {onOrderCoverDays != null ? (
                        <div>
                          Neue Lieferung reicht ~ {nfTop.format(onOrderCoverDays)} Tage ab Ankunft
                        </div>
                      ) : null}
                      {stockAction === "order_supplier" &&
                        cartonOrder &&
                        cartonOrder.orderQty > 0 && (
                          <div>Zusätzlich nachbestellen: {nfTop.format(cartonOrder.orderQty)} Stück</div>
                        )}
                    </div>
                  )}
                  {onOrderUnits <= 0 &&
                    stockAction === "order_supplier" &&
                    reorderPlanTop &&
                    cartonOrder &&
                    cartonOrder.orderQty > 0 && (
                      <div className="mt-1 text-[11px] text-teal-800/75">
                        Charge für {nfTop.format(reorderPlanTop.coverDays)} Tage
                      </div>
                    )}
                  {localQty > 0 &&
                    daysUntilOos != null &&
                    daysUntilOos <= transferLeadDays && (
                      <div className="mt-1 text-[11px] text-teal-800/75">
                        {nfTop.format(localQty)} Stück im eigenen / externen Lager
                      </div>
                    )}
                </div>
              </div>

              {stockAction === "order_supplier" && reorderTiming.status === "too_late" && (
                <p className="mt-3 text-sm leading-relaxed text-slate-600">
                  Pipeline reicht noch ca. {nfTop.format(reorderTiming.daysUntilOos!)} Tage. Bestellst du heute,
                  fehlt die Ware {nfTop.format(Math.abs(reorderTiming.daysUntilMustOrder!))} Tage vor Ankunft –
                  in dem Zeitraum kein Verkauf (laut Prognose).
                </p>
              )}

              {stockAction === "order_supplier" && reorderTiming.status === "already_oos" && (
                <p className="mt-3 text-sm leading-relaxed text-slate-600">
                  Pipeline ist leer. Bestellst du heute, kommt Ware erst in {leadTimeDays} Tagen an – so lange
                  kein Verkauf.
                </p>
              )}
            </div>
          )}
          {skuLoadErr && <div className="mt-2 text-sm text-red-600">{skuLoadErr}</div>}
          {inventoryErr && <div className="mt-2 text-sm text-red-600">{inventoryErr}</div>}
          </div>
        </section>

        <InventorySummarySection
          data={inventory.data}
          loading={inventory.loading}
          error={inventory.error}
          selectedSku={sku || undefined}
          onSelectSku={focusSku}
        />

        <section className="mb-4 rounded-2xl border border-slate-200 bg-white p-3 shadow-sm md:p-4">
          <div className="mb-3 flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
            <div>
              <h2 className="text-base font-semibold text-slate-950">Jahresvergleich</h2>
              <p className="text-xs text-slate-500">
                {sku ? `Einheiten verkauft · ${sku}` : "Einheiten verkauft · alle Produkte"}
              </p>
            </div>
            <div className="flex flex-wrap gap-2">
              <button type="button" className={chartToggleClass(showCurrentYearChart)} onClick={() => setShowCurrentYearChart((v) => !v)}>
                {currentYear}
              </button>
              <button type="button" className={chartToggleClass(showPreviousYearChart)} onClick={() => setShowPreviousYearChart((v) => !v)}>
                {previousYear}
              </button>
              <button type="button" className={chartToggleClass(showOlderYearChart)} onClick={() => setShowOlderYearChart((v) => !v)}>
                {olderYear}
              </button>
            </div>
          </div>

          {loading && (
            <div className="space-y-3 py-2" aria-busy="true" aria-label="Charts werden geladen">
              <div className="h-40 animate-pulse rounded-xl bg-slate-100 md:h-44" />
              <div className="h-40 animate-pulse rounded-xl bg-slate-100 md:h-44" />
            </div>
          )}
          {err && (
            <div className="rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">
              Charts konnten nicht geladen werden: {err}
            </div>
          )}

          {!loading && !err && currentYearData && previousYearData && olderYearData && (
            <div className="space-y-1">
              {!showCurrentYearChart && !showPreviousYearChart && !showOlderYearChart && (
                <div className="py-6 text-center text-sm text-slate-500">Mindestens ein Jahr wählen.</div>
              )}
              {showCurrentYearChart && (
                <YearChart
                  data={currentYearData}
                  year={currentYear}
                  yMax={yMax}
                  sku={sku}
                  events={currentEventMap}
                  prevYearWeekTotals={previousYearMap}
                  currentIso={currentIso}
                  inventoryLeft={inventoryLeft}
                  inventoryOnHand={inventoryOnHand}
                  inventoryInbound={inventoryInbound}
                  oosDateAmazonISO={overviewSku?.estimatedOosDate ?? null}
                  oosDateGesamtISO={overviewSku?.estimatedOosDateAmazonAndLocal ?? null}
                />
              )}
              {showPreviousYearChart && (
                <YearChart
                  data={previousYearData}
                  year={previousYear}
                  yMax={yMax}
                  sku={sku}
                  events={previousEventMap}
                  currentIso={currentIso}
                />
              )}
              {showOlderYearChart && (
                <YearChart
                  data={olderYearData}
                  year={olderYear}
                  yMax={yMax}
                  sku={sku}
                  events={olderEventMap}
                  currentIso={currentIso}
                />
              )}
            </div>
          )}

          {!loading && !err && (!currentYearData || !previousYearData || !olderYearData) && (
            <p className="py-6 text-center text-sm text-slate-500">
              Noch keine Verkaufsdaten für den Jahresvergleich.
            </p>
          )}
        </section>

        <InventoryTableSection
          data={inventory.data}
          loading={inventory.loading}
          error={inventory.error}
          onReload={inventory.reload}
          selectedSku={sku}
          onSelectSku={setSku}
        />
    </div>
  );
}

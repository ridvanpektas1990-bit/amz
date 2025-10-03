"use client";

import { useEffect, useMemo, useState } from "react";
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

type SkuOption = { value: string; label: string };

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

// Label rechts neben einer vertikalen ReferenceLine
function RLRightLabel(props: any) {
  const { viewBox, value, fill = "#dc2626", dx = 8, dy = 12, fontSize = 10 } = props || {};
  const x = (viewBox?.x ?? 0) + dx;
  const y = (viewBox?.y ?? 0) + dy;
  return (
    <text x={x} y={y} textAnchor="start" dominantBaseline="middle" fontSize={fontSize} fill={fill}>
      {value}
    </text>
  );
}

// ISO: Montag einer ISO-Woche finden
function isoMondayOfWeek(isoYear: number, isoWeek: number): Date {
  const jan4 = new Date(Date.UTC(isoYear, 0, 4));
  const dayNr = (jan4.getUTCDay() + 6) % 7; // Mo=0..So=6
  const mondayW1 = new Date(jan4);
  mondayW1.setUTCDate(jan4.getUTCDate() - dayNr);
  const mondayTarget = new Date(mondayW1);
  mondayTarget.setUTCDate(mondayW1.getUTCDate() + (isoWeek - 1) * 7);
  return mondayTarget;
}

function dateToISOUTC(d: Date): string {
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

// ISO-Woche + N Wochen -> neue ISO (Jahr, KW)
function addWeeksToISO(isoYear: number, isoWeek: number, add: number) {
  const monday = isoMondayOfWeek(isoYear, isoWeek);
  monday.setUTCDate(monday.getUTCDate() + add * 7);
  const iso = isoWeekFromDateISO(dateToISOUTC(monday));
  return { year: monday.getUTCFullYear(), week: iso.isoWeek };
}

/* ===== Einzeljahres-Chart ===== */
function YearChart({
  data,
  year,
  yMax,
  sku,
  events,
  prevYearWeekTotals,
  currentIso,
  inventoryLeft,
}: {
  data: Point[];
  year: number;
  yMax: number;
  sku: string;
  events: EventsForYear | null;
  prevYearWeekTotals?: Map<number, number> | null;
  currentIso?: { year: number; week: number } | null;
  inventoryLeft?: number | null;
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
  const cutoffWeek2025 = useMemo(() => {
    if (year !== 2025) return null;
    const wkNow = currentIso?.week ?? 53;
    const maxWithData = data.reduce((m, p) => (p.total > 0 ? Math.max(m, p.isoWeek) : m), 0);
    const cutoff = Math.min(wkNow, Math.max(maxWithData, 0));
    return cutoff > 0 ? cutoff : null;
  }, [year, currentIso, data]);

  // YTD 2025 vs 2024
  const ytd = useMemo(() => {
    if (year !== 2025) return null;
    if (!cutoffWeek2025) return null;

    const sum2025 = data
      .filter((p) => p.isoWeek <= cutoffWeek2025)
      .reduce((acc, p) => acc + (p.total || 0), 0);

    let sum2024: number | null = null;
    if (prevYearWeekTotals) {
      let s = 0;
      for (let w = 1; w <= cutoffWeek2025; w++) {
        s += Math.max(0, prevYearWeekTotals.get?.(w) ?? 0);
      }
      sum2024 = s;
    }

    let pct: number | null = null;
    if (sum2024 !== null) {
      pct = sum2024 === 0 ? (sum2025 === 0 ? 0 : 100) : ((sum2025 - sum2024) / sum2024) * 100;
    }
    return { cutoff: cutoffWeek2025, sum2025, sum2024, pct };
  }, [year, cutoffWeek2025, data, prevYearWeekTotals]);

  const ytdColorClass = useMemo(() => {
    if (!ytd || ytd.pct === null) return "text-gray-600";
    return ytd.pct > 0 ? "text-green-600" : ytd.pct < 0 ? "text-red-600" : "text-gray-600";
  }, [ytd]);

  // Farb-Logik für Bars:
  const colorForBar = (p: Point): string => {
    if (year === 2025 && prevYearWeekTotals) {
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

  // 2025 Wochen (für OOS)
  const weekTotals2025 = useMemo(() => {
    const m = new Map<number, number>();
    if (year === 2025) {
      data.forEach((p) => m.set(p.isoWeek, p.total || 0));
    }
    return m;
  }, [data, year]);

  // --- OOS-Schätzung ---
  const oosForecast = useMemo(() => {
    if (year !== 2025) return null;
    if (!currentIso) return null;
    if (inventoryLeft == null) return null;

    let remaining = inventoryLeft;
    if (remaining <= 0) return { weeks: 0, weekKw: currentIso.week };

    let weeks = 0;
    let hitWeekKw: number | null = null;

    if (prevYearWeekTotals) {
      for (let w = currentIso.week + 1; w <= 53; w++) {
        const s = Math.max(0, prevYearWeekTotals.get?.(w) ?? 0);
        remaining -= s;
        weeks += 1;
        if (remaining <= 0) {
          hitWeekKw = w;
          break;
        }
      }
    }

    if (remaining > 0) {
      for (let w = 1; w <= 53; w++) {
        const s = Math.max(0, weekTotals2025.get(w) ?? 0);
        remaining -= s;
        weeks += 1;
        if (remaining <= 0) {
          hitWeekKw = w;
          break;
        }
      }
    }

    if (remaining > 0) return { weeks: -1, weekKw: null };
    return { weeks, weekKw: hitWeekKw };
  }, [year, currentIso, inventoryLeft, prevYearWeekTotals, weekTotals2025]);

  const oosTextAndColor = useMemo(() => {
    if (year !== 2025 || !sku || inventoryLeft == null) return null;
    if (!oosForecast) return null;
    const { weeks, weekKw } = oosForecast;
    if (weeks === 0) return { text: "OOS: jetzt", cls: "text-red-600" };
    if (weeks === -1) return { text: "OOS-Prognose > 1 Jahr", cls: "text-green-600" };
    const cls = weeks <= 4 ? "text-red-600" : weeks <= 8 ? "text-yellow-600" : "text-green-600";
    const kwPart = weekKw ? ` (KW ${weekKw})` : "";
    return { text: `OOS in ${weeks} ${weeks === 1 ? "Woche" : "Wochen"}${kwPart}`, cls };
  }, [oosForecast, year, sku, inventoryLeft]);

  /* === Hover-Hotspot + SVG-Tooltip für OOS === */
  const [oosTipVisible, setOosTipVisible] = useState(false);

  const oosLabel = useMemo(
    () => (oosForecast?.weekKw ? labelByWeek.get(oosForecast.weekKw) ?? null : null),
    [oosForecast, labelByWeek]
  );

  const oosHoverLines: string[] = useMemo(() => {
    if (!oosForecast?.weekKw) return [];
    return [
      `Basierend auf aktuellen Sales-Daten wirst du`,
      `in KW ${oosForecast.weekKw} ausverkauft sein,`,
      `wenn keine Nachbestellung erfolgt.`,
    ];
  }, [oosForecast]);

  return (
    <section className="mb-8">
      {/* HEADER: Titel zentriert, links YTD, rechts Lager/OOS */}
      <div className="grid grid-cols-3 items-start mb-2">
        {/* LINKS: YTD */}
        <div className="flex flex-col">
          {year === 2025 && ytd && (
            <div className="mt-1 text-sm">
              <div className="text-[11px] uppercase tracking-wide text-gray-500">
                YTD Jahresvergleich bis KW {ytd.cutoff}
              </div>
              <div className="leading-tight">
                <span className="text-base font-semibold">{nf.format(ytd.sum2025)} Stk</span>
                {ytd.pct !== null && (
                  <span className="ml-2 text-sm">
                    <span className={ytdColorClass}>
                      {ytd.pct > 0 ? "▲ +" : ytd.pct < 0 ? "▼ " : "±"}
                      {Math.abs(ytd.pct).toFixed(1).replace(".", ",")}% 
                    </span>{" "}
                    vs. 2024 {ytd.sum2024 !== null ? `(${nf.format(ytd.sum2024)} Stk)` : ""}
                  </span>
                )}
              </div>
            </div>
          )}
        </div>

        {/* MITTE: Titel */}
        <h2 className="justify-self-center text-lg font-semibold">
          {year}{sku ? ` · ${sku}` : ""}
        </h2>

        {/* RECHTS: Lager/OOS */}
        {year === 2025 && sku && typeof inventoryLeft === "number" && (
          <div className="justify-self-end text-right">
            <div className="text-[11px] uppercase tracking-wide text-gray-500">Auf Lager</div>
            <div className="leading-none">
              <span className="text-3xl font-extrabold">{nf.format(inventoryLeft)}</span>
              <span className="ml-1 text-sm font-semibold text-gray-500">Stk</span>
            </div>
            {oosTextAndColor && (
              <div className={`mt-1 text-sm font-medium ${oosTextAndColor.cls}`}>
                {oosTextAndColor.text}
              </div>
            )}
          </div>
        )}
      </div>

      {/* Chart */}
      <div className="w-full h-[400px]">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart
            data={data}
            syncId="kw-sync"
            margin={{ top: 28, right: 16, left: 16, bottom: 8 }}
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
                      label={{ value: "Heute", position: "top", fontSize: 10, fill: "#16a34a" }}
                    />
                  );
                })()
              : null}

            {/* OOS-Linie (2025) + Hover-Hotspot & SVG-Tooltip */}
            {year === 2025 && oosForecast?.weekKw
              ? (() => {
                  const oosKW = oosForecast.weekKw!;
                  const xLabel = labelByWeek.get(oosKW);
                  if (!xLabel) return null;

                  return (
                    <>
                      <ReferenceLine
                        x={xLabel}
                        xAxisId={0}
                        ifOverflow="extendDomain"
                        stroke="#dc2626"
                        strokeWidth={2}
                        strokeDasharray="2 2"
                        label={<RLRightLabel value={`KW ${oosKW} OOS`} fill="#dc2626" dx={8} dy={12} fontSize={10} />}
                      />
                      {/* Hotzone + Tooltip als SVG über Customized */}
                      <Customized
                        key="oos-hotspot"
                        component={(props: any) => {
                          const axisMap = props?.xAxisMap || {};
                          const axis = (Object.values(axisMap) as any[])[0];
                          if (!axis || !axis.scale) return null;

                          // x-Koordinate der OOS-Kategorie bestimmen
                          const xLocal = axis.scale(xLabel); // relative zum Plotbereich
                          if (typeof xLocal !== "number") return null;

                          const left = props?.offset?.left ?? 0;
                          const top = props?.offset?.top ?? 0;
                          const bottom = props?.offset?.bottom ?? 0;
                          const height = props?.height ?? 0;
                          const plotHeight = height - top - bottom;

                          const x = left + xLocal;

                          const tooltipW = 280;
                          const tooltipH = 48;

                          return (
                            <g>
                              {/* Hotzone: breiter, transparenter Streifen, damit Hover sicher triggert */}
                              <rect
                                x={x - 10}
                                y={top}
                                width={20}
                                height={plotHeight}
                                fill="transparent"
                                onMouseEnter={() => setOosTipVisible(true)}
                                onMouseLeave={() => setOosTipVisible(false)}
                                style={{ cursor: "help" }}
                              />

                              {/* Tooltip als SVG-Overlay */}
                              {oosTipVisible && (
                                <g pointerEvents="none">
                                  <rect
                                    x={Math.min(x + 10, (props?.width ?? 0) - tooltipW - 4)}
                                    y={top + 6}
                                    width={tooltipW}
                                    height={tooltipH}
                                    rx={6}
                                    ry={6}
                                    fill="white"
                                    stroke="#ddd"
                                    opacity={0.98}
                                  />
                                  <text
                                    x={Math.min(x + 18, (props?.width ?? 0) - tooltipW + 4)}
                                    y={top + 22}
                                    fontSize={12}
                                    fill="#111827"
                                  >
                                    <tspan x={Math.min(x + 18, (props?.width ?? 0) - tooltipW + 4)} dy={0}>
                                      {oosHoverLines[0] ?? ""}
                                    </tspan>
                                    <tspan x={Math.min(x + 18, (props?.width ?? 0) - tooltipW + 4)} dy={16}>
                                      {oosHoverLines[1] ?? ""}
                                    </tspan>
                                    <tspan x={Math.min(x + 18, (props?.width ?? 0) - tooltipW + 4)} dy={16}>
                                      {oosHoverLines[2] ?? ""}
                                    </tspan>
                                  </text>
                                </g>
                              )}
                            </g>
                          );
                        }}
                      />
                    </>
                  );
                })()
              : null}

            {/* Zukünftige Events */}
            {(events?.futureLines || []).map((f, i) => {
              const xLabel = labelByWeek.get(f.week);
              if (!xLabel) return null;
              return (
                <ReferenceLine
                  key={`${f.week}-${i}`}
                  x={xLabel}
                  xAxisId={0}
                  ifOverflow="extendDomain"
                  stroke="#3809a7ff"
                  strokeWidth={2}
                  strokeDasharray="4 2"
                  label={{ value: f.name, position: "top", fontSize: 10, fill: "#3809a7ff" }}
                />
              );
            })}
          </BarChart>
        </ResponsiveContainer>
      </div>
    </section>
  );
}

/* ===== Page ===== */
export default function DashboardPage() {
  const [y2025, setY2025] = useState<Point[] | null>(null);
  const [y2024, setY2024] = useState<Point[] | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  const [skus, setSkus] = useState<SkuOption[] | null>(null);
  const [sku, setSku] = useState<string>("");
  const [skuLoadErr, setSkuLoadErr] = useState<string | null>(null);
  const [skuLoading, setSkuLoading] = useState<boolean>(true);

  const [ev2025, setEv2025] = useState<RawEvent[] | null>(null);
  const [ev2024, setEv2024] = useState<RawEvent[] | null>(null);

  // Inventory
  const [inventoryLeft, setInventoryLeft] = useState<number | null>(null);
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
        const r = await fetch(`/api/metrics/skus?table=vw_amazon_fees_orders&sku_col=seller_sku`, { cache: "no-store" });
        const j = await r.json();
        if (!r.ok || !j.ok) throw new Error(j?.error || "SKU-Fehler");
        const list: SkuOption[] = (j.skus as any[]).map((v) =>
          typeof v === "string" ? { value: v, label: v.trim() } : v
        );
        setSkus(list);
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
    (async () => {
      setLoading(true);
      setErr(null);
      try {
        const qs = sku ? `&sku=${encodeURIComponent(sku)}` : "";
        const [r25, r24] = await Promise.all([
          fetch(`/api/metrics/orders-per-week?year=2025&fixed=1${qs}`, { cache: "no-store" }),
          fetch(`/api/metrics/orders-per-week?year=2024&fixed=1${qs}`, { cache: "no-store" }),
        ]);
        const j25 = await r25.json();
        const j24 = await r24.json();
        if (!r25.ok || !j25.ok) throw new Error(j25?.error || "Fehler 2025");
        if (!r24.ok || !j24.ok) throw new Error(j24?.error || "Fehler 2024");
        setY2025(j25.points as Point[]);
        setY2024(j24.points as Point[]);
      } catch (e: any) {
        setErr(e.message);
      } finally {
        setLoading(false);
      }
    })();
  }, [sku]);

  // Events je Jahr laden
  useEffect(() => {
    (async () => {
      try {
        const [e25, e24] = await Promise.all([
          fetch(`/api/events?year=2025`, { cache: "no-store" }),
          fetch(`/api/events?year=2024`, { cache: "no-store" }),
        ]);
        const j25 = await e25.json();
        const j24 = await e24.json();
        if (!e25.ok || j25.error) throw new Error(j25?.error || "Events 2025 Fehler");
        if (!e24.ok || j24.error) throw new Error(j24?.error || "Events 2024 Fehler");
        setEv2025(j25.events as RawEvent[]);
        setEv2024(j24.events as RawEvent[]);
      } catch (e) {
        console.warn("Events laden:", e);
        setEv2025([]);
        setEv2024([]);
      }
    })();
  }, []);

  // Inventory laden wenn SKU gesetzt
  useEffect(() => {
    (async () => {
      setInventoryLeft(null);
      setInventoryErr(null);
      if (!sku) return;
      try {
        const r = await fetch(`/api/inventory?sku=${encodeURIComponent(sku)}`, { cache: "no-store" });
        const j = await r.json();
        if (!r.ok || !j.ok) throw new Error(j?.error || "Inventory-Fehler");
        const v =
          typeof j.inventory_left === "number"
            ? j.inventory_left
            : j.inventory_left != null
            ? Number(j.inventory_left)
            : null;
        setInventoryLeft(Number.isFinite(v) ? (v as number) : 0);
      } catch (e: any) {
        setInventoryErr(e?.message ?? "Unbekannter Fehler");
      }
    })();
  }, [sku]);

  const yMax = useMemo(() => {
    const m25 = y2025 ? Math.max(0, ...y2025.map((p) => p.total)) : 0;
    const m24 = y2024 ? Math.max(0, ...y2024.map((p) => p.total)) : 0;
    return Math.max(m25, m24);
  }, [y2025, y2024]);

  const evMap2025 = useMemo<EventsForYear | null>(
    () => (y2025 && ev2025 ? buildEventMappings(y2025, ev2025, todayISO) : null),
    [y2025, ev2025, todayISO]
  );

  const evMap2024 = useMemo<EventsForYear | null>(
    () => (y2024 && ev2024 ? buildEventMappings(y2024, ev2024, todayISO) : null),
    [y2024, ev2024, todayISO]
  );

  // Aktuelle ISO-Woche/Jahr
  const currentIso = useMemo(() => {
    const { isoWeek } = isoWeekFromDateISO(todayISO);
    const y = new Date(todayISO + "T00:00:00Z").getUTCFullYear();
    return { year: y, week: isoWeek };
  }, [todayISO]);

  // Vorjahres-Map
  const prev2024Map = useMemo(() => {
    return y2024 ? new Map<number, number>(y2024.map((p) => [p.isoWeek, p.total])) : null;
  }, [y2024]);

  // Header-Zahlenformat
  const nfTop = useMemo(() => new Intl.NumberFormat("de-DE"), []);

  // 2025 Wochen-Map
  const weekTotals2025Map = useMemo(() => {
    return y2025 ? new Map<number, number>(y2025.map((p) => [p.isoWeek, p.total || 0])) : null;
  }, [y2025]);

  // Reorder-Plan (6 Monate) – korrekt mit Jahreswechseln
  const reorderPlanTop = useMemo(() => {
    if (!sku || inventoryLeft == null) return null;
    if (!currentIso || !prev2024Map || !weekTotals2025Map) return null;

    type YearTag = 2024 | 2025;
    const demandOf = (tag: YearTag, w: number) =>
      Math.max(0, (tag === 2024 ? prev2024Map.get(w) : weekTotals2025Map.get(w)) ?? 0);

    let remaining = inventoryLeft;
    let w = currentIso.week;
    let tag: YearTag = 2024;
    let elapsed = 0;

    for (let guard = 0; guard < 500 && remaining > 0; guard++) {
      w += 1;
      if (w > 53) { w = 1; tag = tag === 2024 ? 2025 : 2024; }
      remaining -= demandOf(tag, w);
      elapsed += 1;
    }

    const oosIso = addWeeksToISO(currentIso.year, currentIso.week, Math.max(elapsed, 0));
    const oosWeek = oosIso.week;
    const oosYear = oosIso.year;

    let need = 0;
    let tw = w;
    let ttag: YearTag = tag;
    for (let i = 0; i < 26; i++) {
      tw += 1;
      if (tw > 53) { tw = 1; ttag = ttag === 2024 ? 2025 : 2024; }
      need += demandOf(ttag, tw);
    }

    const newOOS = addWeeksToISO(oosYear, oosWeek, 26);

    return {
      oosWeek,
      oosYear,
      reorderQty: need,
      newOOSWeek: newOOS.week,
      newOOSYear: newOOS.year,
    };
  }, [sku, inventoryLeft, currentIso, prev2024Map, weekTotals2025Map]);

  // Countdown (optional)
  type UpcomingEvent = RawEvent & { days: number };
  const futureEvents = useMemo<UpcomingEvent[]>(() => {
    const all: RawEvent[] = [...(ev2024 || []), ...(ev2025 || [])];
    const t = new Date(todayISO + "T00:00:00Z").getTime();
    return all
      .map((e) => ({ ...e, days: Math.ceil((new Date(e.event_date + "T00:00:00Z").getTime() - t) / 86400000) }))
      .filter((e) => e.days > 0)
      .sort((a, b) => a.days - b.days)
      .slice(0, 2);
  }, [ev2024, ev2025, todayISO]);

  const emojiFor = (name: string) => {
    const n = name.toLowerCase();
    if (n.includes("prime")) return "📦";
    if (n.includes("black friday")) return "❄️";
    return "⏳";
  };
  const colorFor = (days: number) =>
    days <= 7 ? "text-red-600" : days <= 30 ? "text-yellow-600" : "text-green-600";

  return (
    <div className="mx-auto max-w-6xl px-4 py-8" style={{ fontFamily: "system-ui, sans-serif" }}>
      {/* PAGE-HEADER */}
      <div className="mb-4 md:flex md:items-start md:justify-between md:gap-6">
        <div className="flex-1">
          <h1 className="text-2xl font-semibold">Dashboard · Jahresvergleich</h1>
          <p className="text-sm text-gray-600">
            Quelle: <code>vw_amazon_fees_orders</code> · Metrik: <b>quantity</b> · Datum: <b>purchase_date_berlin</b>
          </p>
          {futureEvents.length > 0 && (
            <div className="mt-2 flex flex-wrap gap-6 text-sm">
              {futureEvents.map((e, i) => (
                <span key={i} className={colorFor(e.days)}>
                  {emojiFor(e.event_name)} Noch {e.days} Tage bis {e.event_name}
                </span>
              ))}
            </div>
          )}
        </div>

        {/* RECHTS: Bestell-/OOS-Block */}
        {reorderPlanTop && sku && (
          <div className="mt-3 md:mt-0 text-right shrink-0 min-w-[300px]">
            <div className="text-[11px] uppercase tracking-wide text-gray-500">Bestellplanung</div>
            <div className="mt-1 text-sm text-gray-800 space-y-1">
              <div>
                <span className="font-semibold">Termin voraussichtlich OOS:</span>{" "}
                KW {reorderPlanTop.oosWeek} {reorderPlanTop.oosYear}
              </div>
              <div>
                <span className="font-semibold">Wie viele bestellen (6 Monate):</span>{" "}
                {nfTop.format(reorderPlanTop.reorderQty)} Stk
              </div>
              <div>
                <span className="font-semibold">OOS mit neuer Lieferung:</span>{" "}
                KW {reorderPlanTop.newOOSWeek} {reorderPlanTop.newOOSYear}
              </div>
            </div>
          </div>
        )}
      </div>

      {/* SKU-Filter */}
      <div className="mb-4 flex flex-col gap-2">
        <div className="flex items-center gap-3">
          <label className="text-sm text-gray-700">SKU:</label>
          <select
            className="border rounded px-2 py-1 text-sm"
            value={sku}
            onChange={(e) => setSku(e.target.value)}
            disabled={skuLoading || !skus}
          >
            <option value="">Alle SKUs</option>
            {skus?.map((o) => (
              <option key={o.value} value={o.value}>{o.label}</option>
            ))}
          </select>
          {!!sku && (
            <button className="text-xs text-blue-600 underline" onClick={() => setSku("")}>
              Zurücksetzen
            </button>
          )}
        </div>
        {!skuLoading && skus && skus.length === 0 && (
          <div className="flex items-center gap-3">
            <label className="text-sm text-gray-700">SKU manuell:</label>
            <input
              className="border rounded px-2 py-1 text-sm w-64"
              placeholder="SKU exakt eintippen…"
              value={sku}
              onChange={(e) => setSku(e.target.value)}
              spellCheck={false}
            />
            <small className="text-gray-500">Tipp: Groß/Kleinschreibung &amp; Leerzeichen exakt wie in der DB.</small>
          </div>
        )}
      </div>

      {loading && <div>lädt…</div>}
      {err && <div style={{ color: "crimson" }}>Fehler: {err}</div>}

      {y2025 && y2024 && (
        <>
          <YearChart
            data={y2025}
            year={2025}
            yMax={Math.max(
              y2025 ? Math.max(0, ...y2025.map((p) => p.total)) : 0,
              y2024 ? Math.max(0, ...y2024.map((p) => p.total)) : 0
            )}
            sku={sku}
            events={evMap2025}
            prevYearWeekTotals={new Map(y2024.map((p) => [p.isoWeek, p.total]))}
            currentIso={(() => {
              const { isoWeek } = isoWeekFromDateISO(todayISO);
              const y = new Date(todayISO + "T00:00:00Z").getUTCFullYear();
              return { year: y, week: isoWeek };
            })()}
            inventoryLeft={inventoryLeft}
          />
          <YearChart
            data={y2024}
            year={2024}
            yMax={Math.max(
              y2025 ? Math.max(0, ...y2025.map((p) => p.total)) : 0,
              y2024 ? Math.max(0, ...y2024.map((p) => p.total)) : 0
            )}
            sku={sku}
            events={evMap2024}
            currentIso={(() => {
              const { isoWeek } = isoWeekFromDateISO(todayISO);
              const y = new Date(todayISO + "T00:00:00Z").getUTCFullYear();
              return { year: y, week: isoWeek };
            })()}
          />
        </>
      )}
    </div>
  );
}

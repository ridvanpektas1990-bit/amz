"use client";

import { useEffect, useMemo, useState } from "react";
import type { DayRunStatus, PipelineSeries } from "@/lib/monitoring";

type FreshnessSource = {
  key: string;
  label: string;
  lastDataAt: string | null;
  ageDays: number | null;
  stale: boolean;
  detail: string | null;
  status: string | null;
};

type MonitoringResponse = {
  ok: boolean;
  error?: string;
  todayISO?: string;
  dayKeys?: string[];
  pipelines?: PipelineSeries[];
  freshness?: {
    overall: "ok" | "warning" | "critical";
    warnings: string[];
    sources: FreshnessSource[];
  };
  recentRuns?: Array<{
    status: string | null;
    startedAt: string | null;
    finishedAt: string | null;
    day: string | null;
    runLog: string | null;
    periodYear: number | null;
    periodMonth: number | null;
  }>;
};

const statusStyle: Record<DayRunStatus, { cell: string; label: string }> = {
  success: { cell: "bg-emerald-500", label: "OK" },
  error: { cell: "bg-rose-500", label: "Fehler" },
  missing: { cell: "bg-slate-200", label: "fehlt" },
};

function formatDay(iso: string): string {
  const [, m, d] = iso.split("-");
  return `${d}.${m}`;
}

function formatWhen(value: string | null): string {
  if (!value) return "–";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("de-DE", {
    timeZone: "Europe/Berlin",
    day: "2-digit",
    month: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}

export default function MonitoringPage() {
  const [data, setData] = useState<MonitoringResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [selected, setSelected] = useState<{
    pipeline: string;
    day: string;
    detail: string | null;
    status: DayRunStatus;
    lastAt: string | null;
  } | null>(null);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      setLoading(true);
      try {
        const response = await fetch("/api/monitoring", { cache: "no-store" });
        const json = (await response.json()) as MonitoringResponse;
        if (!response.ok || !json.ok) throw new Error(json.error || "Monitoring nicht ladbar");
        if (!cancelled) {
          setData(json);
          setError(null);
        }
      } catch (err: unknown) {
        if (!cancelled) setError(err instanceof Error ? err.message : String(err));
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const dayKeys = data?.dayKeys || [];
  const pipelines = data?.pipelines || [];
  const overall = data?.freshness?.overall || "ok";

  const overallTone = useMemo(() => {
    if (overall === "critical") return "border-rose-200 bg-rose-50 text-rose-900";
    if (overall === "warning") return "border-amber-200 bg-amber-50 text-amber-950";
    return "border-emerald-200 bg-emerald-50 text-emerald-900";
  }, [overall]);

  return (
    <div className="mx-auto max-w-6xl px-4 py-5 md:py-6">
      <div className="mb-4 flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 className="text-lg font-semibold tracking-tight text-slate-950 md:text-xl">Monitoring</h1>
          <p className="mt-0.5 text-sm text-slate-500">
            Daily Status der Sync-Pipelines (letzte {dayKeys.length || 14} Tage, Berlin)
          </p>
        </div>
        {data?.todayISO && (
          <div className={`rounded-full border px-3 py-1 text-xs font-medium ${overallTone}`}>
            Datenstand heute: {overall === "ok" ? "ok" : overall === "warning" ? "Warnung" : "kritisch"}
          </div>
        )}
      </div>

      {loading && (
        <div className="space-y-3">
          <div className="h-24 animate-pulse rounded-2xl bg-slate-100" />
          <div className="h-48 animate-pulse rounded-2xl bg-slate-100" />
        </div>
      )}

      {error && (
        <div className="rounded-xl border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-800">
          {error}
        </div>
      )}

      {!loading && !error && data && (
        <>
          <section className="mb-4 grid gap-2 sm:grid-cols-2 lg:grid-cols-4">
            {(data.freshness?.sources || []).map((source) => (
              <div
                key={source.key}
                className="rounded-2xl border border-slate-200 bg-white px-3 py-3 text-center shadow-sm"
              >
                <div className="text-[11px] font-medium uppercase tracking-wide text-slate-500">
                  {source.label}
                </div>
                <div
                  className={`mt-1 text-sm font-semibold ${
                    source.stale ? "text-rose-700" : "text-slate-950"
                  }`}
                >
                  {source.lastDataAt
                    ? source.ageDays === 0
                      ? "aktuell"
                      : `${source.ageDays} Tage alt`
                    : "keine Daten"}
                </div>
                <div className="mt-0.5 text-[10px] text-slate-400">
                  {source.detail || source.lastDataAt || "–"}
                </div>
              </div>
            ))}
          </section>

          <section className="mb-4 overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm">
            <div className="border-b border-slate-200 px-3 py-2.5">
              <h2 className="text-sm font-semibold text-slate-950">Daily Runs</h2>
              <p className="text-xs text-slate-500">
                Grün = gelaufen &amp; ok · Rot = Fehler · Grau = kein Lauf
              </p>
            </div>
            <div className="overflow-x-auto p-3">
              <table className="min-w-[720px] w-full border-collapse text-left text-xs">
                <thead>
                  <tr>
                    <th className="sticky left-0 bg-white px-2 py-1.5 text-left font-semibold text-slate-600">
                      Pipeline
                    </th>
                    {dayKeys.map((day) => (
                      <th key={day} className="px-1 py-1.5 text-center font-medium text-slate-500">
                        {formatDay(day)}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {pipelines.map((pipeline) => (
                    <tr key={pipeline.key} className="border-t border-slate-100">
                      <td className="sticky left-0 bg-white px-2 py-2 font-medium text-slate-800">
                        {pipeline.label}
                      </td>
                      {dayKeys.map((day) => {
                        const cell = pipeline.days[day];
                        const style = statusStyle[cell?.status || "missing"];
                        return (
                          <td key={day} className="px-1 py-2 text-center">
                            <button
                              type="button"
                              title={`${pipeline.label} · ${day}: ${style.label}`}
                              onClick={() =>
                                setSelected({
                                  pipeline: pipeline.label,
                                  day,
                                  detail: cell?.detail || null,
                                  status: cell?.status || "missing",
                                  lastAt: cell?.lastAt || null,
                                })
                              }
                              className={`mx-auto block h-4 w-4 rounded-full ${style.cell} ring-2 ring-transparent transition hover:ring-slate-300`}
                            />
                          </td>
                        );
                      })}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {selected && (
              <div className="border-t border-slate-100 bg-slate-50 px-3 py-2.5 text-xs text-slate-700">
                <div className="font-semibold text-slate-900">
                  {selected.pipeline} · {formatDay(selected.day)} · {statusStyle[selected.status].label}
                </div>
                <div className="mt-0.5 text-slate-500">
                  Letzter Lauf: {formatWhen(selected.lastAt)}
                </div>
                <p className="mt-1 whitespace-pre-wrap text-slate-600">
                  {selected.detail ||
                    (selected.status === "missing"
                      ? "An diesem Tag kein Lauf protokolliert."
                      : "Keine Detailnotiz.")}
                </p>
              </div>
            )}
          </section>

          <section className="rounded-2xl border border-slate-200 bg-white shadow-sm">
            <div className="border-b border-slate-200 px-3 py-2.5">
              <h2 className="text-sm font-semibold text-slate-950">Letzte Läufe</h2>
            </div>
            <ul className="divide-y divide-slate-100">
              {(data.recentRuns || []).length === 0 && (
                <li className="px-3 py-6 text-center text-sm text-slate-500">Noch keine etl_runs Einträge.</li>
              )}
              {(data.recentRuns || []).map((run, index) => (
                <li key={`${run.startedAt}-${index}`} className="flex items-start gap-3 px-3 py-2.5 text-sm">
                  <span
                    className={`mt-1 h-2.5 w-2.5 shrink-0 rounded-full ${
                      (run.status || "").toLowerCase() === "success" ? "bg-emerald-500" : "bg-rose-500"
                    }`}
                  />
                  <div className="min-w-0 flex-1">
                    <div className="font-medium text-slate-900">
                      {formatWhen(run.finishedAt || run.startedAt)}
                      {run.periodYear && run.periodMonth
                        ? ` · Periode ${run.periodMonth}/${run.periodYear}`
                        : ""}
                    </div>
                    <div className="truncate text-xs text-slate-500">{run.runLog || "–"}</div>
                  </div>
                  <span className="shrink-0 text-xs font-medium uppercase text-slate-500">
                    {run.status || "–"}
                  </span>
                </li>
              ))}
            </ul>
          </section>
        </>
      )}
    </div>
  );
}

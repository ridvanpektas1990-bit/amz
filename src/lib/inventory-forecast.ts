export const FORECAST_GROWTH_PRIOR_UNITS = 100;

export type ForecastDemandSource = "seasonal" | "recent" | "none";

export function calculatePositiveGrowthFactor(
  currentUnits: number,
  previousUnits: number,
  stabilizationUnits = FORECAST_GROWTH_PRIOR_UNITS,
): number {
  const current = Math.max(0, Number(currentUnits) || 0);
  const previous = Math.max(0, Number(previousUnits) || 0);
  if (previous <= 0 || current <= previous) return 1;
  return (current + stabilizationUnits) / (previous + stabilizationUnits);
}

export function chooseForecastDemand({
  seasonalDemand,
  recentDemand,
  growthFactor = 1,
}: {
  seasonalDemand: number;
  recentDemand: number;
  growthFactor?: number;
}): { demand: number; source: ForecastDemandSource } {
  const seasonal = Math.max(0, Number(seasonalDemand) || 0);
  const recent = Math.max(0, Number(recentDemand) || 0);
  const growth = Math.max(1, Number(growthFactor) || 1);

  if (seasonal > 0) return { demand: seasonal * growth, source: "seasonal" };
  if (recent > 0) return { demand: recent, source: "recent" };
  return { demand: 0, source: "none" };
}

export function periodsUntilOos(
  inventory: number,
  demands: Iterable<number>,
): number | null {
  let remaining = Math.max(0, Number(inventory) || 0);
  if (remaining <= 0) return 0;

  let period = 0;
  for (const rawDemand of demands) {
    period += 1;
    remaining -= Math.max(0, Number(rawDemand) || 0);
    if (remaining <= 0) return period;
  }
  return null;
}

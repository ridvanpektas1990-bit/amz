"use client";

export default function ShowInactiveListingsToggle({
  checked,
  onChange,
  activeCount,
  inactiveCount,
}: {
  checked: boolean;
  onChange: (next: boolean) => void;
  activeCount?: number | null;
  inactiveCount?: number | null;
}) {
  const counts =
    typeof activeCount === "number" && typeof inactiveCount === "number"
      ? ` (${inactiveCount})`
      : "";

  return (
    <label className="inline-flex cursor-pointer items-center gap-2 whitespace-nowrap rounded-full border border-slate-200/90 bg-white/80 px-3 py-1.5 text-xs text-slate-600 shadow-sm backdrop-blur-[2px] transition hover:border-slate-300 hover:bg-white">
      <input
        type="checkbox"
        className="h-3.5 w-3.5 rounded border-slate-300 text-slate-900 focus:ring-slate-300"
        checked={checked}
        onChange={(event) => onChange(event.target.checked)}
      />
      <span>
        Auch inaktive{counts}
      </span>
    </label>
  );
}

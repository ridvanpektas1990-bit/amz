"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useState } from "react";

const NAV = [
  { href: "/dashboard", label: "Dashboard" },
  { href: "/nachbestellung", label: "Nachbestellung" },
  { href: "/sku-stammdaten", label: "SKU-Stammdaten" },
  { href: "/monitoring", label: "Monitoring" },
] as const;

export default function AppShell({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const [open, setOpen] = useState(false);

  return (
    <div className="min-h-screen bg-slate-50 text-slate-900">
      <div className="flex min-h-screen">
        <aside
          className={`fixed inset-y-0 left-0 z-40 flex w-56 flex-col border-r border-slate-200 bg-white transition-transform md:static md:translate-x-0 ${
            open ? "translate-x-0" : "-translate-x-full"
          }`}
        >
          <div className="flex h-14 items-center justify-between border-b border-slate-200 px-4">
            <Link href="/dashboard" className="text-sm font-semibold tracking-tight text-slate-950" onClick={() => setOpen(false)}>
              Amz Profit
            </Link>
            <button
              type="button"
              className="rounded-md px-2 py-1 text-xs text-slate-500 md:hidden"
              onClick={() => setOpen(false)}
            >
              Schließen
            </button>
          </div>
          <nav className="flex flex-1 flex-col gap-0.5 p-2">
            {NAV.map((item) => {
              const active = pathname === item.href || pathname.startsWith(`${item.href}/`);
              return (
                <Link
                  key={item.href}
                  href={item.href}
                  onClick={() => setOpen(false)}
                  className={`rounded-lg px-3 py-2 text-sm transition ${
                    active
                      ? "bg-slate-900 font-medium text-white"
                      : "text-slate-700 hover:bg-slate-100"
                  }`}
                >
                  {item.label}
                </Link>
              );
            })}
          </nav>
          <div className="border-t border-slate-200 px-4 py-3 text-[11px] text-slate-500">
            Bestand · Nachschub · Monitoring
          </div>
        </aside>

        {open && (
          <button
            type="button"
            aria-label="Menü schließen"
            className="fixed inset-0 z-30 bg-slate-900/20 md:hidden"
            onClick={() => setOpen(false)}
          />
        )}

        <div className="flex min-w-0 flex-1 flex-col">
          <header className="sticky top-0 z-20 flex h-14 items-center gap-3 border-b border-slate-200 bg-white/95 px-4 backdrop-blur md:hidden">
            <button
              type="button"
              className="rounded-md border border-slate-300 px-2.5 py-1.5 text-xs font-medium text-slate-700"
              onClick={() => setOpen(true)}
            >
              Menü
            </button>
            <span className="text-sm font-semibold text-slate-950">Amz Profit</span>
          </header>
          <main className="min-w-0 flex-1">{children}</main>
        </div>
      </div>
    </div>
  );
}

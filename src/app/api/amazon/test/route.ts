import { NextRequest, NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";

function must(name: string){ const v=process.env[name]; if(!v) throw new Error(`Missing env ${name}`); return v; }
async function refreshAccessToken(refreshToken: string){
  const body = new URLSearchParams({
    grant_type: "refresh_token",
    refresh_token: refreshToken,
    client_id: must("LWA_CLIENT_ID"),
    client_secret: must("LWA_CLIENT_SECRET"),
  });
  const r = await fetch("https://api.amazon.com/auth/o2/token", {
    method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded" },
    body,
  });
  if (!r.ok) throw new Error(`refresh_failed_${r.status}`);
  return r.json() as Promise<{ access_token: string; expires_in: number }>;
}

export async function GET(req: NextRequest) {
  const url = new URL(req.url);
  const region = (url.searchParams.get("region") ?? process.env.NEXT_PUBLIC_DEFAULT_REGION ?? "eu").toLowerCase();

  const sb = createClient(must("SUPABASE_URL"), must("SUPABASE_SERVICE_ROLE_KEY"), { auth: { persistSession: false } });
  // nimm die jüngste Verbindung für die Region
  const { data, error } = await sb
    .from("amazon_connections")
    .select("seller_id, refresh_token")
    .eq("region", region)
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) return NextResponse.json({ ok:false, error: error.message }, { status: 500 });
  if (!data?.refresh_token) return NextResponse.json({ ok:false, error:"no_refresh_token" }, { status: 400 });

  const t = await refreshAccessToken(data.refresh_token);
  return NextResponse.json({
    ok: true,
    seller_id: data.seller_id ?? null,
    access_token_preview: (t.access_token ?? "").slice(0, 14) + "...",
    expires_in: t.expires_in
  });
}

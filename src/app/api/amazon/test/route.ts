import { NextRequest, NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";
import { readTenantCookie } from "@/lib/amazon-tenant-cookie";

function must(name: string) {
  const value = process.env[name];
  if (!value) throw new Error(`Missing env ${name}`);
  return value;
}

async function refreshAccessToken(refreshToken: string) {
  const body = new URLSearchParams({
    grant_type: "refresh_token",
    refresh_token: refreshToken,
    client_id: must("LWA_CLIENT_ID"),
    client_secret: must("LWA_CLIENT_SECRET"),
  });
  const response = await fetch("https://api.amazon.com/auth/o2/token", {
    method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded" },
    body,
  });
  if (!response.ok) throw new Error(`refresh_failed_${response.status}`);
  return response.json() as Promise<{ expires_in: number }>;
}

export async function GET(req: NextRequest) {
  try {
    const url = new URL(req.url);
    const region = (
      url.searchParams.get("region") ??
      process.env.NEXT_PUBLIC_DEFAULT_REGION ??
      "eu"
    ).toLowerCase();
    const tenantId = readTenantCookie(
      req.cookies.get("amz_tenant")?.value,
      must("LWA_CLIENT_SECRET"),
    );

    if (!tenantId) {
      return NextResponse.json({ ok: false, error: "not_connected" }, { status: 401 });
    }

    const supabase = createClient(
      must("SUPABASE_URL"),
      must("SUPABASE_SERVICE_ROLE_KEY"),
      { auth: { persistSession: false } },
    );
    const { data, error } = await supabase
      .from("amazon_connections")
      .select("seller_id, refresh_token")
      .eq("tenant_id", tenantId)
      .eq("region", region)
      .maybeSingle();

    if (error) {
      return NextResponse.json(
        { ok: false, error: "connection_lookup_failed" },
        { status: 500 },
      );
    }
    if (!data?.refresh_token) {
      return NextResponse.json({ ok: false, error: "no_refresh_token" }, { status: 400 });
    }

    const token = await refreshAccessToken(data.refresh_token);
    return NextResponse.json({
      ok: true,
      seller_id: data.seller_id ?? null,
      expires_in: token.expires_in,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "unknown_error";
    const safeError = message.startsWith("refresh_failed_")
      ? message
      : "token_test_failed";
    return NextResponse.json({ ok: false, error: safeError }, { status: 500 });
  }
}

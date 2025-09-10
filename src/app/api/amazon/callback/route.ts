import { NextRequest, NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";

const supa = () =>
  createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_ROLE_KEY!, {
    auth: { persistSession: false },
  });

export async function GET(req: NextRequest) {
  const url = new URL(req.url);
  const code = url.searchParams.get("spapi_oauth_code");
  const returnedState = url.searchParams.get("state");

  const cookieState = req.cookies.get("amz_state")?.value;
  const region = (req.cookies.get("amz_region")?.value ?? "eu") as "eu" | "na" | "fe";

  if (!code) return NextResponse.json({ error: "Missing spapi_oauth_code" }, { status: 400 });
  if (!returnedState || returnedState !== cookieState) {
    return NextResponse.json({ error: "Invalid state" }, { status: 400 });
  }

  // Code -> Tokens (LWA)
  const tokenRes = await fetch("https://api.amazon.com/auth/o2/token", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      grant_type: "authorization_code",
      code,
      client_id: process.env.LWA_CLIENT_ID,
      client_secret: process.env.LWA_CLIENT_SECRET,
      redirect_uri: process.env.AMAZON_REDIRECT_URI,
    }),
  });

  const tokens = await tokenRes.json();
  if (!tokenRes.ok || !tokens?.refresh_token) {
    return NextResponse.json({ error: "Token exchange failed", detail: tokens }, { status: 400 });
  }

  // Refresh-Token in Supabase speichern
  const { error } = await supa().from("amazon_connections").insert({
    region,
    refresh_token: tokens.refresh_token,
  });

  if (error) {
    return NextResponse.json({ error: "DB insert failed", detail: error.message }, { status: 500 });
  }

  // Erfolg – einfache JSON-Antwort
  return NextResponse.json({ connected: true, region });
}

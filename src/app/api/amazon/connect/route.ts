import { NextResponse, NextRequest } from "next/server";
import crypto from "node:crypto";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function must(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env ${name}`);
  return v;
}

const REGION_HOST: Record<string, string> = {
  eu: "https://sellercentral-europe.amazon.com",
  na: "https://sellercentral.amazon.com",
  fe: "https://sellercentral.amazon.co.jp",
};

export async function GET(req: NextRequest) {
  const appId = must("AMAZON_APP_ID");
  // Deine .env nutzt REDIRECT_URI
  const redirectUri = must("REDIRECT_URI");

  // Region: URL-Param > NEXT_PUBLIC_DEFAULT_REGION > "eu"
  const regionParam = req.nextUrl.searchParams.get("region")?.toLowerCase();
  const region = (regionParam || process.env.NEXT_PUBLIC_DEFAULT_REGION || "eu").toLowerCase();
  const host = REGION_HOST[region] ?? REGION_HOST.eu;

  const state = crypto.randomBytes(16).toString("hex");

  const url = new URL(`${host}/apps/authorize/consent`);
  url.searchParams.set("application_id", appId);
  url.searchParams.set("redirect_uri", redirectUri);
  url.searchParams.set("state", state);

  // Beta-Auth, solange App im Draft/Review ist
  if (process.env.USE_BETA === "1") {
    url.searchParams.set("version", "beta");
  }

  const res = NextResponse.redirect(url.toString(), 302);

  // sicheres State-Cookie (in Prod mit HTTPS ok)
  res.cookies.set("amz_state", state, {
    httpOnly: true,
    secure: process.env.NODE_ENV === "production",
    sameSite: "lax",
    path: "/",
    maxAge: 10 * 60, // 10 min
  });

  return res;
}

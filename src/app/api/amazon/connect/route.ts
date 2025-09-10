import { NextRequest, NextResponse } from "next/server";
import crypto from "crypto";

const CONSENT_BASE = {
  eu: "https://sellercentral-europe.amazon.com",
  na: "https://sellercentral.amazon.com",
  fe: "https://sellercentral.amazon.co.jp",
} as const;

export async function GET(req: NextRequest) {
  const { searchParams } = new URL(req.url);
  const region = (searchParams.get("region") ?? "eu") as keyof typeof CONSENT_BASE;

  const state = crypto.randomBytes(16).toString("hex"); // CSRF
  const consentUrl = new URL("/apps/authorize/consent", CONSENT_BASE[region]);
  consentUrl.searchParams.set("application_id", process.env.AMAZON_APP_ID!);
  consentUrl.searchParams.set("state", state);
  consentUrl.searchParams.set("redirect_uri", process.env.AMAZON_REDIRECT_URI!);

  const res = NextResponse.redirect(consentUrl.toString(), { status: 302 });
  // State im Cookie merken
  res.cookies.set("amz_state", state, {
    httpOnly: true,
    sameSite: "lax",
    secure: false, // lokal ok; in Prod: true
    path: "/",
    maxAge: 60 * 10,
  });
  // Region auch merken
  res.cookies.set("amz_region", region, { httpOnly: true, sameSite: "lax", path: "/", maxAge: 60 * 10 });
  return res;
}

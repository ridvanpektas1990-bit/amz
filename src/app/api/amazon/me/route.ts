import { NextRequest, NextResponse } from "next/server";
import aws4 from "aws4";
import { loadAmazonConnection } from "@/lib/amazon-connection";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function must(name: string){ const v = process.env[name]; if(!v) throw new Error(`Missing env ${name}`); return v; }

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
  if (!r.ok) throw new Error(`LWA refresh failed ${r.status}`);
  return r.json() as Promise<{ access_token: string; expires_in: number }>;
}

export async function GET(req: NextRequest) {
  try {
    const url = new URL(req.url);
    const region = (url.searchParams.get("region") ?? process.env.NEXT_PUBLIC_DEFAULT_REGION ?? "eu").toLowerCase();

    // 1) Refresh-Token aus Supabase holen (jüngster Datensatz für die Region)
    const connectionResult = await loadAmazonConnection(req, region);
    if (!connectionResult.connection) {
      const status = connectionResult.error === "not_connected" ? 401 : 400;
      return NextResponse.json({ ok:false, error:connectionResult.error }, { status });
    }
    const data = connectionResult.connection;

    // 2) Access-Token holen
    const { access_token } = await refreshAccessToken(data.refresh_token);

    // 3) SigV4 – EU Endpoint
    const host = "sellingpartnerapi-eu.amazon.com";
    const path = "/sellers/v1/marketplaceParticipations";
    const urlStr = `https://${host}${path}`;

    const headers: Record<string,string> = {
      "x-amz-access-token": access_token,
      "accept": "application/json",
      "user-agent": "amz-profit/1.0",
    };

    const signed = aws4.sign(
      {
        host,
        path,
        service: "execute-api",
        region: "eu-west-1",
        method: "GET",
        headers,
      },
      {
        accessKeyId: must("AWS_ACCESS_KEY_ID"),
        secretAccessKey: must("AWS_SECRET_ACCESS_KEY"),
        // falls du STS/AssumeRole verwendest, hier zusätzlich sessionToken: must("AWS_SESSION_TOKEN")
      }
    );

    const resp = await fetch(urlStr, { method: "GET", headers: signed.headers as any, cache: "no-store" });
    const text = await resp.text();

    if (!resp.ok) {
      return NextResponse.json({ ok:false, status: resp.status, body: text.slice(0, 3000) }, { status: resp.status });
    }

    let json: any;
    try { json = JSON.parse(text); } catch { json = { raw: text }; }

    return NextResponse.json({ ok:true, seller_id: data.seller_id ?? null, marketplaces: json });
  } catch (e:any) {
    return NextResponse.json({ ok:false, error: e.message }, { status: 500 });
  }
}

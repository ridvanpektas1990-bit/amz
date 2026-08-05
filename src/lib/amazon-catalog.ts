import aws4 from "aws4";
import type { NextRequest } from "next/server";
import { loadAmazonConnection } from "@/lib/amazon-connection";

export type CatalogMetadata = {
  imageUrl: string | null;
  productName: string | null;
};

type CatalogItem = {
  asin?: string;
  images?: Array<{
    marketplaceId?: string;
    images?: Array<{ variant?: string; link?: string }>;
  }>;
  summaries?: Array<{ marketplaceId?: string; itemName?: string }>;
};

const CACHE_TTL_MS = 24 * 60 * 60 * 1000;
const catalogCache = new Map<string, { value: CatalogMetadata; expiresAt: number }>();

function must(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing env ${name}`);
  return value;
}

async function refreshAccessToken(refreshToken: string): Promise<string> {
  const response = await fetch("https://api.amazon.com/auth/o2/token", {
    method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "refresh_token",
      refresh_token: refreshToken,
      client_id: must("LWA_CLIENT_ID"),
      client_secret: must("LWA_CLIENT_SECRET"),
    }),
    cache: "no-store",
  });
  if (!response.ok) throw new Error(`LWA refresh failed ${response.status}`);
  const json = (await response.json()) as { access_token?: string };
  if (!json.access_token) throw new Error("LWA access token missing");
  return json.access_token;
}

function safeAmazonImage(url: string | undefined): string | null {
  if (!url) return null;
  try {
    const parsed = new URL(url);
    const allowed = parsed.protocol === "https:" && (
      parsed.hostname.endsWith("media-amazon.com") ||
      parsed.hostname.endsWith("ssl-images-amazon.com")
    );
    return allowed ? parsed.toString() : null;
  } catch {
    return null;
  }
}

export async function loadCatalogMetadata(
  req: NextRequest,
  asins: string[],
  marketplaceId: string,
): Promise<Map<string, CatalogMetadata>> {
  const result = new Map<string, CatalogMetadata>();
  const missing: string[] = [];
  const now = Date.now();

  for (const asin of [...new Set(asins.filter(Boolean))]) {
    const cached = catalogCache.get(asin);
    if (cached && cached.expiresAt > now) result.set(asin, cached.value);
    else missing.push(asin);
  }
  if (!missing.length) return result;

  const connectionResult = await loadAmazonConnection(req, "eu");
  if (!connectionResult.connection) return result;
  const accessToken = await refreshAccessToken(connectionResult.connection.refresh_token);
  const host = "sellingpartnerapi-eu.amazon.com";

  for (let start = 0; start < missing.length; start += 20) {
    const batch = missing.slice(start, start + 20);
    const query = new URLSearchParams({
      identifiers: batch.join(","),
      identifiersType: "ASIN",
      marketplaceIds: marketplaceId,
      includedData: "images,summaries",
      pageSize: String(batch.length),
    });
    const path = `/catalog/2022-04-01/items?${query.toString()}`;
    const signed = aws4.sign(
      {
        host,
        path,
        service: "execute-api",
        region: "eu-west-1",
        method: "GET",
        headers: {
          "x-amz-access-token": accessToken,
          accept: "application/json",
          "user-agent": "amz-profit/1.0",
        },
      },
      { accessKeyId: must("AWS_ACCESS_KEY_ID"), secretAccessKey: must("AWS_SECRET_ACCESS_KEY") },
    );
    const response = await fetch(`https://${host}${path}`, {
      headers: signed.headers as HeadersInit,
      cache: "no-store",
    });
    if (!response.ok) throw new Error(`Catalog Items failed ${response.status}`);
    const json = (await response.json()) as { items?: CatalogItem[] };

    for (const item of json.items || []) {
      const asin = String(item.asin || "").trim();
      if (!asin) continue;
      const marketplaceImages = item.images?.find((entry) => entry.marketplaceId === marketplaceId)
        || item.images?.[0];
      const main = marketplaceImages?.images?.find((image) => image.variant === "MAIN")
        || marketplaceImages?.images?.[0];
      const summary = item.summaries?.find((entry) => entry.marketplaceId === marketplaceId)
        || item.summaries?.[0];
      const value = {
        imageUrl: safeAmazonImage(main?.link),
        productName: summary?.itemName?.trim() || null,
      };
      result.set(asin, value);
      catalogCache.set(asin, { value, expiresAt: now + CACHE_TTL_MS });
    }

    for (const asin of batch) {
      if (result.has(asin)) continue;
      const value = { imageUrl: null, productName: null };
      result.set(asin, value);
      catalogCache.set(asin, { value, expiresAt: now + CACHE_TTL_MS });
    }
  }
  return result;
}

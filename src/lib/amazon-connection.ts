import { createClient } from "@supabase/supabase-js";
import type { NextRequest } from "next/server";
import { tenantIdFromRequest } from "@/lib/amazon-tenant-cookie";

type AmazonConnection = {
  seller_id: string | null;
  refresh_token: string;
};

type ConnectionResult =
  | { connection: AmazonConnection; error: null }
  | { connection: null; error: "not_connected" | "connection_lookup_failed" | "no_refresh_token" };

function must(name: string) {
  const value = process.env[name];
  if (!value) throw new Error(`Missing env ${name}`);
  return value;
}

export async function loadAmazonConnection(
  req: NextRequest,
  region: string,
): Promise<ConnectionResult> {
  const tenantId = tenantIdFromRequest(req);
  if (!tenantId) return { connection: null, error: "not_connected" };

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

  if (error) return { connection: null, error: "connection_lookup_failed" };
  if (!data?.refresh_token) return { connection: null, error: "no_refresh_token" };
  return { connection: data as AmazonConnection, error: null };
}

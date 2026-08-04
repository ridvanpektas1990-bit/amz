import crypto from "node:crypto";
import type { NextRequest } from "next/server";

function signatureFor(tenantId: string, secret: string) {
  return crypto.createHmac("sha256", secret).update(tenantId).digest("base64url");
}

export function createTenantCookie(tenantId: string, secret: string) {
  return `${tenantId}.${signatureFor(tenantId, secret)}`;
}

export function readTenantCookie(value: string | undefined, secret: string) {
  if (!value) return null;

  const separator = value.lastIndexOf(".");
  if (separator <= 0) return null;

  const tenantId = value.slice(0, separator);
  const suppliedSignature = value.slice(separator + 1);
  const expectedSignature = signatureFor(tenantId, secret);
  const supplied = Buffer.from(suppliedSignature);
  const expected = Buffer.from(expectedSignature);

  if (supplied.length !== expected.length) return null;
  return crypto.timingSafeEqual(supplied, expected) ? tenantId : null;
}

export function tenantIdFromRequest(req: NextRequest) {
  const secret = process.env.LWA_CLIENT_SECRET;
  if (!secret) return null;
  return readTenantCookie(req.cookies.get("amz_tenant")?.value, secret);
}

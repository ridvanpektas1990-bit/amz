"""
Nightly / manual order-items sync via Amazon flat-file order reports.

Report: GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL (FBA + FBM)
Window: last LOOKBACK_DAYS (default 7, max 30 per Amazon).

Env:
  SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
  LWA_CLIENT_ID, LWA_CLIENT_SECRET
  TENANT_ID, MARKETPLACE (default DE)
  SP_API_REFRESH_TOKEN (optional; else loaded from amazon_connections)
  LOOKBACK_DAYS (default 7) OR REPORT_START / REPORT_END (YYYY-MM-DD)
  LOCAL_TZ (default Europe/Berlin)
"""

from __future__ import annotations

import csv
import io
import json
import os
import time
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from sp_api.api import Reports
from sp_api.auth.exceptions import AuthorizationError
from sp_api.base import (
    Marketplaces,
    SellingApiBadRequestException,
    SellingApiForbiddenException,
    SellingApiRequestThrottledException,
    SellingApiServerException,
)
from sp_api.base.reportTypes import ReportType
from supabase import create_client

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

CI = os.getenv("GITHUB_ACTIONS") == "true"
if (Path.cwd() / ".env").exists():
    load_dotenv(Path.cwd() / ".env", override=not CI)


def require_env(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if not value:
        raise SystemExit(f"Missing env var {name}")
    return value


SUPABASE_URL = require_env("SUPABASE_URL").rstrip("/")
SUPABASE_KEY = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY") or "").strip()
if not SUPABASE_KEY:
    raise SystemExit("Missing SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY)")

TENANT_ID = require_env("TENANT_ID")
MARKETPLACE_CODE = (os.getenv("MARKETPLACE") or "DE").strip().upper()
TABLE = (os.getenv("SUPABASE_ORDER_ITEMS_TABLE") or "amazon_order_items").strip()
LOCAL_TZ = ZoneInfo(os.getenv("LOCAL_TZ") or "Europe/Berlin")
BATCH_SIZE = int(os.getenv("SUPABASE_BATCH_SIZE") or "300")
LOOKBACK_DAYS = max(1, min(30, int(os.getenv("LOOKBACK_DAYS") or "7")))
POLL_SECONDS = int(os.getenv("REPORT_POLL_SECONDS") or "15")
MAX_POLLS = int(os.getenv("REPORT_MAX_POLLS") or "80")
try:
    REPORT_TYPE = ReportType.GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL
except AttributeError:
    REPORT_TYPE = "GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL"


def rest_get(path: str) -> Any:
    request = urllib.request.Request(
        f"{SUPABASE_URL}/rest/v1/{path}",
        headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"},
    )
    with urllib.request.urlopen(request, timeout=45) as response:
        return json.loads(response.read().decode("utf-8"))


def log_etl_run(status: str, note: str) -> None:
    """Write a monitoring row into etl_runs (best-effort)."""
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    today = datetime.now(LOCAL_TZ).date()
    payload = {
        "tenant_id": TENANT_ID,
        "marketplace": MARKETPLACE_CODE,
        "period_year": today.year,
        "period_month": today.month,
        "status": status,
        "started_at": now,
        "finished_at": now,
        "run_log": f"[order_items] {note}",
    }
    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        f"{SUPABASE_URL}/rest/v1/etl_runs",
        data=body,
        method="POST",
        headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            response.read()
        print(f"# etl_runs logged status={status}")
    except Exception as exc:  # noqa: BLE001 — monitoring must not fail the sync
        print(f"::warning::etl_runs log failed: {exc}")


def load_refresh_token() -> tuple[str, str]:
    env_token = (os.getenv("SP_API_REFRESH_TOKEN") or "").strip()
    tenant = urllib.parse.quote(TENANT_ID, safe="")
    rows = rest_get(
        "amazon_connections"
        f"?tenant_id=eq.{tenant}&select=refresh_token,region&limit=1"
    )
    if not rows:
        raise SystemExit(f"No Amazon connection for tenant={TENANT_ID}")
    region = str(rows[0].get("region") or "eu").lower()
    token = env_token or str(rows[0].get("refresh_token") or "").strip()
    if not token:
        raise SystemExit(f"No refresh_token for tenant={TENANT_ID}")
    return token, region


def marketplace() -> Any:
    try:
        return getattr(Marketplaces, MARKETPLACE_CODE)
    except AttributeError as exc:
        raise SystemExit(f"Unknown marketplace code: {MARKETPLACE_CODE}") from exc


def with_throttle_retry(fn, *args, **kwargs):
    for attempt in range(8):
        try:
            return fn(*args, **kwargs)
        except (SellingApiRequestThrottledException, SellingApiServerException) as exc:
            wait = min(60.0, 2.0 * (2**attempt))
            print(f"# Retry {attempt + 1}/8 {fn.__name__}: {type(exc).__name__} → wait {wait:.1f}s")
            time.sleep(wait)
    raise SystemExit(f"Too many retries for {fn.__name__}")


def report_window() -> tuple[date, date]:
    start_env = (os.getenv("REPORT_START") or "").strip()
    end_env = (os.getenv("REPORT_END") or "").strip()
    if start_env and end_env:
        return date.fromisoformat(start_env[:10]), date.fromisoformat(end_env[:10])
    end = datetime.now(LOCAL_TZ).date() - timedelta(days=0)
    # Amazon: end must not be "now"; use today local calendar date is OK for order-date reports.
    start = end - timedelta(days=LOOKBACK_DAYS - 1)
    return start, end


def iso_z_from_date(d: date, end_of_day: bool = False) -> str:
    if end_of_day:
        dt = datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=timezone.utc)
    else:
        dt = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def berlin_date(ts: datetime) -> date:
    return ts.astimezone(LOCAL_TZ).date()


def parse_purchase_date(raw: str) -> datetime | None:
    raw = (raw or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def download_report_text(reports: Reports, document_id: str) -> str:
    doc = with_throttle_retry(
        reports.get_report_document,
        document_id,
        download=True,
        decrypt=True,
    )
    payload = doc.payload or {}
    # Library may return content under different keys depending on version.
    for key in ("document", "content", "payload"):
        val = payload.get(key)
        if isinstance(val, str) and val.strip():
            return val
    url = payload.get("url")
    if url:
        with urllib.request.urlopen(url, timeout=120) as response:
            data = response.read()
        encoding = payload.get("compressionAlgorithm")
        if str(encoding or "").upper() == "GZIP":
            import gzip

            return gzip.decompress(data).decode("utf-8", errors="replace")
        return data.decode("utf-8", errors="replace")
    raise SystemExit(f"Could not read report document {document_id}: keys={list(payload.keys())}")


def parse_tsv(text: str) -> list[dict[str, str]]:
    text = text.lstrip("\ufeff")
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    return [dict(row) for row in reader]


def upsert_rows(rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    total = 0
    for i in range(0, len(rows), BATCH_SIZE):
        chunk = rows[i : i + BATCH_SIZE]
        sb.table(TABLE).upsert(
            chunk,
            on_conflict="tenant_id,marketplace,amazon_order_id,order_item_id",
        ).execute()
        total += len(chunk)
        print(f"# Upserted {total}/{len(rows)} → {TABLE}")
    return total


def main() -> None:
    refresh_token, region = load_refresh_token()
    print(f"::add-mask::{refresh_token}")
    mp = marketplace()
    start_d, end_d = report_window()
    if (end_d - start_d).days > 29:
        raise SystemExit("Report window must be ≤ 30 days")

    creds = {
        "refresh_token": refresh_token,
        "lwa_app_id": require_env("LWA_CLIENT_ID"),
        "lwa_client_secret": require_env("LWA_CLIENT_SECRET"),
    }
    reports = Reports(credentials=creds, marketplace=mp)

    print(
        f"=== Order Items Report | tenant={TENANT_ID} mp={MARKETPLACE_CODE} "
        f"region={region} | {start_d} → {end_d} ==="
    )

    create = with_throttle_retry(
        reports.create_report,
        reportType=REPORT_TYPE,
        dataStartTime=iso_z_from_date(start_d, end_of_day=False),
        dataEndTime=iso_z_from_date(end_d, end_of_day=True),
        marketplaceIds=[mp.marketplace_id],
    )
    report_id = (create.payload or {}).get("reportId")
    if not report_id:
        raise SystemExit(f"No reportId in create response: {create.payload}")

    document_id = None
    for poll in range(MAX_POLLS):
        status_res = with_throttle_retry(reports.get_report, report_id)
        payload = status_res.payload or {}
        status = str(payload.get("processingStatus") or "")
        print(f"# Report {report_id} status={status} poll={poll + 1}/{MAX_POLLS}")
        if status == "DONE":
            document_id = payload.get("reportDocumentId")
            break
        if status in ("CANCELLED", "FATAL"):
            raise SystemExit(f"Report {report_id} failed: {status}")
        time.sleep(POLL_SECONDS)

    if not document_id:
        raise SystemExit(f"Report {report_id} timed out")

    text = download_report_text(reports, document_id)
    raw_rows = parse_tsv(text)
    print(f"# Report rows: {len(raw_rows)}")

    now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    upserts: list[dict[str, Any]] = []
    skipped = 0
    for row in raw_rows:
        order_id = (row.get("amazon-order-id") or "").strip()
        order_item_id = (row.get("order-item-id") or "").strip()
        order_status = (row.get("order-status") or "").strip().lower()
        if not order_id or not order_item_id:
            skipped += 1
            continue
        if order_status in ("cancelled", "canceled"):
            skipped += 1
            continue
        purchase = parse_purchase_date(row.get("purchase-date") or "")
        if not purchase:
            skipped += 1
            continue
        berlin = berlin_date(purchase)
        iso_year, iso_week, _ = berlin.isocalendar()
        qty = max(0, int(float(row.get("quantity") or 0)))
        item_status = (row.get("item-status") or "").strip().lower()
        shipped = 0 if item_status == "unshipped" else qty
        upserts.append(
            {
                "tenant_id": TENANT_ID,
                "marketplace": MARKETPLACE_CODE,
                "amazon_order_id": order_id,
                "order_item_id": order_item_id,
                "seller_sku": (row.get("sku") or "").strip() or None,
                "asin": (row.get("asin") or "").strip() or None,
                "title": (row.get("product-name") or "").strip() or None,
                "quantity_ordered": qty,
                "quantity_shipped": shipped,
                "purchase_date_local": purchase.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
                "purchase_date_berlin": berlin.isoformat(),
                "iso_year": iso_year,
                "iso_week": iso_week,
                "updated_at": now_iso,
            }
        )

    written = upsert_rows(upserts)
    summary = {
        "ok": True,
        "tenant_id": TENANT_ID,
        "marketplace": MARKETPLACE_CODE,
        "start": start_d.isoformat(),
        "end": end_d.isoformat(),
        "report_rows": len(raw_rows),
        "upserted": written,
        "skipped": skipped,
    }
    print(json.dumps(summary))
    log_etl_run(
        "success",
        f"report {start_d.isoformat()}..{end_d.isoformat()} rows={len(raw_rows)} upserted={written}",
    )


if __name__ == "__main__":
    try:
        main()
    except AuthorizationError as exc:
        # Stale/revoked connections in amazon_connections should not fail the whole matrix.
        print(f"::warning::Skipping tenant={TENANT_ID}: LWA authorization failed ({exc})")
        print(json.dumps({"ok": False, "skipped": True, "tenant_id": TENANT_ID, "reason": "unauthorized_client"}))
        log_etl_run("error", f"skipped unauthorized_client: {exc}")
        raise SystemExit(0)
    except Exception as exc:
        log_etl_run("error", f"{type(exc).__name__}: {exc}")
        if isinstance(exc, SellingApiForbiddenException):
            print("403 Forbidden → Reports / Inventory and Order Tracking Rolle prüfen.")
        elif isinstance(exc, SellingApiBadRequestException):
            print(f"400 Bad Request → {exc}")
        raise

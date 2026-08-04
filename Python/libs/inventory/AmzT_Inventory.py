"""Fetch the current FBA inventory and append a daily Supabase snapshot."""

from __future__ import annotations

import json
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from sp_api.api import Inventories
from sp_api.base import Marketplaces, SellingApiRequestThrottledException, SellingApiServerException
from supabase import create_client

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo


if (Path.cwd() / ".env").exists():
    load_dotenv(Path.cwd() / ".env", override=os.getenv("GITHUB_ACTIONS") != "true")


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
TABLE = (os.getenv("SUPABASE_INVENTORY_TABLE") or "amazon_inventory_daily").strip()
LOCAL_TZ = ZoneInfo(os.getenv("LOCAL_TZ") or "Europe/Berlin")
BATCH_SIZE = int(os.getenv("SUPABASE_BATCH_SIZE") or "300")


def rest_get(path: str) -> Any:
    request = urllib.request.Request(
        f"{SUPABASE_URL}/rest/v1/{path}",
        headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"},
    )
    with urllib.request.urlopen(request, timeout=45) as response:
        return json.loads(response.read().decode("utf-8"))


def load_connection() -> dict[str, Any]:
    tenant = urllib.parse.quote(TENANT_ID, safe="")
    rows = rest_get(
        "amazon_connections"
        f"?tenant_id=eq.{tenant}&select=refresh_token,seller_id,region&limit=1"
    )
    if not rows or not rows[0].get("refresh_token"):
        raise SystemExit(f"No Amazon connection found for tenant={TENANT_ID}")
    return rows[0]


def table_columns() -> set[str]:
    try:
        rows = rest_get(f"{TABLE}?select=*&limit=1")
        if rows:
            return set(rows[0].keys())
    except Exception as exc:
        print(f"# Existing-row schema lookup failed: {type(exc).__name__}")

    request = urllib.request.Request(
        f"{SUPABASE_URL}/rest/v1/",
        headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Accept": "application/openapi+json",
        },
    )
    with urllib.request.urlopen(request, timeout=45) as response:
        spec = json.loads(response.read().decode("utf-8"))
    definition = (spec.get("definitions") or {}).get(TABLE) or {}
    columns = set((definition.get("properties") or {}).keys())
    if not columns:
        raise SystemExit(f"Could not discover columns for Supabase table '{TABLE}'")
    return columns


def marketplace() -> Any:
    try:
        return getattr(Marketplaces, MARKETPLACE_CODE)
    except AttributeError as exc:
        raise SystemExit(f"Unknown marketplace code: {MARKETPLACE_CODE}") from exc


def fetch_inventory(credentials: dict[str, str], mp: Any) -> list[dict[str, Any]]:
    api = Inventories(credentials=credentials, marketplace=mp)
    summaries: list[dict[str, Any]] = []
    next_token: str | None = None
    page = 0

    while True:
        kwargs: dict[str, Any] = {
            "details": True,
            "granularityType": "Marketplace",
            "granularityId": mp.marketplace_id,
            "marketplaceIds": [mp.marketplace_id],
        }
        if next_token:
            kwargs["nextToken"] = next_token

        for attempt in range(1, 9):
            try:
                response = api.get_inventory_summary_marketplace(**kwargs)
                break
            except (SellingApiRequestThrottledException, SellingApiServerException):
                if attempt == 8:
                    raise
                wait = min(60, 2**attempt)
                print(f"# Amazon throttled page {page + 1}; retry in {wait}s")
                time.sleep(wait)

        payload = response.payload or {}
        page_rows = payload.get("inventorySummaries") or []
        summaries.extend(page_rows)
        page += 1
        print(f"# Amazon inventory page {page}: {len(page_rows)} rows")
        next_token = (payload.get("pagination") or {}).get("nextToken")
        if not next_token:
            break

    return summaries


def first_present(columns: set[str], *names: str) -> str | None:
    return next((name for name in names if name in columns), None)


def build_rows(
    summaries: list[dict[str, Any]], columns: set[str], seller_id: str, marketplace_id: str
) -> list[dict[str, Any]]:
    now = datetime.now(LOCAL_TZ)
    snapshot_date = now.date().isoformat()
    captured_at = datetime.now(timezone.utc).isoformat()

    aliases = {
        "tenant": first_present(columns, "tenant_id", "tenant"),
        "seller": first_present(columns, "seller_id"),
        "marketplace": first_present(columns, "marketplace", "marketplace_code"),
        "marketplace_id": first_present(columns, "marketplace_id"),
        "date": first_present(columns, "snapshot_date", "inventory_date", "report_date", "date", "day"),
        "captured": first_present(columns, "fetched_at_utc", "captured_at", "snapshot_at", "updated_at"),
        "sku": first_present(columns, "seller_sku", "sku"),
        "asin": first_present(columns, "asin"),
        "fnsku": first_present(columns, "fn_sku", "fnsku"),
        "condition": first_present(columns, "condition", "condition_type"),
        "available": first_present(columns, "inventory_left", "fulfillable_quantity", "quantity_available", "available"),
        "total": first_present(columns, "total_quantity", "quantity_total"),
        "inbound_working": first_present(columns, "inbound_working_quantity"),
        "inbound_shipped": first_present(columns, "inbound_shipped_quantity"),
        "inbound_receiving": first_present(columns, "inbound_receiving_quantity"),
        "reserved": first_present(columns, "reserved_quantity", "total_reserved_quantity"),
        "pending_customer": first_present(columns, "pending_customer_order_quantity"),
        "unfulfillable": first_present(columns, "unfulfillable_quantity", "total_unfulfillable_quantity"),
        "researching": first_present(columns, "researching_quantity", "total_researching_quantity"),
        "details": first_present(columns, "details"),
    }
    if not aliases["sku"] or not aliases["available"]:
        raise SystemExit(
            "Inventory table needs a seller_sku/sku column and an "
            "inventory_left/fulfillable_quantity column. Found: " + ", ".join(sorted(columns))
        )

    rows: list[dict[str, Any]] = []
    for item in summaries:
        details = item.get("inventoryDetails") or {}
        reserved = details.get("reservedQuantity") or {}
        unfulfillable = details.get("unfulfillableQuantity") or {}
        researching = details.get("researchingQuantity") or {}
        values = {
            "tenant": TENANT_ID,
            "seller": seller_id or TENANT_ID,
            "marketplace": MARKETPLACE_CODE,
            "marketplace_id": marketplace_id,
            "date": snapshot_date,
            "captured": captured_at,
            "sku": item.get("sellerSku"),
            "asin": item.get("asin"),
            "fnsku": item.get("fnSku"),
            "condition": item.get("condition"),
            "available": details.get("fulfillableQuantity", 0),
            "total": item.get("totalQuantity", 0),
            "inbound_working": details.get("inboundWorkingQuantity", 0),
            "inbound_shipped": details.get("inboundShippedQuantity", 0),
            "inbound_receiving": details.get("inboundReceivingQuantity", 0),
            "reserved": reserved.get("totalReservedQuantity", 0),
            "pending_customer": reserved.get("pendingCustomerOrderQuantity", 0),
            "unfulfillable": unfulfillable.get("totalUnfulfillableQuantity", 0),
            "researching": researching.get("totalResearchingQuantity", 0),
            "details": item,
        }
        row = {column: values[key] for key, column in aliases.items() if column and values[key] is not None}
        rows.append(row)
    return rows


def insert_rows(rows: list[dict[str, Any]]) -> None:
    if not rows:
        print("# No inventory rows returned; nothing to insert")
        return
    client = create_client(SUPABASE_URL, SUPABASE_KEY)
    for start in range(0, len(rows), BATCH_SIZE):
        client.table(TABLE).insert(rows[start : start + BATCH_SIZE], returning="minimal").execute()
    print(f"# Supabase insert complete: {len(rows)} rows into {TABLE}")


def main() -> None:
    connection = load_connection()
    refresh_token = str(connection["refresh_token"])
    print(f"::add-mask::{refresh_token}")
    mp = marketplace()
    credentials = {
        "refresh_token": refresh_token,
        "lwa_app_id": require_env("LWA_CLIENT_ID"),
        "lwa_client_secret": require_env("LWA_CLIENT_SECRET"),
    }
    columns = table_columns()
    print(f"# Discovered {TABLE} columns: {', '.join(sorted(columns))}")
    summaries = fetch_inventory(credentials, mp)
    rows = build_rows(summaries, columns, str(connection.get("seller_id") or ""), mp.marketplace_id)
    insert_rows(rows)


if __name__ == "__main__":
    main()

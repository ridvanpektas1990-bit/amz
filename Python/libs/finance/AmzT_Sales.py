# AmzT_Sales.py  (optimierte Fassung, mit CLAMP auf gestern 23:59:59 LOCAL_TZ)
# pip install -U python-amazon-sp-api python-dotenv supabase requests tzdata
# optional für XLSX: pandas openpyxl

from pathlib import Path
from datetime import datetime, timezone, timedelta, time as dtime  # ← dtime hinzugefügt
from dotenv import load_dotenv
import os, time, random, csv, re, sys
from typing import Tuple, List, Dict, Any, Optional
from decimal import Decimal, InvalidOperation

from sp_api.api import Orders
from sp_api.base import (
    Marketplaces,
    SellingApiForbiddenException,
    SellingApiBadRequestException,
    SellingApiRequestThrottledException,
    SellingApiServerException,
)
from requests.exceptions import ConnectTimeout, ReadTimeout, Timeout

try:
    from zoneinfo import ZoneInfo  # Py3.9+
except Exception:
    from backports.zoneinfo import ZoneInfo

# =========================
# ENV laden (CI-sicher)
# =========================
CI = (os.getenv("GITHUB_ACTIONS") == "true")

DOTENV_PATH = os.environ.get("DOTENV_PATH") or ""
if DOTENV_PATH:
    DOTENV_PATH = Path(DOTENV_PATH)
else:
    DOTENV_PATH = Path.cwd() / ".env"

if DOTENV_PATH.exists():
    # In CI überschreibt .env NICHT die GitHub-Env
    load_dotenv(DOTENV_PATH, override=not CI)

def require_env(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise SystemExit(f"Missing env var {name}")
    return v

def first_nonempty(*vals: Optional[str]) -> Optional[str]:
    for v in vals:
        if v and str(v).strip():
            return str(v).strip()
    return None

# =========================
# Settings
# =========================
# LWA / SP-API
CREDS = dict(
    refresh_token=require_env("SP_API_REFRESH_TOKEN"),
    lwa_app_id=require_env("LWA_APP_ID"),
    lwa_client_secret=require_env("LWA_CLIENT_SECRET"),
)

MP_CODE = require_env("MARKETPLACE", "DE").upper().strip()
YEAR = int(os.getenv("ORDERS_YEAR", "2025"))
MONTH = int(os.getenv("ORDERS_MONTH", "1"))

LOCAL_TZ = os.getenv("LOCAL_TZ", "Europe/Istanbul")
TZ = ZoneInfo(LOCAL_TZ)

PACE = float(os.getenv("ORDERS_PACE_SECONDS", os.getenv("PACE_SECONDS", "2.5")))
REQ_TIMEOUT = float(os.getenv("SPAPI_REQUEST_TIMEOUT_SECONDS", "60"))
MAX_TOKEN_PAGES = int(os.getenv("SPAPI_MAX_TOKEN_PAGES", "500"))
MAX_RESULTS_PER_PAGE = int(os.getenv("ORDERS_MAX_RESULTS_PER_PAGE", "100"))
MAX_ORDERS_PER_RUN = int(os.getenv("MAX_ORDERS_PER_RUN", "0"))  # 0 = alle
DATE_MODE = os.getenv("ORDERS_DATE_MODE", "created").lower()    # "created" | "updated"

# Optional: Geldbeträge als String (Decimal) statt float senden (DB castet zu numeric)
DECIMAL_AS_STR = os.getenv("DECIMAL_AS_STR", "0") == "1"

# Supabase
from supabase import create_client, Client
from postgrest.exceptions import APIError as PgAPIError

SUPABASE_URL  = require_env("SUPABASE_URL")
SUPABASE_KEY  = first_nonempty(os.getenv("SUPABASE_SERVICE_ROLE_KEY"), os.getenv("SUPABASE_KEY"))
if not SUPABASE_KEY:
    raise SystemExit("Missing SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY)")

SUPABASE_ORDERS_TABLE = os.getenv("SUPABASE_ORDERS_TABLE", "amazon_orders")
SUPABASE_ON_CONFLICT  = os.getenv("SUPABASE_ORDERS_ON_CONFLICT", "tenant_id,amazon_order_id,marketplace")
SUPABASE_BATCH_SIZE   = int(os.getenv("SUPABASE_BATCH_SIZE", "500"))

# Tenant/Customer
TENANT_ID = (os.getenv("TENANT_ID") or "").strip()
SELLER_ID = (os.getenv("SELLER_ID") or "").strip()
if not TENANT_ID or TENANT_ID.lower() == "default":
    TENANT_ID = SELLER_ID or "default"
# CI-Schutz: in Actions kein "default" erlauben
if CI and TENANT_ID == "default":
    raise SystemExit("TENANT_ID must be set in CI (no 'default').")

# Optionale Job-Kopplung
IMPORT_JOB_ID = os.getenv("IMPORT_JOB_ID", "").strip()

# Ausgabe-Artefakte
WRITE_CSV  = os.getenv("WRITE_CSV", "0") == "1"
WRITE_XLSX = os.getenv("WRITE_XLSX", "0") == "1"

# Spaltenstil (snake|camel)
ORDERS_COL_STYLE = os.getenv("ORDERS_COL_STYLE", "snake").lower()

COLMAP = {
    "order_total": {"snake": "order_total", "camel": "orderTotal"},
    "currency": {"snake": "currency", "camel": "currency"},
    "orderstatus": {"snake": "order_status", "camel": "orderStatus"},
    "ordertype": {"snake": "order_type", "camel": "orderType"},
    "fulfillmentchannel": {"snake": "fulfillment_channel", "camel": "fulfillmentChannel"},
    "shipservicelevel": {"snake": "ship_service_level", "camel": "shipServiceLevel"},
    "shipmentservicelevelcategory": {"snake": "shipment_service_level_category", "camel": "shipmentServiceLevelCategory"},
    "isbusinessorder": {"snake": "is_business_order", "camel": "isBusinessOrder"},
    "isprime": {"snake": "is_prime", "camel": "isPrime"},
}
def col(name: str) -> str:
    m = COLMAP[name]
    return m["snake"] if ORDERS_COL_STYLE != "camel" else m["camel"]

IGNORE_MISSING_COLS = os.getenv("SUPABASE_IGNORE_MISSING_COLS", "1") == "1"

def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)

# =========================
# Helpers
# =========================
def pick_marketplace(code: str):
    try:
        mp = getattr(Marketplaces, code)
    except AttributeError:
        raise SystemExit(f"Unknown MARKETPLACE='{code}'. Try e.g. DE, US, GB, FR, IT, ES, TR.")
    return mp

def iso_z(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")

def parse_iso_to_utc(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)

def month_bounds_local(year: int, month: int, tz: ZoneInfo) -> Tuple[datetime, datetime]:
    if not 1 <= month <= 12:
        raise SystemExit(f"ORDERS_MONTH invalid: {month}. Use 1..12.")
    start = datetime(year, month, 1, 0, 0, 0, tzinfo=tz)
    next_start = datetime(year + (1 if month == 12 else 0), (1 if month == 12 else month + 1), 1, 0, 0, 0, tzinfo=tz)
    return start, next_start

def with_throttle_retry(fn, *args, **kwargs):
    max_retry = int(os.getenv("SPAPI_MAX_RETRY", "8"))
    attempt = 0
    while True:
        try:
            return fn(*args, **kwargs)
        except (SellingApiRequestThrottledException, Timeout, ReadTimeout, ConnectTimeout, SellingApiServerException) as e:
            attempt += 1
            if attempt > max_retry:
                raise SystemExit(f"Too many retries ({attempt-1}) in {fn.__name__}: {e}")
            wait = min(60.0, 2.0 * (2 ** (attempt - 1))) + random.uniform(0, 0.3)
            print(f"# Retry {attempt}/{max_retry} {fn.__name__}: {type(e).__name__} → wait {wait:.1f}s")
            time.sleep(wait)

_missing_col_rx = re.compile(r"Could not find the '([^']+)' column", re.I)

def _supa_retry(call, *, max_retry=5):
    for i in range(max_retry):
        try:
            return call()
        except PgAPIError as e:
            msg = (getattr(e, "message", "") or str(e)).lower()
            transient = any(x in msg for x in ("rate limit", "timeout", "temporarily", "too many", "service unavailable"))
            if transient and i < max_retry - 1:
                sleep = min(60, 2 ** i)
                print(f"# Supabase transient error → retry {i+1}/{max_retry} after {sleep}s")
                time.sleep(sleep)
                continue
            raise

def to_numeric_str(v):
    try:
        return None if v is None else str(Decimal(str(v)))
    except InvalidOperation:
        return None

def upsert_rows_resilient(table: str, rows: List[Dict[str, Any]], on_conflict: str, batch_size: int = 500):
    if not rows:
        print(f"# {table}: nothing to upsert.")
        return
    supa = get_supabase()
    total = len(rows)
    work_rows = [dict(r) for r in rows]

    print(f"# Upsert → {total} rows into '{table}' (batch={batch_size})"
          f"{' [resilient]' if IGNORE_MISSING_COLS else ''}")
    i = 0
    while i < total:
        chunk = work_rows[i:i+batch_size]
        try:
            _supa_retry(lambda: supa.table(table).upsert(chunk, on_conflict=on_conflict, returning="minimal").execute())
            i += batch_size
        except PgAPIError as e:
            if not IGNORE_MISSING_COLS:
                raise
            msg = getattr(e, "message", "") or str(e)
            m = _missing_col_rx.search(msg)
            if not m:
                raise
            missing_col = m.group(1)
            print(f"! Column '{missing_col}' missing in '{table}' → drop field & retry this chunk …")
            for r in work_rows:
                r.pop(missing_col, None)
            # retry gleiche Position ohne i++

def mark_job(status: str, note: Optional[str] = None, error: Optional[str] = None):
    if not IMPORT_JOB_ID:
        return
    supa = get_supabase()
    payload = {"status": status, "updated_at": datetime.utcnow().isoformat()+"Z"}
    if note is not None:
        payload["note"] = note
    if error is not None:
        payload["error_message"] = (error[:800] if error else None)
    try:
        supa.table("import_jobs").update(payload).eq("id", IMPORT_JOB_ID).execute()
    except Exception as e:
        print(f"! WARN: mark_job({status}) failed: {e}")

# =========================
# Main
# =========================
def main():
    # Trace-Kontext für CI-Logs
    TRACE = {
        "tenant": TENANT_ID,
        "mp": MP_CODE,
        "run_id": os.getenv("GITHUB_RUN_ID"),
        "job": os.getenv("GITHUB_JOB"),
        "repo": os.getenv("GITHUB_REPOSITORY"),
    }
    print(f"[TRACE] {TRACE}")

    mp = pick_marketplace(MP_CODE)
    marketplace_id = mp.marketplace_id  # z.B. A1PA6795UKMFR9 (DE)

    # Monat + 1-Tages-Puffer in lokaler Zeitzone
    m_start_local, m_next_local = month_bounds_local(YEAR, MONTH, TZ)
    after_local  = m_start_local - timedelta(days=1)
    before_local = m_next_local + timedelta(days=1)

    after_utc  = after_local.astimezone(timezone.utc)
    before_utc = before_local.astimezone(timezone.utc)

    # --- CLAMP: 'before_utc' auf maximal gestern 23:59:59 LOCAL_TZ und nie in die Zukunft ---
    def end_of_yesterday_utc(tz: ZoneInfo) -> datetime:
        today_local = datetime.now(tz).date()
        # gestern 23:59:59 local
        yesterday_end_local = datetime.combine(today_local, dtime(0, 0), tzinfo=tz) - timedelta(seconds=1)
        return yesterday_end_local.astimezone(timezone.utc)

    # 2 Minuten Puffer gegen "no later than 2 minutes from now"
    now_utc_safe = datetime.now(timezone.utc) - timedelta(minutes=2)
    y_end_utc    = end_of_yesterday_utc(TZ)

    safe_before_utc = min(before_utc, y_end_utc, now_utc_safe)
    if safe_before_utc <= after_utc:
        safe_before_utc = min(now_utc_safe, after_utc + timedelta(seconds=1))

    print(f"\n=== Orders Import | {MP_CODE} | TZ={LOCAL_TZ} ===")
    print(f"Tenant         : {TENANT_ID}")
    print(f"Period (local) : {m_start_local.date()} → {m_next_local.date()} (+/-1d padded)")
    print(f"API window UTC : {iso_z(after_utc)} → {iso_z(before_utc)}")
    print(f"API window UTC (clamped): {iso_z(after_utc)} → {iso_z(safe_before_utc)}")  # ← neu
    print(f"DateMode       : {DATE_MODE} | Pace {PACE}s | Timeout {REQ_TIMEOUT}s | PerPage {MAX_RESULTS_PER_PAGE}")
    print(f"Supabase table : {SUPABASE_ORDERS_TABLE} ON CONFLICT ({SUPABASE_ON_CONFLICT})")
    print(f"MarketplaceId  : {marketplace_id}")

    orders_api = Orders(credentials=CREDS, marketplace=mp, timeout=REQ_TIMEOUT)

    params = dict(MarketplaceIds=[marketplace_id], MaxResultsPerPage=MAX_RESULTS_PER_PAGE)
    if DATE_MODE == "updated":
        params["LastUpdatedAfter"]  = iso_z(after_utc)
        params["LastUpdatedBefore"] = iso_z(safe_before_utc)  # ← geklemmt
    else:
        params["CreatedAfter"]  = iso_z(after_utc)
        params["CreatedBefore"] = iso_z(safe_before_utc)      # ← geklemmt

    try:
        res = with_throttle_retry(orders_api.get_orders, **params)
    except SellingApiForbiddenException:
        mark_job("failed", error="403 Forbidden (check roles / refresh token)")
        raise
    except SellingApiBadRequestException as e:
        mark_job("failed", error=f"400 Bad Request: {e}")
        raise

    orders = res.payload.get("Orders") or []
    next_token = res.payload.get("NextToken")
    pages = 1
    seen_tokens = set()
    print(f"Page {pages}: {len(orders)} orders")
    time.sleep(PACE)

    while next_token:
        if next_token in seen_tokens or len(seen_tokens) >= MAX_TOKEN_PAGES:
            print("! stop: token loop or max pages reached")
            break
        seen_tokens.add(next_token)
        r2 = with_throttle_retry(orders_api.get_orders, NextToken=next_token)
        more = r2.payload.get("Orders") or []
        orders += more
        pages += 1
        print(f"Page {pages}: +{len(more)} (total {len(orders)})")
        next_token = r2.payload.get("NextToken")
        time.sleep(PACE)

    total_raw = len(orders)
    if MAX_ORDERS_PER_RUN > 0:
        orders = orders[:MAX_ORDERS_PER_RUN]

    # Filtern auf exakten Monat in lokaler TZ + Mappen
    rows_csv: List[tuple] = []
    upserts: List[Dict[str, Any]] = []

    for idx, o in enumerate(orders, start=1):
        pd_raw = o.get("PurchaseDate") or ""
        oid = o.get("AmazonOrderId") or ""
        if not (pd_raw and oid):
            continue
        try:
            dt_utc = parse_iso_to_utc(pd_raw)
            dt_local = dt_utc.astimezone(TZ)
        except Exception:
            continue
        if not (m_start_local <= dt_local < m_next_local):
            continue

        shipped = int(o.get("NumberOfItemsShipped") or 0)
        unshipped = int(o.get("NumberOfItemsUnshipped") or 0)

        # Optional CSV
        if WRITE_CSV:
            rows_csv.append((
                dt_local.replace(microsecond=0).isoformat(),
                oid,
                shipped + unshipped,
                o.get("SalesChannel") or "",
                (o.get("OrderStatus") or "").strip()
            ))

        # OrderTotal
        ot = (o.get("OrderTotal") or {})
        amount   = ot.get("Amount")
        currency = (ot.get("CurrencyCode") or None)

        order_total_value = (
            to_numeric_str(amount) if DECIMAL_AS_STR
            else (float(amount) if amount is not None else None)
        )

        row = {
            "tenant_id": TENANT_ID,
            "amazon_order_id": oid,
            # stabil für Analysen
            "marketplace": marketplace_id,
            "purchase_date_utc": dt_utc.replace(tzinfo=timezone.utc).isoformat(),
            "purchase_date_local": dt_local.isoformat(),
            "number_of_items_shipped": shipped,
            "number_of_items_unshipped": unshipped,
            "period_year": dt_local.year,   # aus lokaler Zeit abgeleitet
            "period_month": dt_local.month, # verhindert Grenzfehler
            col("order_total"): order_total_value,
            col("currency"): currency,
            col("orderstatus"): (o.get("OrderStatus") or "").strip(),
            col("ordertype"): o.get("OrderType"),
            col("fulfillmentchannel"): o.get("FulfillmentChannel"),
            col("shipservicelevel"): o.get("ShipServiceLevel"),
            col("shipmentservicelevelcategory"): o.get("ShipmentServiceLevelCategory"),
            col("isbusinessorder"): (bool(o.get("IsBusinessOrder")) if o.get("IsBusinessOrder") is not None else None),
            col("isprime"): (bool(o.get("IsPrime")) if o.get("IsPrime") is not None else None),
        }
        upserts.append(row)

    # CSV/XLSX (optional)
    if WRITE_CSV and rows_csv:
        out_dir = Path("output")
        out_dir.mkdir(exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        out_csv = out_dir / f"orders_{YEAR}-{MONTH:02d}_{MP_CODE}_{ts}.csv"
        with out_csv.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["PurchaseDateLocal", "AmazonOrderId", "Quantity", "SalesChannel", "OrderStatus"])
            w.writerows(rows_csv)
        print(f"CSV: {out_csv}")

        if WRITE_XLSX:
            try:
                import pandas as pd
                out_xlsx = out_csv.with_suffix(".xlsx")
                df = pd.DataFrame(rows_csv, columns=["PurchaseDateLocal", "AmazonOrderId", "Quantity", "SalesChannel", "OrderStatus"])
                df.to_excel(out_xlsx, index=False)
                print(f"XLSX: {out_xlsx}")
            except Exception:
                print("XLSX skipped (install pandas+openpyxl to enable)")

    print(f"\nAPI raw orders : {total_raw}")
    print(f"In-month rows  : {len(upserts)}")

    # Upsert
    upsert_rows_resilient(SUPABASE_ORDERS_TABLE, upserts, SUPABASE_ON_CONFLICT, SUPABASE_BATCH_SIZE)
    print(f"Done: {len(upserts)} rows → {SUPABASE_ORDERS_TABLE}")

    mark_job("loaded", note=f"{len(upserts)} rows loaded for {YEAR}-{MONTH:02d}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        mark_job("failed", error=str(e))
        raise

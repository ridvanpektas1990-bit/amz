# pip install -U python-amazon-sp-api python-dotenv supabase requests
# (optional) pip install tzdata

# -*- coding: utf-8 -*-
from pathlib import Path
from datetime import datetime, timedelta, timezone, time as dtime
from dotenv import load_dotenv
from decimal import Decimal, InvalidOperation
from typing import Dict, Any, List, Optional, Tuple
from collections import defaultdict
import os, time, csv, random, json, re, hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed

from sp_api.api import Finances
from sp_api.base import (
    Marketplaces,
    SellingApiForbiddenException,
    SellingApiBadRequestException,
    SellingApiRequestThrottledException,
)

from requests.exceptions import ConnectTimeout, ReadTimeout, Timeout

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:
    from backports.zoneinfo import ZoneInfo  # Fallback

from supabase import create_client, Client
from postgrest.exceptions import APIError as PostgrestAPIError

# === .env laden (ENV-first, optional .env) ===================================
DOTENV_PATH = os.environ.get("DOTENV_PATH")
if DOTENV_PATH:
    p = Path(DOTENV_PATH)
    if p.exists():
        load_dotenv(p, override=True)
else:
    p = Path.cwd() / ".env"
    if p.exists():
        load_dotenv(p, override=True)

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

# === Credentials =============================================================
CREDS = dict(
    refresh_token=require_env("SP_API_REFRESH_TOKEN"),
    lwa_app_id=require_env("LWA_APP_ID"),
    lwa_client_secret=require_env("LWA_CLIENT_SECRET"),
)

# === Parametrisierung (Monat & Jahr) ========================================
MP_CODE       = os.getenv("MARKETPLACE", "DE").upper().strip()
ORDERS_YEAR   = int(os.getenv("ORDERS_YEAR", "2025"))
ORDERS_MONTH  = int(os.getenv("ORDERS_MONTH", "1"))
LOCAL_TZ_NAME = os.getenv("LOCAL_TZ", "Europe/Istanbul")
TZ            = ZoneInfo(LOCAL_TZ_NAME)

# === Lauf-Settings ===========================================================
PACE_SECONDS    = float(os.getenv("FINANCE_PACE_SECONDS", "3.5"))
DEBUG_RAW       = os.getenv("FINANCE_DEBUG_RAW", "0") == "1"
UNKNOWN_SAMPLES = int(os.getenv("PROMO_UNKNOWN_SAMPLES", "80"))
MAX_TOKEN_PAGES = int(os.getenv("SPAPI_MAX_TOKEN_PAGES", "500"))
WORKERS         = int(os.getenv("SPAPI_WORKERS", "1"))  # 1 = sequentiell

# Optional: CSV-Audits
SKIP_AUDIT_CSV = os.getenv("SKIP_AUDIT_CSV", "1") == "1"
SKIP_FEE_LINES = os.getenv("SKIP_FEE_LINES", "0") == "1"

SCRIPT_DIR = Path(__file__).parent
OUT_DIR = SCRIPT_DIR / "output"
OUT_DIR.mkdir(exist_ok=True)

# === Supabase ================================================================
SUPABASE_URL = require_env("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = first_nonempty(
    os.getenv("SUPABASE_SERVICE_ROLE_KEY"),
    os.getenv("SUPABASE_KEY")
)
if not SUPABASE_SERVICE_ROLE_KEY:
    raise SystemExit("Missing env var SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY)")

# Tabellen / Konfliktschlüssel
FEES_TABLE       = os.getenv("SUPABASE_FEES_TABLE", "amazon_fees")
FEES_ON_CONFLICT = os.getenv(
    "SUPABASE_FEES_ON_CONFLICT",
    "amazon_order_id,seller_sku,marketplace,period_year,period_month,currency,transaction_phase",
)
BATCH_SIZE = int(os.getenv("SUPABASE_BATCH_SIZE", "300"))

FEE_LINES_TABLE        = os.getenv("SUPABASE_FEE_LINES_TABLE", "amazon_fee_lines")
FEE_LINES_ON_CONFLICT  = os.getenv("SUPABASE_FEE_LINES_ON_CONFLICT", "line_hash")
FEE_LINES_TYPE_COL     = (os.getenv("FEE_LINES_TYPE_COLUMN", "fee_type") or "fee_type").strip()
FEE_LINES_CATEGORY_COL = (os.getenv("FEE_LINES_CATEGORY_COLUMN", "fee_category") or "fee_category").strip()

ACCOUNT_TABLE = (os.getenv("SUPABASE_ACCOUNT_FEES_TABLE", "amazon_account_fees") or "amazon_account_fees").strip()
ACCOUNT_ON_CONFLICT = os.getenv(
    "SUPABASE_ACCOUNT_FEES_ON_CONFLICT",
    "tenant_id,marketplace,date,category,type,currency,financial_event_group_id,period_year,period_month",
)

TENANT_ID = (os.getenv("TENANT_ID", "default") or "default").strip()

print(f"# DEBUG → fee_lines mapping: type_col={FEE_LINES_TYPE_COL} | category_col={FEE_LINES_CATEGORY_COL}")
print(f"# DEBUG → TENANT_ID={TENANT_ID}")
print(f"# DEBUG → FEES table='{FEES_TABLE}', ACCOUNT_FEES table='{ACCOUNT_TABLE}'")

# === Upsert-Helper mit Schema-Fallback ======================================
_OPTIONAL_COLUMNS = {"tenant_id", "marketplace"}

def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def _filter_conflict(conflict: str, removed: set) -> str:
    cols = [c.strip() for c in (conflict or "").split(",") if c.strip()]
    cols = [c for c in cols if c not in removed]
    return ",".join(cols)

def upsert_rows(table: str, rows: List[dict], on_conflict: str, batch_size: int = 300):
    if not rows:
        print(f"# {table}: keine Daten zum Upsert.")
        return
    supa = get_supabase()
    total = len(rows)
    removed: set = set()
    print(f"# Supabase Upsert → {total} Zeilen in '{table}' (Batches à {batch_size})")
    while True:
        try:
            for i in range(0, total, batch_size):
                chunk = rows[i:i+batch_size]
                adj_chunk = [{k: v for k, v in r.items() if k not in removed}] if removed and len(chunk) == 1 \
                            else [{k: v for k, v in r.items() if k not in removed} for r in chunk] if removed else chunk
                adj_conflict = _filter_conflict(on_conflict, removed)
                if adj_conflict:
                    supa.table(table).upsert(adj_chunk, on_conflict=adj_conflict, returning='minimal').execute()
                else:
                    supa.table(table).upsert(adj_chunk, returning='minimal').execute()
                print(f"  · Batch {i//batch_size + 1}: {len(chunk)} Rows upserted")
            if removed:
                print(f"  · Hinweis: '{table}' ohne Spalten {', '.join(sorted(removed))} upserted (Spalten fehlen in DB).")
            return
        except PostgrestAPIError as e:
            msg = ""
            code = ""
            try:
                first = e.args[0]
                if isinstance(first, dict):
                    msg = str(first.get("message", "")) or ""
                    code = str(first.get("code", "")) or ""
                else:
                    msg = str(first) if first else ""
            except Exception:
                msg = str(e)
            if not code and "PGRST" in msg:
                mcode = re.search(r"\b(PGRST\d{3})\b", msg)
                if mcode:
                    code = mcode.group(1)
            missing = None
            m = re.search(r"Could not find the '([^']+)' column", msg or "")
            if (code == "PGRST204" or "PGRST204" in msg) and m:
                missing = m.group(1)
                if missing in _OPTIONAL_COLUMNS and missing not in removed:
                    removed.add(missing)
                    print(f"  · Warnung: '{table}' hat keine Spalte '{missing}' → entferne aus Payload/ON CONFLICT und retry.")
                    continue
            raise

# === Utils ===================================================================
def pick_marketplace(code: str) -> Marketplaces:
    try:
        return getattr(Marketplaces, code)
    except AttributeError:
        raise SystemExit(f"Unknown MARKETPLACE='{code}'. Try e.g. DE, US, GB, FR, IT, ES, TR.")

def iso_z(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")

def parse_iso_z(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        if s.endswith("Z"):
            return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

def to_decimal(val) -> Optional[Decimal]:
    if val is None or val == "":
        return None
    try:
        return Decimal(str(val))
    except (InvalidOperation, ValueError):
        return None

def money_amount(m: Optional[Dict[str, Any]]) -> Tuple[Optional[Decimal], str]:
    if not m:
        return None, ""
    cur = (m.get("CurrencyCode") or m.get("currencyCode") or "")
    val = m.get("Amount")
    if val is None:
        val = m.get("CurrencyAmount")
    return to_decimal(val), cur

def with_throttle_retry(fn, *args, **kwargs):
    attempt = 0
    while True:
        try:
            return fn(*args, **kwargs)
        except SellingApiRequestThrottledException:
            attempt += 1
            wait = min(60.0, 2.0 * (2 ** (attempt - 1))) + random.uniform(0, 0.3)
            print(f"Throttled (429). Waiting {wait:.1f}s… (attempt {attempt})")
            time.sleep(wait)
        except SellingApiBadRequestException:
            raise

def month_bounds_local(year: int, month: int, tz: ZoneInfo) -> tuple[datetime, datetime]:
    if not 1 <= month <= 12:
        raise SystemExit(f"ORDERS_MONTH invalid: {month}. Use 1..12.")
    start = datetime(year, month, 1, 0, 0, 0, tzinfo=tz)
    next_start = datetime(year + (month==12), 1 if month==12 else month+1, 1, 0, 0, 0, tzinfo=tz)
    return start, next_start

# === Promo-Normalizer / Typ-Cannon ===========================================
PROMO_REGEXES: List[Tuple[re.Pattern, str]] = [
    (re.compile(r"\blightning\b", re.I),               "LightningDeal"),
    (re.compile(r"\bblitzangebot\b", re.I),            "LightningDeal"),
    (re.compile(r"\blightning\s*deal\b", re.I),        "LightningDeal"),
    (re.compile(r"\bdeal of the day\b|\bdotd\b", re.I),"DealOfTheDay"),
    (re.compile(r"\b7[ -]?day\b|\bbest deal\b", re.I), "BestDeal"),
    (re.compile(r"\bprime\s+exclusive\b", re.I),       "PrimeExclusiveDiscount"),
    (re.compile(r"\bprice\s*discount\b|\bdiscount\b", re.I), "PriceDiscount"),
    (re.compile(r"\bcoupon\b|\bvoucher\b", re.I),      "Coupon"),
    (re.compile(r"\bsubscribe(\s*&\s*save|\s*and\s*save|\s*&\s*s)\b|\bS&S\b", re.I), "SubscribeAndSave"),
    (re.compile(r"\boutlet\b", re.I),                  "OutletDeal"),
    (re.compile(r"\bship(ping)?\s*(promo|discount)\b", re.I), "ShipPromotion"),
    (re.compile(r"\bship(ping)?\b", re.I),             "ShipPromotion"),
]

PROMO_ID_REGEXES: List[Tuple[re.Pattern, str]] = [
    (re.compile(r"\bfree\s*shipping\b|\bcore\s*free\s*shipping\b", re.I), "ShipPromotion"),
    (re.compile(r"\bpercentage\s+off\b", re.I), "PriceDiscount"),
    (re.compile(r"\bplcc\b|\bfree[- ]financing\b|\bfinancing\b", re.I), "CreditCardFinancing"),
    (re.compile(r"\bpaws[-_]?v2\b", re.I), "SystemPromo"),
    (re.compile(r"\bplm[-_]", re.I), "PlatformPromo"),
    (re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I), "PromoIdOnly"),
    (re.compile(r"\bblitzangebot\b", re.I),            "LightningDeal"),
    (re.compile(r"\blightning\s*deal\b", re.I),        "LightningDeal"),
]

def normalize_promo(ptype: Optional[str], source_list: str, charge_type: str = "", raw: Optional[dict] = None, promotion_id: Optional[str] = None) -> str:
    text = " ".join(x for x in [ptype or "", charge_type or ""] if x).strip()
    raw_obj = raw or {}
    raw_str = json.dumps(raw_obj, ensure_ascii=False).lower()

    if source_list == "CouponPaymentEventList":
        return "Coupon"

    if source_list == "SellerDealPaymentEventList":
        et   = str(raw_obj.get("EventType") or raw_obj.get("eventType") or "").upper()
        desc = (raw_obj.get("DealDescription") or raw_obj.get("dealDescription") or "").lower()
        pid  = (promotion_id or raw_obj.get("DealId") or raw_obj.get("dealId") or "")
        if "LIGHTNING" in et or "blitzangebot" in desc or "lightning" in desc or re.search(r"\bblitzangebot\b", str(pid), re.I):
            return "LightningDeal"
        if "BEST" in et or "7-DAY" in et or "7 DAY" in et or "best deal" in desc:
            return "BestDeal"
        return "Deal"

    if promotion_id:
        for rx, label in PROMO_ID_REGEXES:
            if rx.search(str(promotion_id)):
                return label

    for rx, label in PROMO_REGEXES:
        if rx.search(text) or rx.search(raw_str):
            return label

    if charge_type.lower() == "promotion" and source_list == "ShipmentEventList":
        return "Promo_Item"
    if ptype == "PromotionMetaDataDefinitionValue":
        return "Promotion(Unknown)"

    return (ptype or charge_type or "Promotion").strip()

def canonical_type(t: Optional[str]) -> str:
    if not t:
        return ""
    t = str(t).strip()
    MAP = {
        "COMPENSATED_CLAWBACK": "CompensatedClawback",
        "WAREHOUSE_LOST": "WarehouseLost",
        "WAREHOUSE_DAMAGE": "WarehouseDamage",
        "FBA_INVENTORY_PLACEMENT_SERVICE_FEE": "FBAInventoryPlacementServiceFee",
        "LONG_TERM_STORAGE_FEE": "LongTermStorageFee",
        "REVERSAL_REIMBURSEMENT": "ReversalReimbursement",
    }
    if t in MAP:
        return MAP[t]
    if re.fullmatch(r"[A-Z0-9_]+", t):
        return "".join(p.capitalize() for p in t.split("_"))
    return t

# === Output/Aggregation Helpers ==============================================
def add_row_and_accumulate(
    *,
    all_rows: List[List[Any]],
    abs_by_key_all: Dict[Tuple[Optional[str], str, str, str], Dict[str, Decimal]],
    signed_by_key_all: Dict[Tuple[Optional[str], str, str, str], Dict[str, Decimal]],
    signed_by_cat_key_all: Dict[Tuple[Optional[str], str, str, str], Dict[str, Decimal]],
    date_local: str, posted_at_utc: datetime,
    category: str, typ: str, cur: str,
    amount_signed: Optional[Decimal],
    order_id: Optional[str], sku: Optional[str], asin: Optional[str],
    group_id: str, source_list: str
):
    if amount_signed is None:
        return
    amount_abs = abs(amount_signed)

    all_rows.append([
        date_local, category or "", typ or "", cur or "",
        float(amount_signed), float(amount_abs),
        order_id or "", sku or "", asin or "", group_id or "", source_list or "",
        iso_z(posted_at_utc)
    ])

    sku_val  = (sku or "_ORDER_LEVEL_")
    asin_val = (asin or "")
    key = (order_id or None, sku_val, asin_val, cur or "")

    typ_can = canonical_type(typ or "")

    abs_map = abs_by_key_all.setdefault(key, defaultdict(lambda: Decimal("0")))
    abs_map[typ_can] += Decimal(str(amount_abs))

    s_map = signed_by_key_all.setdefault(key, defaultdict(lambda: Decimal("0")))
    s_map[typ_can] += Decimal(str(amount_signed))

    c_map = signed_by_cat_key_all.setdefault(key, defaultdict(lambda: Decimal("0")))
    c_map[f"{category}:{typ_can}"] += Decimal(str(amount_signed))

def event_dt(ev: Dict[str, Any], group_start: Optional[datetime], group_end: Optional[datetime]) -> datetime:
    pd = (
        ev.get("PostedDate")
        or ev.get("postedDate")
        or ev.get("EventDate")
        or ev.get("eventDate")
        or ev.get("Date")
        or ev.get("date")
    )
    dt = parse_iso_z(pd) if isinstance(pd, str) else None
    if not dt:
        dt = group_end or group_start or datetime.now(timezone.utc)
    return dt

def event_date_local_iso(ev: Dict[str, Any], gs: Optional[datetime], ge: Optional[datetime]) -> Tuple[str, datetime]:
    dt_utc = event_dt(ev, gs, ge)
    dt_loc = dt_utc.astimezone(TZ)
    return dt_loc.date().isoformat(), dt_utc

# === Quantity-Accumulator (nur aus Finances) =================================
QtyKey = Tuple[str, str, str, str]

def qty_bump(qmap: Dict[QtyKey, int], *, order_id: Optional[str], sku: Optional[str], asin: Optional[str], qty: Optional[int], phase: str):
    if not order_id:
        return
    try:
        q = int(qty or 0)
    except Exception:
        q = 0
    if q <= 0:
        return
    key: QtyKey = (order_id, (sku or "_ORDER_LEVEL_"), (asin or ""), phase)
    qmap[key] = qmap.get(key, 0) + q

# === Unknown-Promo Heuristics ================================================
def nearly_equal(a: Optional[Decimal], b: Optional[Decimal], tol: Decimal = Decimal("0.02")) -> bool:
    if a is None or b is None:
        return False
    try:
        return abs(a - b) <= tol
    except Exception:
        return False

def sum_item_shipping_components(item: dict) -> Tuple[Decimal, Decimal]:
    ship_total = Decimal("0")
    ship_tax_total = Decimal("0")
    for ch in (item.get("ItemChargeList") or []):
        ctype = (ch.get("ChargeType") or "").strip()
        amt, _ = money_amount(ch.get("ChargeAmount") or {})
        if amt is None:
            continue
        if ctype in ("ShippingCharge", "Shipping"):
            ship_total += abs(amt)
        elif ctype == "ShippingTax":
            ship_tax_total += abs(amt)
    return ship_total, ship_tax_total

# === Extractors ==============================================================#
PROMO_DUP_SKIPPED = 0
PROMO_SEEN: set = set()
REFUND_SCAN_USED  = 0
LINEHASH_SEEN: set = set()
LINEHASH_SKIPPED  = 0
STRICT_DEDUP_PROMOTIONS = os.getenv("STRICT_DEDUP_PROMOTIONS", "1") == "1"

def extract_from_shipment(events: Dict[str, Any], *,
                          all_rows, abs_by_key_all, signed_by_key_all, signed_by_cat_key_all,
                          qty_by_key_phase: Dict[QtyKey, int],
                          group_id: str, gs, ge,
                          unknown_rows: List[List[Any]],
                          month_start_local: datetime, month_next_local: datetime):
    ship = events.get("ShipmentEventList") or []
    if DEBUG_RAW and ship:
        print("\n[RAW] ShipmentEventList sample:"); print(json.dumps(ship[:1], indent=2, default=str))
    for ev in ship:
        day, dt_utc = event_date_local_iso(ev, gs, ge)
        if not (month_start_local.date() <= datetime.fromisoformat(day).date() < month_next_local.date()):
            continue
        order_id = ev.get("AmazonOrderId")

        for item in (ev.get("ShipmentItemList") or []):
            sku, asin = item.get("SellerSKU"), item.get("ASIN")

            qty_fin = item.get("QuantityShipped") or item.get("QuantityOrdered")
            qty_bump(qty_by_key_phase, order_id=order_id, sku=sku, asin=asin, qty=qty_fin, phase="Payment")

            ship_total_for_item, ship_tax_total_for_item = sum_item_shipping_components(item)

            for fee in (item.get("ItemFeeList") or []):
                amt, cur = money_amount(fee.get("FeeAmount"))
                add_row_and_accumulate(
                    all_rows=all_rows, abs_by_key_all=abs_by_key_all,
                    signed_by_key_all=signed_by_key_all, signed_by_cat_key_all=signed_by_cat_key_all,
                    date_local=day, posted_at_utc=dt_utc,
                    category="ShipmentItemFee", typ=(fee.get("FeeType") or ""), cur=cur,
                    amount_signed=amt, order_id=order_id, sku=sku, asin=asin,
                    group_id=group_id, source_list="ShipmentEventList"
                )

            for ch in (item.get("ItemChargeList") or []):
                amt, cur = money_amount(ch.get("ChargeAmount") or {})
                if amt is None:
                    continue
                ctype_raw = (ch.get("ChargeType") or "").strip()
                add_row_and_accumulate(
                    all_rows=all_rows, abs_by_key_all=abs_by_key_all,
                    signed_by_key_all=signed_by_key_all, signed_by_cat_key_all=signed_by_cat_key_all,
                    date_local=day, posted_at_utc=dt_utc,
                    category="ShipmentItemCharge", typ=ctype_raw, cur=cur,
                    amount_signed=amt, order_id=order_id, sku=sku, asin=asin,
                    group_id=group_id, source_list="ShipmentEventList"
                )

            for pr in (item.get("PromotionList") or []):
                ptype = pr.get("PromotionType") or ""
                pid   = pr.get("PromotionId") or ""
                amt, cur = money_amount(pr.get("PromotionAmount"))
                bucket = normalize_promo(ptype, "ShipmentEventList", raw=pr, promotion_id=pid)

                if bucket in ("Promotion(Unknown)", "Deal") and amt is not None and amt < 0:
                    if ship_total_for_item and nearly_equal(abs(amt), ship_total_for_item):
                        bucket = "ShipPromotion"
                    elif ship_tax_total_for_item and nearly_equal(abs(amt), ship_tax_total_for_item):
                        bucket = "ShipPromotion"

                if bucket == "Promotion(Unknown)" and (amt is None or abs(amt) < Decimal("0.005")):
                    if unknown_rows is not None:
                        unknown_rows.append([
                            ev.get("AmazonOrderId") or "",
                            "ShipmentEventList",
                            ptype, "", json.dumps(pr, ensure_ascii=False),
                            float(amt or 0), cur or ""
                        ])
                    continue

                if STRICT_DEDUP_PROMOTIONS:
                    k = (order_id or "", sku or "_ORDER_LEVEL_", asin or "", bucket, float(amt or 0), cur or "", iso_z(dt_utc), group_id or "")
                    global PROMO_DUP_SKIPPED
                    if k in PROMO_SEEN:
                        PROMO_DUP_SKIPPED += 1
                        continue
                    PROMO_SEEN.add(k)

                add_row_and_accumulate(
                    all_rows=all_rows, abs_by_key_all=abs_by_key_all,
                    signed_by_key_all=signed_by_key_all, signed_by_cat_key_all=signed_by_cat_key_all,
                    date_local=day, posted_at_utc=dt_utc,
                    category="Promotion", typ=bucket, cur=cur,
                    amount_signed=amt, order_id=order_id, sku=sku, asin=asin,
                    group_id=group_id, source_list="ShipmentEventList"
                )

        for item in (ev.get("ShipmentItemAdjustmentList") or []):
            sku, asin = item.get("SellerSKU"), item.get("ASIN")
            for fee in (item.get("ItemFeeList") or []):
                amt, cur = money_amount(fee.get("FeeAmount"))
                add_row_and_accumulate(
                    all_rows=all_rows, abs_by_key_all=abs_by_key_all,
                    signed_by_key_all=signed_by_key_all, signed_by_cat_key_all=signed_by_cat_key_all,
                    date_local=day, posted_at_utc=dt_utc,
                    category="ShipmentItemAdjustmentFee", typ=(fee.get("FeeType") or ""), cur=cur,
                    amount_signed=amt, order_id=order_id, sku=sku, asin=asin,
                    group_id=group_id, source_list="ShipmentEventList"
                )

def extract_from_service_fee(events: Dict[str, Any], *,
                             all_rows, abs_by_key_all, signed_by_key_all, signed_by_cat_key_all,
                             group_id: str, gs, ge,
                             month_start_local: datetime, month_next_local: datetime):
    svc = events.get("ServiceFeeEventList") or []
    if DEBUG_RAW and svc:
        print("\n[RAW] ServiceFeeEventList sample:"); print(json.dumps(svc[:1], indent=2, default=str))
    for ev in svc:
        day, dt_utc = event_date_local_iso(ev, gs, ge)
        if not (month_start_local.date() <= datetime.fromisoformat(day).date() < month_next_local.date()):
            continue
        order_id, sku, asin = ev.get("AmazonOrderId"), ev.get("SellerSKU"), ev.get("ASIN")
        for fee in (ev.get("FeeList") or []):
            amt, cur = money_amount(fee.get("FeeAmount"))
            ftype_raw = (fee.get("FeeType") or fee.get("Type") or "")
            fdesc     = (fee.get("FeeDescription") or fee.get("Description") or "")
            l = (f"{ftype_raw} {fdesc}").lower()

            if "vine" in l:
                ftype = "VineFee"
            elif ("lightning" in l) or ("blitzangebot" in l):
                ftype = "LightningDealFee"
            elif ("best deal" in l) or ("7-day" in l) or ("7 day" in l):
                ftype = "BestDealFee"
            else:
                ftype = ftype_raw or "ServiceFee"

            add_row_and_accumulate(
                all_rows=all_rows, abs_by_key_all=abs_by_key_all,
                signed_by_key_all=signed_by_key_all, signed_by_cat_key_all=signed_by_cat_key_all,
                date_local=day, posted_at_utc=dt_utc,
                category="ServiceFee", typ=ftype, cur=cur,
                amount_signed=amt, order_id=order_id, sku=sku, asin=asin,
                group_id=group_id, source_list="ServiceFeeEventList"
            )

def scan_money_components(obj: Any, path: List[str], out: List[Tuple[str, str, Optional[Decimal], str]]):
    if isinstance(obj, dict):
        fee_amt = obj.get("FeeAmount")
        chg_amt = obj.get("ChargeAmount") or obj.get("Amount")
        typ     = obj.get("FeeType") or obj.get("ChargeType") or obj.get("Type") or "/".join(path[-2:]) or ""
        if fee_amt is not None:
            amt, cur = money_amount(fee_amt); out.append(("RefundFee", typ, amt, cur))
        if chg_amt is not None and isinstance(chg_amt, dict):
            amt, cur = money_amount(chg_amt); out.append(("RefundCharge", typ, amt, cur))
        for k, v in obj.items():
            scan_money_components(v, path + [k], out)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            scan_money_components(v, path + [f"[{i}]"] , out)

def extract_from_refund(events: Dict[str, Any], *,
                        all_rows, abs_by_key_all, signed_by_key_all, signed_by_cat_key_all,
                        qty_by_key_phase: Dict[QtyKey, int],
                        group_id: str, gs, ge,
                        month_start_local: datetime, month_next_local: datetime, unknown_rows: List[List[Any]]):
    global REFUND_SCAN_USED
    ref = events.get("RefundEventList") or []
    if DEBUG_RAW and ref:
        print("\n[RAW] RefundEventList sample:"); print(json.dumps(ref[:1], indent=2, default=str))
    for ev in ref:
        day, dt_utc = event_date_local_iso(ev, gs, ge)
        if not (month_start_local.date() <= datetime.fromisoformat(day).date() < month_next_local.date()):
            continue
        order_id = ev.get("AmazonOrderId") or ev.get("OrderId")
        sku, asin = ev.get("SellerSKU"), ev.get("ASIN")

        for it in (ev.get("ShipmentItemAdjustmentList") or []):
            qty_fin = it.get("QuantityShipped") or it.get("QuantityOrdered")
            qty_bump(qty_by_key_phase, order_id=order_id, sku=it.get("SellerSKU") or sku,
                     asin=it.get("ASIN") or asin, qty=qty_fin, phase="Refund")
        for it in (ev.get("ShipmentItemList") or []):
            qty_fin = it.get("QuantityShipped") or it.get("QuantityOrdered")
            qty_bump(qty_by_key_phase, order_id=order_id, sku=it.get("SellerSKU") or sku,
                     asin=it.get("ASIN") or asin, qty=qty_fin, phase="Refund")

        charge_lists = (ev.get("RefundChargeList") or ev.get("ChargeList") or [])
        fee_lists    = (ev.get("RefundFeeList")   or ev.get("FeeList") or [])

        for ch in charge_lists:
            amt, cur = money_amount(ch.get("ChargeAmount") or ch.get("Amount") or {})
            ctype = ch.get("ChargeType") or ch.get("Type") or ""
            add_row_and_accumulate(
                all_rows=all_rows, abs_by_key_all=abs_by_key_all,
                signed_by_key_all=signed_by_key_all, signed_by_cat_key_all=signed_by_cat_key_all,
                date_local=day, posted_at_utc=dt_utc,
                category="RefundCharge", typ=ctype, cur=cur,
                amount_signed=amt, order_id=order_id, sku=sku, asin=asin,
                group_id=group_id, source_list="RefundEventList"
            )

        for fee in fee_lists:
            amt, cur = money_amount(fee.get("FeeAmount"))
            ftype = fee.get("FeeType") or fee.get("Type") or ""
            add_row_and_accumulate(
                all_rows=all_rows, abs_by_key_all=abs_by_key_all,
                signed_by_key_all=signed_by_key_all, signed_by_cat_key_all=signed_by_cat_key_all,
                date_local=day, posted_at_utc=dt_utc,
                category="RefundFee", typ=ftype, cur=cur,
                amount_signed=amt, order_id=order_id, sku=sku, asin=asin,
                group_id=group_id, source_list="RefundEventList"
            )

        for item in (ev.get("ShipmentItemAdjustmentList") or []):
            sku_i, asin_i = item.get("SellerSKU"), item.get("ASIN")
            for ch in (item.get("ItemChargeAdjustmentList") or []):
                amt, cur = money_amount(ch.get("ChargeAmount") or ch.get("Amount") or {})
                ctype = ch.get("ChargeType") or ch.get("Type") or ""
                add_row_and_accumulate(
                    all_rows=all_rows, abs_by_key_all=abs_by_key_all,
                    signed_by_key_all=signed_by_key_all, signed_by_cat_key_all=signed_by_cat_key_all,
                    date_local=day, posted_at_utc=dt_utc,
                    category="RefundChargeAdjustment", typ=ctype, cur=cur,
                    amount_signed=amt, order_id=order_id, sku=sku_i or sku, asin=asin_i or asin,
                    group_id=group_id, source_list="RefundEventList"
                )
            for fee in (item.get("ItemFeeAdjustmentList") or []):
                amt, cur = money_amount(fee.get("FeeAmount"))
                ftype = fee.get("FeeType") or fee.get("Type") or ""
                add_row_and_accumulate(
                    all_rows=all_rows, abs_by_key_all=abs_by_key_all,
                    signed_by_key_all=signed_by_key_all, signed_by_cat_key_all=signed_by_cat_key_all,
                    date_local=day, posted_at_utc=dt_utc,
                    category="RefundFeeAdjustment", typ=ftype, cur=cur,
                    amount_signed=amt, order_id=order_id, sku=sku_i or sku, asin=asin_i or asin,
                    group_id=group_id, source_list="RefundEventList"
                )

        had_explicit = bool(charge_lists or fee_lists)
        if not had_explicit:
            for item in (ev.get("ShipmentItemAdjustmentList") or []):
                if (item.get("ItemChargeAdjustmentList") or item.get("ItemFeeAdjustmentList")):
                    had_explicit = True
                    break

        if not had_explicit:
            bucket: List[Tuple[str, str, Optional[Decimal], str]] = []
            scan_money_components(ev, ["RefundEvent"], bucket)
            for cat, typ, amt, cur in bucket:
                add_row_and_accumulate(
                    all_rows=all_rows, abs_by_key_all=abs_by_key_all,
                    signed_by_key_all=signed_by_key_all, signed_by_cat_key_all=signed_by_cat_key_all,
                    date_local=day, posted_at_utc=dt_utc,
                    category=cat or "", typ=typ or "", cur=cur,
                    amount_signed=amt, order_id=order_id, sku=sku, asin=asin,
                    group_id=group_id, source_list="RefundEventList/Scan"
                )
            REFUND_SCAN_USED += 1

def extract_generic_fee_charge_list(events: Dict[str, Any], *,
                                    all_rows, abs_by_key_all, signed_by_key_all, signed_by_cat_key_all,
                                    qty_by_key_phase: Dict[Tuple[str, str, str, str], int],
                                    group_id: str, gs, ge, list_name: str,
                                    month_start_local: datetime, month_next_local: datetime,
                                    unknown_rows: Optional[List[List[Any]]] = None):
    global PROMO_DUP_SKIPPED

    evs = events.get(list_name) or []
    if DEBUG_RAW and evs:
        print(f"\n[RAW] {list_name} sample:")
        print(json.dumps(evs[:1], indent=2, default=str))

    prefix = list_name.replace("EventList", "")
    phase_hint = "Refund" if list_name in ("GuaranteeClaimEventList", "ChargebackEventList") else None

    for ev in evs:
        day, dt_utc = event_date_local_iso(ev, gs, ge)
        if not (month_start_local.date() <= datetime.fromisoformat(day).date() < month_next_local.date()):
            continue

        order_id = ev.get("AmazonOrderId") or ev.get("OrderId") or ev.get("amazonOrderId") or ev.get("orderId")
        sku      = ev.get("SellerSKU")     or ev.get("sellerSku") or ev.get("sellerSKU")
        asin     = ev.get("ASIN")          or ev.get("asin")

        if phase_hint:
            for it in (ev.get("ShipmentItemAdjustmentList") or []):
                qty_fin = it.get("QuantityShipped") or it.get("QuantityOrdered")
                qty_bump(qty_by_key_phase, order_id=order_id, sku=it.get("SellerSKU") or sku,
                         asin=it.get("ASIN") or asin, qty=qty_fin, phase=phase_hint)
            for it in (ev.get("ShipmentItemList") or []):
                qty_fin = it.get("QuantityShipped") or it.get("QuantityOrdered")
                qty_bump(qty_by_key_phase, order_id=order_id, sku=it.get("SellerSKU") or sku,
                         asin=it.get("ASIN") or asin, qty=qty_fin, phase=phase_hint)

        for ch in (ev.get("ChargeList") or ev.get("chargeList") or []):
            amt, cur = money_amount(ch.get("ChargeAmount") or ch.get("Amount") or ch.get("chargeAmount") or ch.get("amount") or {})
            ctype = ch.get("ChargeType") or ch.get("Type") or ch.get("chargeType") or ch.get("type") or ""
            add_row_and_accumulate(
                all_rows=all_rows, abs_by_key_all=abs_by_key_all,
                signed_by_key_all=signed_by_key_all, signed_by_cat_key_all=signed_by_cat_key_all,
                date_local=day, posted_at_utc=dt_utc,
                category=f"{prefix}Charge", typ=ctype, cur=cur,
                amount_signed=amt, order_id=order_id, sku=sku, asin=asin,
                group_id=group_id, source_list=list_name
            )

        for fee in (ev.get("FeeList") or ev.get("feeList") or []):
            amt, cur = money_amount(fee.get("FeeAmount") or fee.get("feeAmount") or {})
            ftype = fee.get("FeeType") or fee.get("Type") or fee.get("feeType") or fee.get("type") or ""
            add_row_and_accumulate(
                all_rows=all_rows, abs_by_key_all=abs_by_key_all,
                signed_by_key_all=signed_by_key_all, signed_by_cat_key_all=signed_by_cat_key_all,
                date_local=day, posted_at_utc=dt_utc,
                category=f"{prefix}Fee", typ=ftype, cur=cur,
                amount_signed=amt, order_id=order_id, sku=sku, asin=asin,
                group_id=group_id, source_list=list_name
            )

        if list_name in ("CouponPaymentEventList", "SellerDealPaymentEventList"):
            amt_ev, cur_ev = money_amount(
                ev.get("TotalAmount") or ev.get("totalAmount") or
                ev.get("Amount")      or ev.get("amount")      or {}
            )
            pid = (ev.get("PromotionId") or ev.get("promotionId") or
                   ev.get("DealId")      or ev.get("dealId")      or
                   ev.get("CouponId")    or ev.get("couponId")    or "")
            bucket = normalize_promo(None, list_name, raw=ev, promotion_id=pid)

            if STRICT_DEDUP_PROMOTIONS:
                k = (order_id or "", sku or "_ORDER_LEVEL_", asin or "", bucket,
                     float(amt_ev or 0), cur_ev or "", iso_z(dt_utc), group_id or "")
                if k in PROMO_SEEN:
                    PROMO_DUP_SKIPPED += 1
                    continue
                PROMO_SEEN.add(k)

            add_row_and_accumulate(
                all_rows=all_rows, abs_by_key_all=abs_by_key_all,
                signed_by_key_all=signed_by_key_all, signed_by_cat_key_all=signed_by_cat_key_all,
                date_local=day, posted_at_utc=dt_utc,
                category="Promotion", typ=bucket, cur=cur_ev,
                amount_signed=amt_ev, order_id=order_id, sku=sku, asin=asin,
                group_id=group_id, source_list=list_name
            )

def extract_from_adjustments(events: Dict[str, Any], *,
                             all_rows, abs_by_key_all, signed_by_key_all, signed_by_cat_key_all,
                             group_id: str, gs, ge,
                             month_start_local: datetime, month_next_local: datetime):
    adj = events.get("AdjustmentEventList") or []
    if DEBUG_RAW and adj:
        print("\n[RAW] AdjustmentEventList sample:"); print(json.dumps(adj[:1], indent=2, default=str))
    for ev in adj:
        day, dt_utc = event_date_local_iso(ev, gs, ge)
        if not (month_start_local.date() <= datetime.fromisoformat(day).date() < month_next_local.date()):
            continue
        order_id = ev.get("AmazonOrderId")
        typ_raw  = ev.get("AdjustmentType") or ""
        typ_can  = canonical_type(typ_raw)
        amt, cur = money_amount(ev.get("AdjustmentAmount"))
        add_row_and_accumulate(
            all_rows=all_rows, abs_by_key_all=abs_by_key_all,
            signed_by_key_all=signed_by_key_all, signed_by_cat_key_all=signed_by_cat_key_all,
            date_local=day, posted_at_utc=dt_utc,
            category="Adjustment", typ=typ_can, cur=cur,
            amount_signed=amt, order_id=order_id, sku=None, asin=None,
            group_id=group_id, source_list="AdjustmentEventList"
        )

# === Account-Fees ohne OrderId (aggregiert) ==================================
def push_account_fees_detail(all_rows: List[List[Any]], *, marketplace_code: str):
    if not ACCOUNT_TABLE:
        print("# Account-Fees: keine Tabelle konfiguriert → skip.")
        return

    agg: Dict[Tuple[str, str, str, str, str, int, int], float] = {}
    meta: Dict[Tuple[str, str, str, str, str, int, int], str] = {}

    for row in all_rows:
        date_s, category, typ, cur = row[0], row[1], row[2], row[3]
        amt_signed, amt_abs        = row[4], row[5]
        order_id, gid, source      = row[6], row[9], row[10]

        if order_id:
            continue

        key = (date_s, category or "", typ or "", cur or "", gid or "", ORDERS_YEAR, ORDERS_MONTH)
        val = float(amt_abs if amt_abs is not None else (abs(amt_signed) if amt_signed is not None else 0.0))
        agg[key] = agg.get(key, 0.0) + val
        if key not in meta:
            meta[key] = source or ""

    if not agg:
        print("# Account-Fees: nichts zu speichern.")
        return

    payload: List[dict] = []
    for (date_s, category, typ, cur, gid, py, pm), amount in agg.items():
        payload.append({
            "tenant_id": TENANT_ID,
            "marketplace": marketplace_code,
            "date": date_s,
            "category": category,
            "type": typ,
            "currency": cur,
            "amount": round(float(amount), 2),
            "financial_event_group_id": gid,
            "source_list": meta[(date_s, category, typ, cur, gid, py, pm)],
            "period_year": py,
            "period_month": pm,
        })

    upsert_rows(ACCOUNT_TABLE, payload, ACCOUNT_ON_CONFLICT, BATCH_SIZE)
    print(f"# Account-Fees gespeichert: {len(payload)} Zeilen → {ACCOUNT_TABLE}")

# === Fee-Lines (Audit) Upsert ===============================================
def md5(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()

def is_refund_category(category: str) -> bool:
    return category.startswith("Refund") or category.startswith("GuaranteeClaim") or category.startswith("Chargeback")

def push_fee_lines(all_rows: List[List[Any]], *, marketplace: str):
    global LINEHASH_SKIPPED
    payload = []
    for row in all_rows:
        date_local, category, typ, cur = row[0], row[1], row[2], row[3]
        amt_signed, amt_abs = row[4], row[5]
        order_id, sku, asin, gid, source, posted_utc_iso = row[6], row[7], row[8], row[9], row[10], row[11]

        phase = "Refund" if is_refund_category(category) else "Payment"

        cat_val = (category or "").strip() or "UnknownCategory"
        typ_val = (typ or "").strip() or "UnknownType"
        cur_val = (cur or "").strip() or "EUR"
        if not posted_utc_iso:
            try:
                fallback_dt = datetime.fromisoformat(date_local).replace(tzinfo=TZ).astimezone(timezone.utc)
            except Exception:
                fallback_dt = datetime.now(timezone.utc)
            posted_utc_iso = iso_z(fallback_dt)

        key_str = "|".join([
            posted_utc_iso or "",
            date_local or "",
            phase,
            cat_val, typ_val, cur_val,
            f"{float(amt_signed):.6f}",
            f"{float(amt_abs):.6f}",
            order_id or "", sku or "", asin or "",
            gid or "", source or "",
            marketplace or "",
            str(ORDERS_YEAR),
            str(ORDERS_MONTH),
            TENANT_ID,
        ])
        line_hash = md5(key_str)

        if line_hash in LINEHASH_SEEN:
            LINEHASH_SKIPPED += 1
            continue
        LINEHASH_SEEN.add(line_hash)

        rowdict = {
            "line_hash": line_hash,
            "finance_date_utc": posted_utc_iso,
            "finance_date_local": date_local,
            "transaction_phase": phase,
            "currency": cur_val,
            "amount_signed": round(float(amt_signed), 6),
            "amount_abs": round(float(amt_abs), 6),
            "amazon_order_id": order_id or None,
            "seller_sku": sku or None,
            "asin": asin or None,
            "financial_event_group_id": gid or None,
            "source_list": source or None,
            "marketplace": marketplace,   # Code (z. B. "DE")
            "period_year": ORDERS_YEAR,
            "period_month": ORDERS_MONTH,
            "tenant_id": TENANT_ID,
        }
        rowdict[FEE_LINES_CATEGORY_COL] = cat_val
        rowdict[FEE_LINES_TYPE_COL]     = typ_val

        if FEE_LINES_CATEGORY_COL != "category":
            rowdict.pop("category", None)
        if FEE_LINES_TYPE_COL != "type":
            rowdict.pop("type", None)

        payload.append(rowdict)

    upsert_rows(FEE_LINES_TABLE, payload, FEE_LINES_ON_CONFLICT, BATCH_SIZE)
    print(f"# Fee-Lines gespeichert: {len(payload)} Zeilen → {FEE_LINES_TABLE} (skipped line_hash dupes: {LINEHASH_SKIPPED})")

# === Main ====================================================================
def run():
    print(f"[TRACE] {{'tenant':'{TENANT_ID}','mp':'{MP_CODE}','run_id':'{os.getenv('GITHUB_RUN_ID')}','job':'{os.getenv('GITHUB_JOB')}','repo':'{os.getenv('GITHUB_REPOSITORY')}'}}")

    mp = pick_marketplace(MP_CODE)
    mp_name = mp.name
    print(f"\n=== All Costs for {mp_name} | Monat {ORDERS_YEAR}-{ORDERS_MONTH:02d} ({LOCAL_TZ_NAME}) ===")
    fin = Finances(credentials=CREDS, marketplace=mp)

    month_start_local, month_next_local = month_bounds_local(ORDERS_YEAR, ORDERS_MONTH, TZ)
    after_local  = month_start_local - timedelta(days=1)
    before_local = month_next_local + timedelta(days=1)
    after_utc, before_utc = after_local.astimezone(timezone.utc), before_local.astimezone(timezone.utc)

    print(f"Monatsfenster (local): {month_start_local.isoformat()} → {month_next_local.isoformat()}")
    print(f"API (UTC) Fenster    : {iso_z(after_utc)} → {iso_z(before_utc)}")

    # --- CLAMP: 'before_utc' auf maximal gestern 23:59:59 LOCAL_TZ (und nie in die Zukunft) ---
    def end_of_yesterday_utc(tz: ZoneInfo) -> datetime:
        today_local = datetime.now(tz).date()
        today_local_midnight = datetime.combine(today_local, dtime(0, 0), tzinfo=tz)
        y_end_local = today_local_midnight - timedelta(seconds=1)
        return y_end_local.astimezone(timezone.utc)

    now_utc = datetime.now(timezone.utc) - timedelta(minutes=5)  # SP-API: nicht in der Zukunft
    yesterday_end_utc = end_of_yesterday_utc(TZ)

    safe_before_utc = min(before_utc, yesterday_end_utc, now_utc)
    if safe_before_utc <= after_utc:
        safe_before_utc = min(now_utc, after_utc + timedelta(seconds=1))

    print(f"API (UTC) Fenster (geklemmt): {iso_z(after_utc)} → {iso_z(safe_before_utc)}")

    # --- Groups holen --------------------------------------------------------
    def fetch_groups(fin: Finances, after: str, before: str) -> List[Dict[str, Any]]:
        res = with_throttle_retry(
            fin.list_financial_event_groups,
            FinancialEventGroupStartedAfter=after,
            FinancialEventGroupStartedBefore=before,
            MaxResultsPerPage=100
        )
        groups = res.payload.get("FinancialEventGroupList", []) or []
        next_token = res.payload.get("NextToken")
        pages = 1
        time.sleep(PACE_SECONDS)
        while next_token and pages < MAX_TOKEN_PAGES:
            res = with_throttle_retry(fin.list_financial_event_groups_by_next_token, NextToken=next_token)
            groups += res.payload.get("FinancialEventGroupList", []) or []
            next_token = res.payload.get("NextToken")
            pages += 1
            time.sleep(PACE_SECONDS)
        return groups

    def fetch_events_for_group(fin: Finances, gid: str) -> Dict[str, Any]:
        resp = with_throttle_retry(
            fin.list_financial_events_by_group_id,
            event_group_id=gid,
            MaxResultsPerPage=100
        )
        events = resp.payload.get("FinancialEvents", {}) or {}
        next_token = resp.payload.get("NextToken")
        pages = 1
        time.sleep(PACE_SECONDS)

        has_by_next = hasattr(fin, "list_financial_events_by_next_token")

        while next_token and pages < MAX_TOKEN_PAGES:
            try:
                if has_by_next:
                    resp = with_throttle_retry(fin.list_financial_events_by_next_token, NextToken=next_token)
                else:
                    resp = with_throttle_retry(fin.list_financial_events, NextToken=next_token)
            except SellingApiBadRequestException:
                print("  NextToken outside retention → stop paging this group")
                break

            new_events = resp.payload.get("FinancialEvents", {}) or {}
            for k, v in new_events.items():
                if not v:
                    continue
                if isinstance(v, list):
                    events.setdefault(k, []); events[k].extend(v)
                else:
                    events.setdefault(k, []); events[k].append(v)

            next_token = resp.payload.get("NextToken")
            pages += 1
            time.sleep(PACE_SECONDS)

        return events

    groups = fetch_groups(fin, iso_z(after_utc), iso_z(safe_before_utc))
    if not groups:
        print("Keine FinancialEventGroups im Fenster gefunden.")
        return

    # --- Aggregation-Container ----------------------------------------------
    all_rows: List[List[Any]] = []
    unknown_rows: List[List[Any]] = []

    abs_by_key_all: Dict[Tuple[Optional[str], str, str, str], Dict[str, Decimal]] = {}
    signed_by_key_all: Dict[Tuple[Optional[str], str, str, str], Dict[str, Decimal]] = {}
    signed_by_cat_key_all: Dict[Tuple[Optional[str], str, str, str], Dict[str, Decimal]] = {}

    qty_by_key_phase: Dict[QtyKey, int] = {}

    # optional parallel laden
    def _one_group(g):
        gid = g.get("FinancialEventGroupId")
        gs  = parse_iso_z(g.get("FinancialEventGroupStart"))
        ge  = parse_iso_z(g.get("FinancialEventGroupEnd"))
        print(f"- Group {gid} | {gs} → {ge}")
        try:
            evs = fetch_events_for_group(fin, gid)
            return (gid, gs, ge, evs, None)
        except SellingApiBadRequestException:
            return (gid, gs, ge, None, "Group outside retention")
        except Exception as e:
            return (gid, gs, ge, None, f"Error: {e}")

    jobs = []
    if WORKERS > 1:
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futures = [ex.submit(_one_group, g) for g in groups]
            for fut in as_completed(futures):
                jobs.append(fut.result())
    else:
        for g in groups:
            jobs.append(_one_group(g))

    for gid, gs, ge, events, err in jobs:
        if err or not events:
            print(f"  {gid}: {err or 'no events'}")
            continue

        extract_from_shipment(
            events,
            all_rows=all_rows,
            abs_by_key_all=abs_by_key_all,
            signed_by_key_all=signed_by_key_all,
            signed_by_cat_key_all=signed_by_cat_key_all,
            qty_by_key_phase=qty_by_key_phase,
            group_id=gid, gs=gs, ge=ge,
            unknown_rows=unknown_rows,
            month_start_local=month_start_local, month_next_local=month_next_local
        )
        extract_from_service_fee(
            events,
            all_rows=all_rows,
            abs_by_key_all=abs_by_key_all,
            signed_by_key_all=signed_by_key_all,
            signed_by_cat_key_all=signed_by_cat_key_all,
            group_id=gid, gs=gs, ge=ge,
            month_start_local=month_start_local, month_next_local=month_next_local
        )
        extract_from_refund(
            events,
            all_rows=all_rows,
            abs_by_key_all=abs_by_key_all,
            signed_by_key_all=signed_by_key_all,
            signed_by_cat_key_all=signed_by_cat_key_all,
            qty_by_key_phase=qty_by_key_phase,
            group_id=gid, gs=gs, ge=ge,
            month_start_local=month_start_local, month_next_local=month_next_local,
            unknown_rows=unknown_rows
        )
        for name in [
            "AdjustmentEventList",
            "GuaranteeClaimEventList",
            "ChargebackEventList",
            "RemovalShipmentEventList",
            "RemovalShipmentAdjustmentEventList",
            "SellerDealPaymentEventList",
            "CouponPaymentEventList",
            "ProductAdsPaymentEventList",
            "ValueAddedServiceChargeEventList",
            "CapacityReservationBillingEventList",
            "ImagingServicesFeeEventList",
            "NetworkComminglingTransactionEventList",
        ]:
            if name == "AdjustmentEventList":
                extract_from_adjustments(
                    events,
                    all_rows=all_rows,
                    abs_by_key_all=abs_by_key_all,
                    signed_by_key_all=signed_by_key_all,
                    signed_by_cat_key_all=signed_by_cat_key_all,
                    group_id=gid, gs=gs, ge=ge,
                    month_start_local=month_start_local, month_next_local=month_next_local
                )
            else:
                extract_generic_fee_charge_list(
                    events,
                    all_rows=all_rows,
                    abs_by_key_all=abs_by_key_all,
                    signed_by_key_all=signed_by_key_all,
                    signed_by_cat_key_all=signed_by_cat_key_all,
                    qty_by_key_phase=qty_by_key_phase,
                    group_id=gid, gs=gs, ge=ge, list_name=name,
                    month_start_local=month_start_local, month_next_local=month_next_local,
                )

    # --- phasen-spezifische Zeitstempel --------------------------------------
    last_date_per_sku_phase: Dict[Tuple[str, str, str, str, str], datetime] = {}
    for row in all_rows:
        date_s, category, cur = row[0], row[1], row[3]
        order_id, sku, asin   = row[6], row[7], row[8]
        posted_utc_iso        = row[11]
        if not order_id:
            continue
        phase = "Refund" if is_refund_category(category) else "Payment"
        try:
            d = parse_iso_z(posted_utc_iso) or datetime.fromisoformat(date_s).replace(tzinfo=TZ).astimezone(timezone.utc)
        except Exception:
            d = datetime(ORDERS_YEAR, ORDERS_MONTH, 1, tzinfo=TZ).astimezone(timezone.utc)
        keyp = (order_id, sku or "_ORDER_LEVEL_", asin or "", cur or "", phase)
        last_date_per_sku_phase[keyp] = max(last_date_per_sku_phase.get(keyp, d), d)

    # --- CSV (Audit) ----------------------------------------------------------
    if not SKIP_AUDIT_CSV:
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        suffix = f"{ORDERS_YEAR}-{ORDERS_MONTH:02d}"
        header = [
            "DateLocal","Category","Type","Currency",
            "AmountSigned","AmountAbs",
            "AmazonOrderId","SellerSKU","ASIN","FinancialEventGroupId","SourceList",
            "PostedAtUTC"
        ]
        out_path = OUT_DIR / f"samples_{mp_name}_{suffix}_{ts}.csv"
        with out_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f); w.writerow(header); w.writerows(all_rows)
        print(f"{out_path.name}  ({len(all_rows)} rows)")

        if unknown_rows:
            unk_path = OUT_DIR / f"promotion_unknown_samples_{mp_name}_{suffix}_{ts}.csv"
            with unk_path.open("w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["AmazonOrderId","SourceList","PromotionTypeRaw","ChargeTypeRaw","RawSnippet","Amount","Currency"])
                w.writerows(unknown_rows[:UNKNOWN_SAMPLES])
            print(f"{unk_path.name}  ({len(unknown_rows[:UNKNOWN_SAMPLES])} rows)")

    # --- Account-Fees ohne OrderId -------------------------------------------
    push_account_fees_detail(all_rows, marketplace_code=MP_CODE)

    # --- Audit: fee_lines -----------------------------------------------------
    if not SKIP_FEE_LINES:
        push_fee_lines(all_rows, marketplace=MP_CODE)

    # --- Upsert Aggregat (Payment/Refund) → **nur** details_by_category_signed
    rows_by_sku = []
    skipped_account_level = 0

    for (oid, seller_sku, asin_val, cur), cat_map_all in signed_by_cat_key_all.items():
        if not oid:
            skipped_account_level += 1
            continue

        # Split in Payment vs Refund anhand Category-Präfix
        pay_cat: Dict[str, Decimal] = {}
        ref_cat: Dict[str, Decimal] = {}
        for catkey, s in cat_map_all.items():
            category = catkey.split(":", 1)[0]
            if is_refund_category(category):
                ref_cat[catkey] = s
            else:
                pay_cat[catkey] = s

        for phase, cat_map in (("Payment", pay_cat), ("Refund", ref_cat)):
            if not cat_map:
                continue

            # fee_total = Summe der Absolutbeträge
            fee_total_abs = float(sum(abs(v) for v in cat_map.values()))

            # letzte Posted-Zeit für diese SKU/Phase (Fallback = Monatsanfang)
            keyp = (oid, seller_sku or "_ORDER_LEVEL_", asin_val or "", cur or "", phase)
            last_dt = last_date_per_sku_phase.get(
                keyp,
                datetime(ORDERS_YEAR, ORDERS_MONTH, 1, tzinfo=TZ).astimezone(timezone.utc)
            )

            # Menge (falls vorhanden)
            qty_key = (oid, seller_sku or "_ORDER_LEVEL_", asin_val or "", phase)
            qty_val = int(qty_by_key_phase.get(qty_key) or 0)

            # **Nur** die flache Map speichern (unter dem bekannten Schlüssel-Namen)
            fee_breakdown_min = {
                "details_by_category_signed": {k: float(v) for k, v in cat_map.items()}
            }

            row = {
                "amazon_order_id": oid,
                "seller_sku": seller_sku or "_ORDER_LEVEL_",
                "asin": asin_val or None,
                "marketplace": MP_CODE,
                "currency": cur or "EUR",
                "fee_total": fee_total_abs,
                "fee_breakdown": fee_breakdown_min,  # <- minimal!
                "transaction_phase": phase,
                "last_posted_at": iso_z(last_dt),
                "period_year": ORDERS_YEAR,
                "period_month": ORDERS_MONTH,
                "tenant_id": TENANT_ID,
                "quantity": qty_val,
            }
            rows_by_sku.append(row)

    if skipped_account_level:
        print(f"# Hinweis: {skipped_account_level} account-level Key(s) NICHT nach '{FEES_TABLE}' (keine OrderId).")

    upsert_rows(FEES_TABLE, rows_by_sku, FEES_ON_CONFLICT, BATCH_SIZE)

    print(f"Fertig: {len(rows_by_sku)} Zeilen (Payment/Refund getrennt) in Supabase ({FEES_TABLE}).")
    print(f"# Dedupe-Info → promo_dups_skipped={PROMO_DUP_SKIPPED}, refund_scan_used={REFUND_SCAN_USED}")

if __name__ == "__main__":
    for k in ("LWA_APP_ID", "LWA_CLIENT_SECRET", "SP_API_REFRESH_TOKEN"):
        print(f"{k}: {'OK' if os.getenv(k) else 'MISSING'}")
    try:
        run()
    except SellingApiForbiddenException:
        print("403 Forbidden → Finance-Rolle (EU: AISP) fehlt oder App nicht autorisiert (Refresh-Token).")
        raise
    except SellingApiBadRequestException:
        print("400 Bad Request → Datums-/Parameterfenster prüfen.")
        raise
    except SellingApiRequestThrottledException:
        print("429 Too Many Requests → Backoff aktiv.")
        raise
    except (Timeout, ReadTimeout, ConnectTimeout) as e:
        print(f"Transient API error: {type(e).__name__} → bitte erneut versuchen / Backoff.")
        raise

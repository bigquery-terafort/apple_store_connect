"""
Apple App Store Connect → BigQuery  ·  COMPLETE PIPELINE v4
============================================================
⚠️  YE FILE DONO APPLE REPOS MEIN LAGTI HAI:
        apple_store_connect          (BQ_DATASET=apple_store_data)
        apple_console_terafort_us    (BQ_DATASET=apple_console_terafort_us)
    Farq sirf env vars ka hai (vendor number + dataset).

🔴 v3 KYUN THA — SCRIPT DATA KHA RAHI THI
────────────────────────────────────────
    `date BETWEEN min AND max` DELETE + chup-chaap toota hua fetch =
    analytics_app_store_downloads ka Jan/Feb/Mar/May aur June ke 18 din gaye.
    v3 ne fetch ki nakami ginni shuru ki aur adhoore data pe DELETE rok di.

🔴 v4 KYUN — v3 KA GUARD SAHI THA MAGAR BOHOT BHONDA
────────────────────────────────────────────────────
Run #181 (2026-09-02):
    12:24  segment download failed Mega Car Stunt Drive Car Games/
           analytics_app_store_discovery: Read timed out (120s)
    12:37  🚨 1 fetch failure(s) — Skipping delete+load entirely
           exit 1

    EK S3 read-timeout ki wajah se 5,523,049 rows (9 tables, 30 apps,
    37 minute ka kaam) raddi ho gaya. Baqi 29 apps bilkul theek thin.

v4 KE FIX:
  🛡️ 1. HTTP RETRY — har Apple/S3 call ab exponential backoff ke saath
        3 baar try hoti hai. Read-timeout sab se zyada retry-able error
        hai aur v3 mein uska ek bhi retry nahi tha.

  🛡️ 2. GUARD AB SCOPED HAI — nakami ab (table, app_id) par ginni jati hai,
        global nahi. Ek app ka discovery report fail ho to sirf USI app ka
        USI table chhoot-ta hai; baqi 29 apps aur 8 tables load ho jate hain.

  🛡️ 3. DELETE AB APP-SCOPED BHI HAI —
            date IN (...) AND app_id IN (kaamyaab apps)
        Nakaam app ka purana data chhua tak nahi jata.

  🛡️ 4. SALES PATH KA WAHI PURANA BUG THEEK — v3 mein `get_sales_report`
        HTTP error pe [] deta tha aur `date BETWEEN start AND end` DELETE
        us din ko uda deti thi. Ab sirf wo din delete hote hain jinka
        HTTP 200 aaya (`date IN (...)`).

  🛡️ 5. INSERT ab explicit column list ke saath — `SELECT *` column order
        pe bharosa karta tha; schema drift pe khamoshi se data kharab hota.

  🛡️ 6. Saare DELETE filters ab QUERY PARAMETERS se (string interpolation
        khatam).

Auth: JWT (ES256), auto-refreshed every 20 minutes

Tables (14):
  SALES (4):     sales_daily, subscription_daily, subscription_event_daily, subscriber_daily
  FINANCE (1):   finance_monthly
  ANALYTICS (9): analytics_sessions, analytics_installs, analytics_crashes,
                 analytics_app_store_discovery, analytics_app_store_downloads,
                 analytics_app_store_purchases, analytics_subscription_state,
                 analytics_app_store_web_preview, analytics_app_store_preorders
"""

import os, re, json, gzip, time, io, csv, sys, logging, requests
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery
from google.oauth2 import service_account
import jwt

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ─── CONFIG ──────────────────────────────────────────────────────────────────
APPLE_KEY_ID         = os.environ["APPLE_KEY_ID"].strip()
APPLE_ISSUER_ID      = os.environ["APPLE_ISSUER_ID"].strip()
APPLE_PRIVATE_KEY    = os.environ["APPLE_PRIVATE_KEY"].strip().replace("\\n", "\n")
APPLE_VENDOR_NUMBER  = os.environ["APPLE_VENDOR_NUMBER"].strip()
GCP_PROJECT          = os.environ["GCP_PROJECT"].strip()
BQ_DATASET           = os.environ.get("BQ_DATASET", "apple_store_data")
GCP_CREDENTIALS_JSON = os.environ["GCP_CREDENTIALS_JSON"]
SALES_LOOKBACK_DAYS     = int(os.environ.get("SALES_LOOKBACK_DAYS", "7"))
FINANCE_LOOKBACK_MONTHS = int(os.environ.get("FINANCE_LOOKBACK_MONTHS", "3"))

# 🆕 v4 retry knobs
HTTP_RETRIES      = int(os.environ.get("HTTP_RETRIES", "3"))
HTTP_BACKOFF      = int(os.environ.get("HTTP_BACKOFF", "5"))     # seconds, multiplied by attempt
SEGMENT_TIMEOUT   = int(os.environ.get("SEGMENT_TIMEOUT", "300"))  # v3 had 120 — too tight for big segments
API_TIMEOUT       = int(os.environ.get("API_TIMEOUT", "60"))

BASE_URL = "https://api.appstoreconnect.apple.com/v1"

# ─── JWT AUTH ─────────────────────────────────────────────────────────────────
_token_cache = {"token": None, "expires_at": 0}

def get_jwt():
    now = int(time.time())
    if _token_cache["token"] and now < _token_cache["expires_at"] - 60:
        return _token_cache["token"]
    payload = {
        "iss": APPLE_ISSUER_ID,
        "iat": now,
        "exp": now + 1200,
        "aud": "appstoreconnect-v1",
    }
    headers = {"alg": "ES256", "kid": APPLE_KEY_ID, "typ": "JWT"}
    token = jwt.encode(payload, APPLE_PRIVATE_KEY, algorithm="ES256", headers=headers)
    _token_cache["token"] = token
    _token_cache["expires_at"] = now + 1200
    return token

def auth():
    return {"Authorization": f"Bearer {get_jwt()}"}

# ─── 🆕 v4: HTTP RETRY ────────────────────────────────────────────────────────
RETRYABLE_STATUS = {429, 500, 502, 503, 504}

def http_get(url, *, params=None, headers=None, timeout=None, label="request",
             retries=None, use_auth=True):
    """v4: har HTTP call ab retry hoti hai.

    Run #181 ek S3 read-timeout pe mara tha aur v3 mein us ka ek bhi retry
    nahi tha. Read-timeout aur 5xx sab se zyada retry-able errors hain.

    Returns: requests.Response  |  raises last exception after all retries.
    """
    retries = retries if retries is not None else HTTP_RETRIES
    timeout = timeout if timeout is not None else API_TIMEOUT
    last_err = None

    for attempt in range(1, retries + 1):
        try:
            hdrs = dict(headers or {})
            if use_auth:
                hdrs.update(auth())          # fresh JWT every attempt
            resp = requests.get(url, params=params, headers=hdrs, timeout=timeout)
            if resp.status_code in RETRYABLE_STATUS and attempt < retries:
                last_err = RuntimeError(f"HTTP {resp.status_code}")
                wait = HTTP_BACKOFF * attempt
                log.warning(f"  [{label}] HTTP {resp.status_code} — retry "
                            f"{attempt}/{retries} in {wait}s")
                time.sleep(wait)
                continue
            return resp
        except (requests.Timeout, requests.ConnectionError) as e:
            last_err = e
            if attempt < retries:
                wait = HTTP_BACKOFF * attempt
                log.warning(f"  [{label}] {type(e).__name__} — retry "
                            f"{attempt}/{retries} in {wait}s")
                time.sleep(wait)
                continue
        except Exception as e:
            last_err = e
            break

    raise last_err if last_err else RuntimeError(f"{label}: unknown failure")

# ─── HELPERS ─────────────────────────────────────────────────────────────────
def sf(v):
    try: return float(v) if v not in (None, "", "--", "N/A") else None
    except Exception: return None

def si(v):
    try: return int(float(v)) if v not in (None, "", "--", "N/A") else None
    except Exception: return None

def now_ts():
    return datetime.utcnow().isoformat()

def parse_date(s):
    if not s: return None
    s = str(s).strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%Y%m%d"):
        try: return datetime.strptime(s[:10], fmt).strftime("%Y-%m-%d")
        except Exception: pass
    return s[:10] if len(s) >= 10 else s

def is_valid_date(s):
    return bool(re.match(r'^\d{4}-\d{2}-\d{2}$', str(s or "").strip()))

def tsv_rows(gz_bytes):
    try:
        with gzip.open(io.BytesIO(gz_bytes)) as gz:
            content = gz.read().decode("utf-8")
        return list(csv.DictReader(io.StringIO(content), delimiter="\t"))
    except Exception as e:
        log.warning(f"  TSV parse error: {e}")
        return []

def get_sales_date_range():
    end   = date.today() - timedelta(days=1)
    start = end - timedelta(days=SALES_LOOKBACK_DAYS - 1)
    return start, end

# ─── SCHEMAS ─────────────────────────────────────────────────────────────────
S = bigquery.SchemaField
SCHEMAS = {
    # app catalog — apple_id ↔ bundle_id ↔ sku
    # Ye table app_master_v2.ios_bundle_id bharne ke liye lazmi hai.
    "apps_dim": [
        S("apple_id",       "STRING"),
        S("bundle_id",      "STRING"),
        S("sku",            "STRING"),
        S("name",           "STRING"),
        S("primary_locale", "STRING"),
        S("_loaded_at",     "TIMESTAMP"),
    ],
    "sales_daily": [
        S("date","DATE"),
        S("provider","STRING"), S("provider_country","STRING"), S("sku","STRING"),
        S("developer","STRING"), S("title","STRING"), S("version","STRING"),
        S("product_type_id","STRING"), S("units","FLOAT"), S("developer_proceeds","FLOAT"),
        S("begins_period","DATE"), S("ends_period","DATE"),
        S("customer_currency","STRING"), S("country_code","STRING"),
        S("currency_of_proceeds","STRING"), S("apple_identifier","STRING"),
        S("customer_price","FLOAT"), S("promo_code","STRING"),
        S("parent_identifier","STRING"), S("subscription","STRING"),
        S("period","STRING"), S("category","STRING"), S("cmb","STRING"),
        S("device","STRING"), S("supported_platforms","STRING"),
        S("proceeds_reason","STRING"), S("preserved_pricing","STRING"),
        S("client","STRING"), S("order_type","STRING"),
        S("_ingested_at","TIMESTAMP"),
    ],
    "subscription_daily": [
        S("date","DATE"),
        S("app_name","STRING"), S("app_apple_id","STRING"),
        S("subscription_name","STRING"), S("subscription_apple_id","STRING"),
        S("subscription_group_id","STRING"), S("standard_subscription_duration","STRING"),
        S("promotional_offer_name","STRING"), S("promotional_offer_id","STRING"),
        S("customer_price","FLOAT"), S("customer_currency","STRING"),
        S("developer_proceeds","FLOAT"), S("proceeds_currency","STRING"),
        S("preserved_pricing","STRING"), S("proceeds_reason","STRING"),
        S("client","STRING"), S("device","STRING"), S("state","STRING"),
        S("country","STRING"),
        S("active_standard_price_subscriptions","FLOAT"),
        S("active_free_trial_introductory_offer_subscriptions","FLOAT"),
        S("active_pay_as_you_go_introductory_offer_subscriptions","FLOAT"),
        S("active_pay_up_front_introductory_offer_subscriptions","FLOAT"),
        S("active_promotional_offer_subscriptions","FLOAT"),
        S("free_trial_offer_code_subscriptions","FLOAT"),
        S("pay_as_you_go_offer_code_subscriptions","FLOAT"),
        S("pay_up_front_offer_code_subscriptions","FLOAT"),
        S("marketing_opt_ins","FLOAT"),
        S("_ingested_at","TIMESTAMP"),
    ],
    "subscription_event_daily": [
        S("event_date","DATE"), S("event","STRING"),
        S("app_name","STRING"), S("app_apple_id","STRING"),
        S("subscription_name","STRING"), S("subscription_apple_id","STRING"),
        S("subscription_group_id","STRING"), S("standard_subscription_duration","STRING"),
        S("subscription_offer_type","STRING"), S("subscription_offer_duration","STRING"),
        S("marketing_opt_in","STRING"), S("country","STRING"),
        S("state","STRING"), S("proceeds_reason","STRING"),
        S("preserved_pricing","STRING"), S("client","STRING"),
        S("device","STRING"), S("quantity","FLOAT"),
        S("_ingested_at","TIMESTAMP"),
    ],
    "subscriber_daily": [
        S("event_date","DATE"), S("app_name","STRING"), S("app_apple_id","STRING"),
        S("subscription_name","STRING"), S("subscription_apple_id","STRING"),
        S("subscription_group_id","STRING"), S("standard_subscription_duration","STRING"),
        S("customer_price","FLOAT"), S("customer_currency","STRING"),
        S("developer_proceeds","FLOAT"), S("proceeds_currency","STRING"),
        S("country","STRING"), S("quantity","FLOAT"),
        S("_ingested_at","TIMESTAMP"),
    ],
    "finance_monthly": [
        S("report_month","STRING"), S("start_date","DATE"), S("end_date","DATE"),
        S("vendor_identifier","STRING"), S("quantity","FLOAT"),
        S("partner_share","FLOAT"), S("extended_partner_share","FLOAT"),
        S("partner_share_currency","STRING"), S("sales_or_return","STRING"),
        S("apple_identifier","STRING"), S("title","STRING"),
        S("product_type_identifier","STRING"), S("units","FLOAT"),
        S("developer_proceeds","FLOAT"), S("begins_period","STRING"),
        S("ends_period","STRING"), S("customer_price","FLOAT"),
        S("customer_currency","STRING"), S("country_of_sale","STRING"),
        S("proceeds_reason","STRING"), S("preserved_pricing","STRING"),
        S("parent_identifier","STRING"),
        S("_ingested_at","TIMESTAMP"),
    ],
    "analytics_sessions": [
        S("date","DATE"), S("app_id","STRING"), S("app_name","STRING"),
        S("app_version","STRING"), S("device","STRING"),
        S("platform_version","STRING"), S("source_type","STRING"),
        S("page_type","STRING"), S("territory","STRING"),
        S("sessions","INTEGER"), S("total_session_duration","FLOAT"),
        S("unique_devices","INTEGER"), S("_ingested_at","TIMESTAMP"),
    ],
    "analytics_installs": [
        S("date","DATE"), S("app_id","STRING"), S("app_name","STRING"),
        S("event","STRING"), S("download_type","STRING"),
        S("app_version","STRING"), S("device","STRING"),
        S("platform_version","STRING"), S("source_type","STRING"),
        S("page_type","STRING"), S("territory","STRING"),
        S("counts","INTEGER"), S("unique_devices","INTEGER"),
        S("_ingested_at","TIMESTAMP"),
    ],
    "analytics_crashes": [
        S("date","DATE"), S("app_id","STRING"), S("app_name","STRING"),
        S("crashes","INTEGER"), S("crash_rate","FLOAT"),
        S("app_version","STRING"), S("device","STRING"),
        S("platform_version","STRING"), S("_ingested_at","TIMESTAMP"),
    ],
    "analytics_app_store_discovery": [
        S("date","DATE"), S("app_id","STRING"), S("app_name","STRING"),
        S("event","STRING"), S("page_type","STRING"),
        S("source_type","STRING"), S("engagement_type","STRING"),
        S("device","STRING"), S("platform_version","STRING"),
        S("territory","STRING"), S("counts","INTEGER"),
        S("unique_counts","INTEGER"), S("_ingested_at","TIMESTAMP"),
    ],
    "analytics_app_store_downloads": [
        S("date","DATE"), S("app_id","STRING"), S("app_name","STRING"),
        S("download_type","STRING"), S("app_version","STRING"),
        S("device","STRING"), S("platform_version","STRING"),
        S("source_type","STRING"), S("page_type","STRING"),
        S("territory","STRING"), S("counts","INTEGER"),
        S("_ingested_at","TIMESTAMP"),
    ],
    "analytics_app_store_purchases": [
        S("date","DATE"), S("app_id","STRING"), S("app_name","STRING"),
        S("purchase_type","STRING"), S("content_name","STRING"),
        S("device","STRING"), S("platform_version","STRING"),
        S("source_type","STRING"), S("page_type","STRING"),
        S("territory","STRING"), S("purchases","INTEGER"),
        S("proceeds_usd","FLOAT"), S("sales_usd","FLOAT"),
        S("paying_users","INTEGER"), S("_ingested_at","TIMESTAMP"),
    ],
    "analytics_subscription_state": [
        S("date","DATE"), S("app_id","STRING"), S("app_name","STRING"),
        S("subscription_name","STRING"), S("subscription_apple_id","STRING"),
        S("subscription_group_id","STRING"),
        S("paid_subscriptions","INTEGER"), S("free_trials","INTEGER"),
        S("paid_offers","INTEGER"), S("billing_retry","INTEGER"),
        S("grace_period","INTEGER"), S("voluntary_churn","INTEGER"),
        S("involuntary_churn","INTEGER"), S("source_type","STRING"),
        S("territory","STRING"), S("device","STRING"),
        S("_ingested_at","TIMESTAMP"),
    ],
    "analytics_app_store_web_preview": [
        S("date","DATE"), S("app_id","STRING"), S("app_name","STRING"),
        S("impressions","INTEGER"), S("page_views","INTEGER"),
        S("taps","INTEGER"), S("source_type","STRING"),
        S("page_type","STRING"), S("territory","STRING"),
        S("_ingested_at","TIMESTAMP"),
    ],
    "analytics_app_store_preorders": [
        S("date","DATE"), S("app_id","STRING"), S("app_name","STRING"),
        S("preorders","INTEGER"), S("canceled_preorders","INTEGER"),
        S("source_type","STRING"), S("territory","STRING"),
        S("device","STRING"), S("_ingested_at","TIMESTAMP"),
    ],
}

ANALYTICS_REPORT_MAP = {
    "App Sessions Standard":                        "analytics_sessions",
    "App Store Installation and Deletion Standard": "analytics_installs",
    "App Crashes":                                  "analytics_crashes",
    "App Store Discovery and Engagement Standard":  "analytics_app_store_discovery",
    "App Downloads Standard":                       "analytics_app_store_downloads",
    "App Store Purchases Standard":                 "analytics_app_store_purchases",
    "Subscription State":                           "analytics_subscription_state",
    "App Store Web Preview":                        "analytics_app_store_web_preview",
    "App Store Pre-Orders Standard":                "analytics_app_store_preorders",
}

# ─── SALES REPORTS   🛡️ v4: (rows, ok) — nakami ab chhupti nahi ─────────────
def get_sales_report(report_type, report_subtype, frequency, report_date):
    """v4 returns (rows, ok).

    v3 har surat mein [] deta tha — HTTP 500 aur "report maujood nahi"
    bilkul ek jaise dikhte the. Phir `date BETWEEN` DELETE us din ko uda
    deti thi. Ab `ok=False` wale din ko haath hi nahi lagate.
    """
    params = {
        "filter[vendorNumber]":  APPLE_VENDOR_NUMBER,
        "filter[reportType]":    report_type,
        "filter[reportSubType]": report_subtype,
        "filter[frequency]":     frequency,
        "filter[reportDate]":    str(report_date),
    }
    try:
        resp = http_get(f"{BASE_URL}/salesReports", params=params,
                        timeout=API_TIMEOUT, label=f"{report_type}/{report_date}")
        if resp.status_code == 200:
            return tsv_rows(resp.content), True
        if resp.status_code in (400, 404):
            # Report genuinely nahi bani. Ye nakami nahi — magar hum us din
            # ka purana data bhi delete NAHI karenge (ok=False).
            return [], False
        log.warning(f"  {report_type}/{report_date}: HTTP {resp.status_code} "
                    f"— day will NOT be deleted")
        return [], False
    except Exception as e:
        log.warning(f"  {report_type}/{report_date}: {e} — day will NOT be deleted")
        return [], False

def _fetch_sales_generic(label, report_type, subtype, mapper):
    """Shared loop for all 4 sales tables. Returns (rows, ok_days)."""
    log.info(f"Fetching {label}...")
    rows, ok_days = [], []
    start, end = get_sales_date_range()
    current, done, total = start, 0, (end - start).days + 1
    while current <= end:
        day_str = current.strftime("%Y-%m-%d")
        raw, ok = get_sales_report(report_type, subtype, "DAILY", day_str)
        if ok:
            ok_days.append(day_str)
            for r in raw:
                rows.append(mapper(r, current))
        done += 1
        if done % 30 == 0:
            log.info(f"  {label}: {done}/{total} days, {len(rows)} rows")
        current += timedelta(days=1)
    log.info(f"  ✓ {label}: {len(rows)} rows across {len(ok_days)}/{total} fetched days")
    return rows, ok_days

def _map_sales(r, current):
    return {
        "date":                 current.strftime("%Y-%m-%d"),
        "provider":             r.get("Provider"),
        "provider_country":     r.get("Provider Country"),
        "sku":                  r.get("SKU"),
        "developer":            r.get("Developer"),
        "title":                r.get("Title"),
        "version":              r.get("Version"),
        "product_type_id":      r.get("Product Type Identifier"),
        "units":                sf(r.get("Units")),
        "developer_proceeds":   sf(r.get("Developer Proceeds")),
        "begins_period":        parse_date(r.get("Begin Date")),
        "ends_period":          parse_date(r.get("End Date")),
        "customer_currency":    r.get("Customer Currency"),
        "country_code":         r.get("Country Code"),
        "currency_of_proceeds": r.get("Currency of Proceeds"),
        "apple_identifier":     r.get("Apple Identifier"),
        "customer_price":       sf(r.get("Customer Price")),
        "promo_code":           r.get("Promo Code"),
        "parent_identifier":    r.get("Parent Identifier"),
        "subscription":         r.get("Subscription"),
        "period":               r.get("Period"),
        "category":             r.get("Category"),
        "cmb":                  r.get("CMB"),
        "device":               r.get("Device"),
        "supported_platforms":  r.get("Supported Platforms"),
        "proceeds_reason":      r.get("Proceeds Reason"),
        "preserved_pricing":    r.get("Preserved Pricing"),
        "client":               r.get("Client"),
        "order_type":           r.get("Order Type"),
        "_ingested_at":         now_ts(),
    }

def _map_subscription(r, current):
    return {
        "date": str(current),
        "app_name": r.get("App Name"), "app_apple_id": r.get("App Apple ID"),
        "subscription_name": r.get("Subscription Name"),
        "subscription_apple_id": r.get("Subscription Apple ID"),
        "subscription_group_id": r.get("Subscription Group ID"),
        "standard_subscription_duration": r.get("Standard Subscription Duration"),
        "promotional_offer_name": r.get("Promotional Offer Name"),
        "promotional_offer_id": r.get("Promotional Offer ID"),
        "customer_price": sf(r.get("Customer Price")),
        "customer_currency": r.get("Customer Currency"),
        "developer_proceeds": sf(r.get("Developer Proceeds")),
        "proceeds_currency": r.get("Proceeds Currency"),
        "preserved_pricing": r.get("Preserved Pricing"),
        "proceeds_reason": r.get("Proceeds Reason"),
        "client": r.get("Client"), "device": r.get("Device"),
        "state": r.get("State"), "country": r.get("Country"),
        "active_standard_price_subscriptions": sf(r.get("Active Standard Price Subscriptions")),
        "active_free_trial_introductory_offer_subscriptions": sf(r.get("Active Free Trial Introductory Offer Subscriptions")),
        "active_pay_as_you_go_introductory_offer_subscriptions": sf(r.get("Active Pay As You Go Introductory Offer Subscriptions")),
        "active_pay_up_front_introductory_offer_subscriptions": sf(r.get("Active Pay Up Front Introductory Offer Subscriptions")),
        "active_promotional_offer_subscriptions": sf(r.get("Active Promotional Offer Subscriptions")),
        "free_trial_offer_code_subscriptions": sf(r.get("Free Trial Offer Code Subscriptions")),
        "pay_as_you_go_offer_code_subscriptions": sf(r.get("Pay As You Go Offer Code Subscriptions")),
        "pay_up_front_offer_code_subscriptions": sf(r.get("Pay Up Front Offer Code Subscriptions")),
        "marketing_opt_ins": sf(r.get("Marketing Opt-Ins")),
        "_ingested_at": now_ts(),
    }

def _map_sub_event(r, current):
    return {
        "event_date": parse_date(r.get("Event Date")),
        "event": r.get("Event"), "app_name": r.get("App Name"),
        "app_apple_id": r.get("App Apple ID"),
        "subscription_name": r.get("Subscription Name"),
        "subscription_apple_id": r.get("Subscription Apple ID"),
        "subscription_group_id": r.get("Subscription Group ID"),
        "standard_subscription_duration": r.get("Standard Subscription Duration"),
        "subscription_offer_type": r.get("Subscription Offer Type"),
        "subscription_offer_duration": r.get("Subscription Offer Duration"),
        "marketing_opt_in": r.get("Marketing Opt-In"),
        "country": r.get("Country"), "state": r.get("State"),
        "proceeds_reason": r.get("Proceeds Reason"),
        "preserved_pricing": r.get("Preserved Pricing"),
        "client": r.get("Client"), "device": r.get("Device"),
        "quantity": sf(r.get("Quantity")),
        "_ingested_at": now_ts(),
    }

def _map_subscriber(r, current):
    return {
        "event_date": parse_date(r.get("Event Date")),
        "app_name": r.get("App Name"), "app_apple_id": r.get("App Apple ID"),
        "subscription_name": r.get("Subscription Name"),
        "subscription_apple_id": r.get("Subscription Apple ID"),
        "subscription_group_id": r.get("Subscription Group ID"),
        "standard_subscription_duration": r.get("Standard Subscription Duration"),
        "customer_price": sf(r.get("Customer Price")),
        "customer_currency": r.get("Customer Currency"),
        "developer_proceeds": sf(r.get("Developer Proceeds")),
        "proceeds_currency": r.get("Proceeds Currency"),
        "country": r.get("Country"),
        "quantity": sf(r.get("Quantity")),
        "_ingested_at": now_ts(),
    }

def fetch_sales_daily():
    return _fetch_sales_generic("sales_daily", "SALES", "SUMMARY", _map_sales)

def fetch_subscription_daily():
    return _fetch_sales_generic("subscription_daily", "SUBSCRIPTION", "SUMMARY", _map_subscription)

def fetch_subscription_event_daily():
    return _fetch_sales_generic("subscription_event_daily", "SUBSCRIPTION_EVENT", "SUMMARY", _map_sub_event)

def fetch_subscriber_daily():
    return _fetch_sales_generic("subscriber_daily", "SUBSCRIBER", "DETAILED", _map_subscriber)

# ─── FINANCE REPORTS ──────────────────────────────────────────────────────────
def fetch_finance_monthly():
    log.info("Fetching Finance Monthly...")
    rows = []
    today = date.today()
    for i in range(1, FINANCE_LOOKBACK_MONTHS + 1):
        report_month = today - relativedelta(months=i)
        report_date  = report_month.strftime("%Y-%m")
        params = {
            "filter[vendorNumber]": APPLE_VENDOR_NUMBER,
            "filter[reportType]":   "FINANCIAL",
            "filter[regionCode]":   "ZZ",
            "filter[reportDate]":   report_date,
        }
        try:
            resp = http_get(f"{BASE_URL}/financeReports", params=params,
                            timeout=API_TIMEOUT, label=f"finance/{report_date}")
            if resp.status_code == 200:
                month_rows = 0
                for r in tsv_rows(resp.content):
                    raw_start = parse_date(r.get("Start Date"))
                    if not is_valid_date(raw_start):
                        continue
                    rows.append({
                        "report_month": report_date,
                        "start_date": raw_start,
                        "end_date": parse_date(r.get("End Date")),
                        "vendor_identifier": r.get("Vendor Identifier"),
                        "quantity": sf(r.get("Quantity")),
                        "partner_share": sf(r.get("Partner Share")),
                        "extended_partner_share": sf(r.get("Extended Partner Share")),
                        "partner_share_currency": r.get("Partner Share Currency"),
                        "sales_or_return": r.get("Sales or Return"),
                        "apple_identifier": r.get("Apple Identifier"),
                        "title": r.get("Title"),
                        "product_type_identifier": r.get("Product Type Identifier"),
                        "units": sf(r.get("Units")),
                        "developer_proceeds": sf(r.get("Developer Proceeds")),
                        "begins_period": r.get("Begin Date"),
                        "ends_period": r.get("End Date"),
                        "customer_price": sf(r.get("Customer Price")),
                        "customer_currency": r.get("Customer Currency"),
                        "country_of_sale": r.get("Country Of Sale"),
                        "proceeds_reason": r.get("Proceeds Reason"),
                        "preserved_pricing": r.get("Preserved Pricing"),
                        "parent_identifier": r.get("Parent Identifier"),
                        "_ingested_at": now_ts(),
                    })
                    month_rows += 1
                if month_rows: log.info(f"  Finance {report_date}: {month_rows} rows")
            elif resp.status_code in (400, 404):
                pass
            else:
                log.warning(f"  Finance {report_date}: HTTP {resp.status_code}")
        except Exception as e:
            log.warning(f"  Finance {report_date}: {e}")
    log.info(f"  ✓ finance_monthly: {len(rows)} rows")
    return rows

# ─── ANALYTICS ────────────────────────────────────────────────────────────────
def get_all_apps():
    apps, url = [], f"{BASE_URL}/apps"
    params = {"limit": 200}
    while url:
        try:
            resp = http_get(url, params=params, timeout=API_TIMEOUT, label="apps.list")
            data = resp.json()
            for a in data.get("data", []):
                at = a.get("attributes", {}) or {}
                apps.append({
                    "id":             a["id"],
                    "name":           at.get("name", ""),
                    "bundle_id":      at.get("bundleId"),
                    "sku":            at.get("sku"),
                    "primary_locale": at.get("primaryLocale"),
                })
            url = data.get("links", {}).get("next")
            params = {}
        except Exception as e:
            log.warning(f"  Apps list error: {e}")
            break
    log.info(f"  Found {len(apps)} apps")
    return apps

def ensure_analytics_request(app_id):
    try:
        resp = http_get(f"{BASE_URL}/apps/{app_id}/analyticsReportRequests",
                        params={"filter[accessType]": "ONGOING"},
                        timeout=30, label=f"reportRequests/{app_id}")
        existing = resp.json().get("data", [])
        if existing:
            return existing[0]["id"]
        payload = {"data": {
            "type": "analyticsReportRequests",
            "attributes": {"accessType": "ONGOING"},
            "relationships": {"app": {"data": {"type": "apps", "id": app_id}}}
        }}
        resp2 = requests.post(f"{BASE_URL}/analyticsReportRequests",
                              json=payload, headers=auth(), timeout=30)
        if resp2.status_code in (200, 201):
            return resp2.json()["data"]["id"]
        log.warning(f"  create reportRequest {app_id}: HTTP {resp2.status_code}")
    except Exception as e:
        log.warning(f"  Analytics request error for {app_id}: {e}")
    return None

def parse_analytics_row(r, table_name, app_id, app_name, proc_date):
    base = {
        "date":     parse_date(r.get("Date") or r.get("date") or proc_date),
        "app_id":   app_id,
        "app_name": app_name,
        "_ingested_at": now_ts(),
    }
    if table_name == "analytics_sessions":
        return {**base,
            "app_version":            r.get("App Version"),
            "device":                 r.get("Device"),
            "platform_version":       r.get("Platform Version"),
            "source_type":            r.get("Source Type"),
            "page_type":              r.get("Page Type"),
            "territory":              r.get("Territory"),
            "sessions":               si(r.get("Sessions")),
            "total_session_duration": sf(r.get("Total Session Duration")),
            "unique_devices":         si(r.get("Unique Devices")),
        }
    elif table_name == "analytics_installs":
        return {**base,
            "event":            r.get("Event"),
            "download_type":    r.get("Download Type"),
            "app_version":      r.get("App Version"),
            "device":           r.get("Device"),
            "platform_version": r.get("Platform Version"),
            "source_type":      r.get("Source Type"),
            "page_type":        r.get("Page Type"),
            "territory":        r.get("Territory"),
            "counts":           si(r.get("Counts")),
            "unique_devices":   si(r.get("Unique Devices")),
        }
    elif table_name == "analytics_crashes":
        return {**base,
            "crashes": si(r.get("Crashes") or r.get("Total Crashes")),
            "crash_rate": sf(r.get("Crash Rate")),
            "app_version": r.get("App Version"),
            "device": r.get("Device"), "platform_version": r.get("Platform Version"),
        }
    elif table_name == "analytics_app_store_discovery":
        return {**base,
            "event":            r.get("Event"),
            "page_type":        r.get("Page Type"),
            "source_type":      r.get("Source Type"),
            "engagement_type":  r.get("Engagement Type"),
            "device":           r.get("Device"),
            "platform_version": r.get("Platform Version"),
            "territory":        r.get("Territory"),
            "counts":           si(r.get("Counts")),
            "unique_counts":    si(r.get("Unique Counts")),
        }
    elif table_name == "analytics_app_store_downloads":
        return {**base,
            "download_type":    r.get("Download Type"),
            "app_version":      r.get("App Version"),
            "device":           r.get("Device"),
            "platform_version": r.get("Platform Version"),
            "source_type":      r.get("Source Type"),
            "page_type":        r.get("Page Type"),
            "territory":        r.get("Territory"),
            "counts":           si(r.get("Counts")),
        }
    elif table_name == "analytics_app_store_purchases":
        return {**base,
            "purchase_type":    r.get("Purchase Type"),
            "content_name":     r.get("Content Name"),
            "device":           r.get("Device"),
            "platform_version": r.get("Platform Version"),
            "source_type":      r.get("Source Type"),
            "page_type":        r.get("Page Type"),
            "territory":        r.get("Territory"),
            "purchases":        si(r.get("Purchases")),
            "proceeds_usd":     sf(r.get("Proceeds in USD")),
            "sales_usd":        sf(r.get("Sales in USD")),
            "paying_users":     si(r.get("Paying Users")),
        }
    elif table_name == "analytics_subscription_state":
        return {**base,
            "subscription_name": r.get("Subscription Name"),
            "subscription_apple_id": r.get("Subscription Apple ID"),
            "subscription_group_id": r.get("Subscription Group ID"),
            "paid_subscriptions": si(r.get("Paid Subscriptions")),
            "free_trials": si(r.get("Free Trial Subscriptions") or r.get("Free Trials")),
            "paid_offers": si(r.get("Paid Offer Subscriptions") or r.get("Paid Offers")),
            "billing_retry": si(r.get("Billing Retry Subscriptions") or r.get("Billing Retry")),
            "grace_period": si(r.get("Grace Period Subscriptions") or r.get("Grace Period")),
            "voluntary_churn": si(r.get("Voluntary Churn")),
            "involuntary_churn": si(r.get("Involuntary Churn")),
            "source_type": r.get("Source Type"), "territory": r.get("Territory"),
            "device": r.get("Device"),
        }
    elif table_name == "analytics_app_store_web_preview":
        return {**base,
            "impressions": si(r.get("Impressions")),
            "page_views": si(r.get("Page Views")),
            "taps": si(r.get("Taps")),
            "source_type": r.get("Source Type"), "page_type": r.get("Page Type"),
            "territory": r.get("Territory"),
        }
    elif table_name == "analytics_app_store_preorders":
        return {**base,
            "preorders": si(r.get("Pre-Orders") or r.get("Preorders")),
            "canceled_preorders": si(r.get("Canceled Pre-Orders") or r.get("Canceled Preorders")),
            "source_type": r.get("Source Type"), "territory": r.get("Territory"),
            "device": r.get("Device"),
        }
    return base

# 🛡️ v4: nakami ab (table, app_id) par — global nahi
def fetch_all_analytics(apps):
    """Returns (results, failed_pairs, attempted_pairs).

    failed_pairs / attempted_pairs = set of (table_name, app_id).
    v3 sirf ek global counter rakhta tha, is liye ek app ka ek timeout
    poore 30 apps ka load rok deta tha (run #181).
    """
    log.info(f"Fetching Analytics for {len(apps)} apps...")
    results   = {t: [] for t in ANALYTICS_REPORT_MAP.values()}
    failed    = set()
    attempted = set()

    for app in apps:
        app_id, app_name = app["id"], app["name"]
        request_id = ensure_analytics_request(app_id)
        if not request_id:
            log.warning(f"  No analytics request for {app_name} — all tables skipped")
            for t in ANALYTICS_REPORT_MAP.values():
                attempted.add((t, app_id))
                failed.add((t, app_id))
            continue

        try:
            resp = http_get(f"{BASE_URL}/analyticsReportRequests/{request_id}/reports",
                            timeout=30, label=f"reports/{app_name}")
            reports = resp.json().get("data", [])
        except Exception as e:
            log.warning(f"  Reports list error {app_name}: {e} — all tables skipped")
            for t in ANALYTICS_REPORT_MAP.values():
                attempted.add((t, app_id))
                failed.add((t, app_id))
            continue

        for report in reports:
            report_id   = report["id"]
            report_name = report["attributes"].get("name", "")
            table_name  = None
            for key, tbl in ANALYTICS_REPORT_MAP.items():
                if key.lower() in report_name.lower():
                    table_name = tbl
                    break
            if not table_name:
                continue      # hamara table nahi — nakami nahi

            attempted.add((table_name, app_id))

            try:
                resp = http_get(f"{BASE_URL}/analyticsReports/{report_id}/instances",
                                params={"filter[granularity]": "DAILY", "limit": 200},
                                timeout=30, label=f"instances/{app_name}")
                instances = resp.json().get("data", [])
            except Exception as e:
                log.warning(f"  instances error {app_name}/{table_name}: {e}")
                failed.add((table_name, app_id))
                continue

            for instance in instances:
                instance_id = instance["id"]
                proc_date   = instance["attributes"].get("processingDate", "")
                try:
                    resp = http_get(
                        f"{BASE_URL}/analyticsReportInstances/{instance_id}/segments",
                        timeout=30, label=f"segments/{app_name}")
                    segments = resp.json().get("data", [])
                except Exception as e:
                    log.warning(f"  segments error {app_name}/{table_name}: {e}")
                    failed.add((table_name, app_id))
                    continue

                for seg in segments:
                    dl_url = seg["attributes"].get("url")
                    if not dl_url:
                        continue
                    try:
                        # 🛡️ v4: ye wahi call hai jo run #181 mein mari thi.
                        #    Ab 3 retries + 300s timeout (v3: 0 retries, 120s).
                        dl = http_get(dl_url, timeout=SEGMENT_TIMEOUT,
                                      use_auth=False,
                                      label=f"segment/{app_name}/{table_name}")
                        if dl.status_code != 200:
                            log.warning(f"  segment HTTP {dl.status_code} "
                                        f"{app_name}/{table_name}")
                            failed.add((table_name, app_id))
                            continue
                        for r in tsv_rows(dl.content):
                            results[table_name].append(
                                parse_analytics_row(r, table_name, app_id,
                                                    app_name, proc_date))
                    except Exception as e:
                        log.warning(f"  segment download failed "
                                    f"{app_name}/{table_name}: {e}")
                        failed.add((table_name, app_id))
                        continue

        time.sleep(0.2)

    for tbl, rows in results.items():
        n_fail = sum(1 for (t, _) in failed if t == tbl)
        n_att  = sum(1 for (t, _) in attempted if t == tbl)
        suffix = f"  (⚠️ {n_fail}/{n_att} apps failed)" if n_fail else ""
        log.info(f"  ✓ {tbl}: {len(rows)} rows{suffix}")

    if failed:
        log.warning(f"  ⚠️  {len(failed)} (table, app) pair(s) failed — those "
                    f"specific apps/tables will NOT be touched. Everything "
                    f"else loads normally.")
    return results, failed, attempted

# ─── BIGQUERY ─────────────────────────────────────────────────────────────────
def dedup_rows(rows, key_fields):
    """Deduplicate rows by key fields — keeps last occurrence."""
    seen = {}
    for r in rows:
        key = tuple(r.get(f) for f in key_fields)
        seen[key] = r
    return list(seen.values())

ANALYTICS_DEDUP_KEYS = {
    "analytics_sessions":              ["date", "app_id", "app_version", "device", "platform_version", "source_type", "page_type", "territory"],
    "analytics_installs":              ["date", "app_id", "event", "download_type", "app_version", "device", "platform_version", "source_type", "page_type", "territory"],
    "analytics_app_store_discovery":   ["date", "app_id", "event", "page_type", "source_type", "engagement_type", "device", "platform_version", "territory"],
    "analytics_app_store_downloads":   ["date", "app_id", "download_type", "app_version", "device", "platform_version", "source_type", "page_type", "territory"],
    "analytics_app_store_purchases":   ["date", "app_id", "purchase_type", "content_name", "device", "platform_version", "source_type", "page_type", "territory"],
    "analytics_subscription_state":    ["date", "app_id", "subscription_name", "source_type", "territory", "device"],
    "analytics_app_store_web_preview": ["date", "app_id", "page_type", "source_type", "territory"],
    "analytics_app_store_preorders":   ["date", "app_id", "source_type", "territory", "device"],
    "analytics_crashes":               ["date", "app_id", "app_version", "device", "platform_version"],
}

def get_bq():
    creds = service_account.Credentials.from_service_account_info(
        json.loads(GCP_CREDENTIALS_JSON),
        scopes=["https://www.googleapis.com/auth/cloud-platform"])
    return bigquery.Client(project=GCP_PROJECT, credentials=creds)

def ensure_dataset(bq):
    try:
        bq.get_dataset(BQ_DATASET)
    except Exception:
        log.info(f"Creating dataset {BQ_DATASET}")
        bq.create_dataset(bigquery.Dataset(f"{GCP_PROJECT}.{BQ_DATASET}"))

def ensure_table(bq, name):
    ref = bq.dataset(BQ_DATASET).table(name)
    try:
        bq.get_table(ref)
    except Exception:
        log.info(f"Creating table {name}")
        bq.create_table(bigquery.Table(ref, schema=SCHEMAS[name]))

# 🛡️ v4: ATOMIC + explicit columns + query parameters
def load_to_bq(bq, name, rows, delete_where=None, params=None):
    """DELETE aur INSERT ya dono chalte hain ya kuch nahi.

    v4: `SELECT *` ki jagah explicit column list — v3 column ORDER pe
    bharosa karta tha; schema drift pe khamoshi se data kharab hota.
    """
    if not rows:
        log.info(f"  No rows for {name}")
        return

    table_ref = f"{GCP_PROJECT}.{BQ_DATASET}.{name}"
    stg_ref   = f"{table_ref}_stg"
    cols      = ", ".join(f.name for f in SCHEMAS[name])

    # Step 1: PEHLE staging mein. Yahan fail hua to asli table salamat.
    job_config = bigquery.LoadJobConfig(
        schema=SCHEMAS[name],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    bq.load_table_from_json(rows, stg_ref, job_config=job_config).result()

    # Step 2: ek atomic transaction
    where = f"WHERE {delete_where}" if delete_where else ""
    sql = f"""
        BEGIN TRANSACTION;
          DELETE FROM `{table_ref}` {where};
          INSERT INTO `{table_ref}` ({cols}) SELECT {cols} FROM `{stg_ref}`;
        COMMIT TRANSACTION;
    """
    cfg = bigquery.QueryJobConfig(query_parameters=params or [])
    bq.query(sql, job_config=cfg).result()
    log.info(f"  ✅ {len(rows):,} rows → {name} (atomic)")

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    log.info("🍎 Apple App Store Connect → BigQuery v4 (14 tables)")
    log.info(f"   Dataset:          {BQ_DATASET}")
    log.info(f"   Sales lookback:   {SALES_LOOKBACK_DAYS} days")
    log.info(f"   Finance lookback: {FINANCE_LOOKBACK_MONTHS} months")
    log.info(f"   HTTP retries:     {HTTP_RETRIES} · segment timeout {SEGMENT_TIMEOUT}s")

    bq = get_bq()
    ensure_dataset(bq)
    for t in SCHEMAS:
        ensure_table(bq, t)

    exit_code = 0

    # ══════════ SALES — 🛡️ v4: sirf kaamyaab din delete hote hain ══════════
    log.info("── Sales Reports ──")
    for tbl, fetcher, date_col in [
        ("sales_daily",              fetch_sales_daily,              "date"),
        ("subscription_daily",       fetch_subscription_daily,       "date"),
        ("subscription_event_daily", fetch_subscription_event_daily, "event_date"),
        ("subscriber_daily",         fetch_subscriber_daily,         "event_date"),
    ]:
        rows, ok_days = fetcher()
        if not ok_days:
            log.warning(f"  {tbl}: 0 days fetched successfully — nothing touched")
            continue
        load_to_bq(
            bq, tbl, rows,
            delete_where=f"{date_col} IN UNNEST(@days)",
            params=[bigquery.ArrayQueryParameter("days", "DATE", ok_days)],
        )

    # ══════════ FINANCE ══════════
    log.info("── Finance Reports ──")
    finance_rows = fetch_finance_monthly()
    if finance_rows:
        months = sorted({r["report_month"] for r in finance_rows if r.get("report_month")})
        load_to_bq(
            bq, "finance_monthly", finance_rows,
            delete_where="report_month IN UNNEST(@months)",
            params=[bigquery.ArrayQueryParameter("months", "STRING", months)],
        )
    else:
        log.warning("  No finance rows — nothing deleted, nothing loaded")

    # ══════════ ANALYTICS ══════════
    log.info("── Analytics Reports ──")
    apps = get_all_apps()

    if not apps:
        log.error("🚨 No apps found — skipping analytics entirely "
                  "(existing data preserved).")
        sys.exit(1)

    _ts = now_ts()
    _dim = [{
        "apple_id":       str(a.get("id") or ""),
        "bundle_id":      a.get("bundle_id"),
        "sku":            a.get("sku"),
        "name":           a.get("name"),
        "primary_locale": a.get("primary_locale"),
        "_loaded_at":     _ts,
    } for a in apps]
    _nb = sum(1 for d in _dim if d["bundle_id"])
    load_to_bq(bq, "apps_dim", _dim, delete_where="TRUE")
    log.info(f"  🔑 apps_dim: {len(_dim)} apps · bundle_id bhara {_nb}/{len(_dim)}")
    if _nb == 0:
        log.warning("  ⚠️  bundle_id kisi par nahi aaya — API key ka ROLE check karein "
                    "(App Store Connect → Users and Access → Integrations → Keys → "
                    "'App Manager' ya 'Admin' chahiye)")

    analytics, failed_pairs, attempted_pairs = fetch_all_analytics(apps)

    for table_name, rows in analytics.items():
        failed_apps = {a for (t, a) in failed_pairs if t == table_name}

        # 🛡️ GUARD 1 (v4, scoped): nakaam app ki rows load hi nahi hotin —
        #    wo adhoori hain. Aur us app ka purana data delete bhi nahi hota.
        if failed_apps:
            before = len(rows)
            rows = [r for r in rows if r.get("app_id") not in failed_apps]
            log.warning(f"  {table_name}: {len(failed_apps)} app(s) failed — "
                        f"dropped {before - len(rows):,} partial rows, "
                        f"their existing data left untouched")
            exit_code = 1

        if not rows:
            log.info(f"  No loadable rows for {table_name}")
            continue

        key_fields = ANALYTICS_DEDUP_KEYS.get(table_name)
        if key_fields:
            before = len(rows)
            rows = dedup_rows(rows, key_fields)
            if len(rows) < before:
                log.info(f"  Deduped {table_name}: {before} → {len(rows)} rows")

        # 🛡️ GUARD 2: range NAHI — sirf wahi din jo asal mein aaye hain.
        days = sorted({str(r["date"])[:10] for r in rows
                       if r.get("date") and is_valid_date(str(r["date"])[:10])})
        if not days:
            log.warning(f"  {table_name}: no usable dates — skipping")
            continue

        # 🛡️ GUARD 3 (v4): DELETE ab app-scoped bhi hai. Sirf un apps ka
        #    data hatega jinka data is run mein kaamyabi se aaya.
        ok_apps = sorted({r["app_id"] for r in rows if r.get("app_id")})
        if not ok_apps:
            log.warning(f"  {table_name}: no app_ids — skipping")
            continue

        log.info(f"  {table_name}: replacing {len(days)} day(s) "
                 f"({days[0]} … {days[-1]}) × {len(ok_apps)} app(s)")
        load_to_bq(
            bq, table_name, rows,
            delete_where="date IN UNNEST(@days) AND app_id IN UNNEST(@apps)",
            params=[
                bigquery.ArrayQueryParameter("days", "DATE",   days),
                bigquery.ArrayQueryParameter("apps", "STRING", ok_apps),
            ],
        )

    if exit_code:
        log.error("⚠️  Sync finished with partial failures — see warnings above. "
                  "Loaded data is complete for every app/table that succeeded; "
                  "failed pairs were left untouched. Re-run to pick them up.")
    else:
        log.info("✅ Apple App Store Connect sync v4 complete! 14 tables.")
    sys.exit(exit_code)

if __name__ == "__main__":
    main()

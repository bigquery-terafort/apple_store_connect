"""
Apple App Store Connect → BigQuery  ·  COMPLETE PIPELINE v2
============================================================
Fixes vs v1:
  1. Replaced insert_rows_json (streaming) with load_table_from_json (batch load jobs)
  2. Delete-before-insert for all sales/analytics tables by date range
  3. Finance monthly: delete by report_month before loading
  4. subscription_daily: added date column to schema and fetch function
  5. Analytics: track min/max date in fetched data and delete that range before load

Auth: JWT (ES256), auto-refreshed every 20 minutes

Tables (14):
  SALES (4):     sales_daily, subscription_daily, subscription_event_daily, subscriber_daily
  FINANCE (1):   finance_monthly
  ANALYTICS (9): analytics_sessions, analytics_installs, analytics_crashes,
                 analytics_app_store_discovery, analytics_app_store_downloads,
                 analytics_app_store_purchases, analytics_subscription_state,
                 analytics_app_store_web_preview, analytics_app_store_preorders
"""

import os, re, json, gzip, time, io, csv, logging, requests
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

# ─── HELPERS ─────────────────────────────────────────────────────────────────
def sf(v):
    try: return float(v) if v not in (None, "", "--", "N/A") else None
    except: return None

def si(v):
    try: return int(float(v)) if v not in (None, "", "--", "N/A") else None
    except: return None

def now_ts():
    return datetime.utcnow().isoformat()

def parse_date(s):
    if not s: return None
    s = str(s).strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%Y%m%d"):
        try: return datetime.strptime(s[:10], fmt).strftime("%Y-%m-%d")
        except: pass
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
    "sales_daily": [
        S("date","DATE"),  # report date — the date we fetched this report for
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
        # FIX v2: Added date column — was missing, impossible to dedup without it
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
        S("sessions","INTEGER"), S("active_devices","INTEGER"),
        S("average_session_duration_seconds","FLOAT"),
        S("source_type","STRING"), S("device","STRING"),
        S("platform_version","STRING"), S("_ingested_at","TIMESTAMP"),
    ],
    "analytics_installs": [
        S("date","DATE"), S("app_id","STRING"), S("app_name","STRING"),
        S("installations","INTEGER"), S("first_time_downloads","INTEGER"),
        S("redownloads","INTEGER"), S("deletions","INTEGER"),
        S("source_type","STRING"), S("device","STRING"),
        S("platform_version","STRING"), S("territory","STRING"),
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
        S("impressions","INTEGER"), S("impressions_unique_device","INTEGER"),
        S("page_views","INTEGER"), S("page_views_unique_device","INTEGER"),
        S("taps","INTEGER"), S("source_type","STRING"),
        S("page_type","STRING"), S("event_type","STRING"),
        S("territory","STRING"), S("device","STRING"),
        S("_ingested_at","TIMESTAMP"),
    ],
    "analytics_app_store_downloads": [
        S("date","DATE"), S("app_id","STRING"), S("app_name","STRING"),
        S("total_downloads","INTEGER"), S("first_time_downloads","INTEGER"),
        S("redownloads","INTEGER"), S("source_type","STRING"),
        S("territory","STRING"), S("device","STRING"),
        S("platform_version","STRING"), S("_ingested_at","TIMESTAMP"),
    ],
    "analytics_app_store_purchases": [
        S("date","DATE"), S("app_id","STRING"), S("app_name","STRING"),
        S("purchases","INTEGER"), S("paying_users","INTEGER"),
        S("proceeds","FLOAT"), S("proceeds_currency","STRING"),
        S("purchase_type","STRING"), S("content_name","STRING"),
        S("source_type","STRING"), S("territory","STRING"),
        S("device","STRING"), S("_ingested_at","TIMESTAMP"),
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
    "App Sessions":                       "analytics_sessions",
    "App Installations and Deletions":    "analytics_installs",
    "App Crashes":                        "analytics_crashes",
    "App Store Discovery and Engagement": "analytics_app_store_discovery",
    "App Store Downloads":                "analytics_app_store_downloads",
    "App Store Purchases":                "analytics_app_store_purchases",
    "Subscription State":                 "analytics_subscription_state",
    "App Store Web Preview":              "analytics_app_store_web_preview",
    "App Store Pre-orders":               "analytics_app_store_preorders",
}

# Tables with a "date" column — delete by date range before insert
DATE_TABLES = {
    "sales_daily",
    "subscription_daily",
    "subscription_event_daily",
    "subscriber_daily",
    "analytics_sessions", "analytics_installs", "analytics_crashes",
    "analytics_app_store_discovery", "analytics_app_store_downloads",
    "analytics_app_store_purchases", "analytics_subscription_state",
    "analytics_app_store_web_preview", "analytics_app_store_preorders",
}

# Date column name per table
DATE_COL = {
    "sales_daily":                      "begins_period",
    "subscription_daily":               "date",
    "subscription_event_daily":         "event_date",
    "subscriber_daily":                 "event_date",
    "analytics_sessions":               "date",
    "analytics_installs":               "date",
    "analytics_crashes":                "date",
    "analytics_app_store_discovery":    "date",
    "analytics_app_store_downloads":    "date",
    "analytics_app_store_purchases":    "date",
    "analytics_subscription_state":     "date",
    "analytics_app_store_web_preview":  "date",
    "analytics_app_store_preorders":    "date",
}

# ─── SALES REPORTS ───────────────────────────────────────────────────────────
def get_sales_report(report_type, report_subtype, frequency, report_date):
    params = {
        "filter[vendorNumber]":  APPLE_VENDOR_NUMBER,
        "filter[reportType]":    report_type,
        "filter[reportSubType]": report_subtype,
        "filter[frequency]":     frequency,
        "filter[reportDate]":    str(report_date),
    }
    try:
        resp = requests.get(f"{BASE_URL}/salesReports", params=params,
                            headers=auth(), timeout=60)
        if resp.status_code == 200:
            return tsv_rows(resp.content)
        elif resp.status_code in (400, 404):
            return []
        else:
            log.warning(f"  {report_type}/{report_date}: HTTP {resp.status_code}")
            return []
    except Exception as e:
        log.warning(f"  {report_type}/{report_date}: {e}")
        return []

def fetch_sales_daily():
    log.info("Fetching Sales Daily...")
    rows = []
    start, end = get_sales_date_range()
    current, done, total = start, 0, (end - start).days + 1
    while current <= end:
        for r in get_sales_report("SALES", "SUMMARY", "DAILY", current.strftime("%Y-%m-%d")):
            rows.append({
                "date":               current.strftime("%Y-%m-%d"),  # report date
                "provider": r.get("Provider"),
                "provider_country": r.get("Provider Country"),
                "sku": r.get("SKU"), "developer": r.get("Developer"),
                "title": r.get("Title"), "version": r.get("Version"),
                "product_type_id": r.get("Product Type Identifier"),
                "units": sf(r.get("Units")),
                "developer_proceeds": sf(r.get("Developer Proceeds")),
                "begins_period": parse_date(r.get("Begin Date")),
                "ends_period": parse_date(r.get("End Date")),
                "customer_currency": r.get("Customer Currency"),
                "country_code": r.get("Country Code"),
                "currency_of_proceeds": r.get("Currency of Proceeds"),
                "apple_identifier": r.get("Apple Identifier"),
                "customer_price": sf(r.get("Customer Price")),
                "promo_code": r.get("Promo Code"),
                "parent_identifier": r.get("Parent Identifier"),
                "subscription": r.get("Subscription"),
                "period": r.get("Period"), "category": r.get("Category"),
                "cmb": r.get("CMB"), "device": r.get("Device"),
                "supported_platforms": r.get("Supported Platforms"),
                "proceeds_reason": r.get("Proceeds Reason"),
                "preserved_pricing": r.get("Preserved Pricing"),
                "client": r.get("Client"), "order_type": r.get("Order Type"),
                "_ingested_at": now_ts(),
            })
        done += 1
        if done % 30 == 0: log.info(f"  Sales: {done}/{total} days, {len(rows)} rows")
        current += timedelta(days=1)
    log.info(f"  ✓ sales_daily: {len(rows)} rows")
    return rows

def fetch_subscription_daily():
    log.info("Fetching Subscription Daily...")
    rows = []
    start, end = get_sales_date_range()
    current = start
    while current <= end:
        for r in get_sales_report("SUBSCRIPTION", "SUMMARY", "DAILY", current.strftime("%Y-%m-%d")):
            rows.append({
                # FIX v2: date field added — was missing in v1
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
            })
        current += timedelta(days=1)
    log.info(f"  ✓ subscription_daily: {len(rows)} rows")
    return rows

def fetch_subscription_event_daily():
    log.info("Fetching Subscription Event Daily...")
    rows = []
    start, end = get_sales_date_range()
    current = start
    while current <= end:
        for r in get_sales_report("SUBSCRIPTION_EVENT", "SUMMARY", "DAILY", current.strftime("%Y-%m-%d")):
            rows.append({
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
            })
        current += timedelta(days=1)
    log.info(f"  ✓ subscription_event_daily: {len(rows)} rows")
    return rows

def fetch_subscriber_daily():
    log.info("Fetching Subscriber Daily...")
    rows = []
    start, end = get_sales_date_range()
    current = start
    while current <= end:
        for r in get_sales_report("SUBSCRIBER", "DETAILED", "DAILY", current.strftime("%Y-%m-%d")):
            rows.append({
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
            })
        current += timedelta(days=1)
    log.info(f"  ✓ subscriber_daily: {len(rows)} rows")
    return rows

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
            resp = requests.get(f"{BASE_URL}/financeReports", params=params,
                                headers=auth(), timeout=60)
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
            resp = requests.get(url, params=params, headers=auth(), timeout=60)
            data = resp.json()
            for a in data.get("data", []):
                apps.append({"id": a["id"], "name": a["attributes"].get("name", "")})
            url = data.get("links", {}).get("next")
            params = {}
        except Exception as e:
            log.warning(f"  Apps list error: {e}")
            break
    log.info(f"  Found {len(apps)} apps")
    return apps

def ensure_analytics_request(app_id):
    try:
        resp = requests.get(
            f"{BASE_URL}/apps/{app_id}/analyticsReportRequests",
            params={"filter[accessType]": "ONGOING"},
            headers=auth(), timeout=30
        )
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
            "sessions": si(r.get("Sessions") or r.get("Total Sessions")),
            "active_devices": si(r.get("Active Devices") or r.get("Unique Devices")),
            "average_session_duration_seconds": sf(r.get("Average Session Duration (Seconds)")),
            "source_type": r.get("Source Type"), "device": r.get("Device"),
            "platform_version": r.get("Platform Version"),
        }
    elif table_name == "analytics_installs":
        return {**base,
            "installations": si(r.get("Installations") or r.get("Total Installations")),
            "first_time_downloads": si(r.get("First Time Downloads")),
            "redownloads": si(r.get("Re-Downloads") or r.get("Redownloads")),
            "deletions": si(r.get("Deletions")),
            "source_type": r.get("Source Type"), "device": r.get("Device"),
            "platform_version": r.get("Platform Version"), "territory": r.get("Territory"),
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
            "impressions": si(r.get("Impressions")),
            "impressions_unique_device": si(r.get("Impressions Unique Device")),
            "page_views": si(r.get("Page Views")),
            "page_views_unique_device": si(r.get("Page Views Unique Device")),
            "taps": si(r.get("Taps")),
            "source_type": r.get("Source Type"), "page_type": r.get("Page Type"),
            "event_type": r.get("Event Type"), "territory": r.get("Territory"),
            "device": r.get("Device"),
        }
    elif table_name == "analytics_app_store_downloads":
        return {**base,
            "total_downloads": si(r.get("Total Downloads") or r.get("Downloads")),
            "first_time_downloads": si(r.get("First Time Downloads")),
            "redownloads": si(r.get("Re-Downloads") or r.get("Redownloads")),
            "source_type": r.get("Source Type"), "territory": r.get("Territory"),
            "device": r.get("Device"), "platform_version": r.get("Platform Version"),
        }
    elif table_name == "analytics_app_store_purchases":
        return {**base,
            "purchases": si(r.get("Purchases")),
            "paying_users": si(r.get("Paying Users")),
            "proceeds": sf(r.get("Proceeds")),
            "proceeds_currency": r.get("Proceeds Currency"),
            "purchase_type": r.get("Purchase Type"), "content_name": r.get("Content Name"),
            "source_type": r.get("Source Type"), "territory": r.get("Territory"),
            "device": r.get("Device"),
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

def fetch_all_analytics(apps):
    log.info(f"Fetching Analytics for {len(apps)} apps...")
    results = {t: [] for t in ANALYTICS_REPORT_MAP.values()}

    for app in apps:
        app_id, app_name = app["id"], app["name"]
        request_id = ensure_analytics_request(app_id)
        if not request_id:
            continue

        try:
            resp = requests.get(
                f"{BASE_URL}/analyticsReportRequests/{request_id}/reports",
                headers=auth(), timeout=30
            )
            reports = resp.json().get("data", [])
        except Exception as e:
            log.warning(f"  Reports list error {app_name}: {e}")
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
                continue

            try:
                resp = requests.get(
                    f"{BASE_URL}/analyticsReports/{report_id}/instances",
                    params={"filter[granularity]": "DAILY", "limit": 200},
                    headers=auth(), timeout=30
                )
                instances = resp.json().get("data", [])
            except:
                continue

            for instance in instances:
                instance_id = instance["id"]
                proc_date   = instance["attributes"].get("processingDate", "")
                try:
                    resp = requests.get(
                        f"{BASE_URL}/analyticsReportInstances/{instance_id}/segments",
                        headers=auth(), timeout=30
                    )
                    segments = resp.json().get("data", [])
                except:
                    continue

                for seg in segments:
                    dl_url = seg["attributes"].get("url")
                    if not dl_url:
                        continue
                    try:
                        dl = requests.get(dl_url, timeout=120)
                        if dl.status_code == 200:
                            for r in tsv_rows(dl.content):
                                parsed = parse_analytics_row(r, table_name, app_id, app_name, proc_date)
                                results[table_name].append(parsed)
                    except:
                        pass

        time.sleep(0.2)

    for tbl, rows in results.items():
        log.info(f"  ✓ {tbl}: {len(rows)} rows")
    return results

# ─── BIGQUERY ─────────────────────────────────────────────────────────────────
def get_bq():
    creds = service_account.Credentials.from_service_account_info(
        json.loads(GCP_CREDENTIALS_JSON),
        scopes=["https://www.googleapis.com/auth/cloud-platform"])
    return bigquery.Client(project=GCP_PROJECT, credentials=creds)

def ensure_dataset(bq):
    try: bq.get_dataset(BQ_DATASET)
    except:
        log.info(f"Creating dataset {BQ_DATASET}")
        bq.create_dataset(bigquery.Dataset(f"{GCP_PROJECT}.{BQ_DATASET}"))

def ensure_table(bq, name):
    ref = bq.dataset(BQ_DATASET).table(name)
    try: bq.get_table(ref)
    except:
        log.info(f"Creating table {name}")
        bq.create_table(bigquery.Table(ref, schema=SCHEMAS[name]))

def load_to_bq(bq, name, rows, delete_filter=None):
    """
    FIX v2: Batch load jobs — no streaming buffer.
    delete_filter: SQL WHERE clause string for clearing existing data.
    """
    if not rows:
        log.info(f"  No rows for {name}")
        return

    table_ref = f"{GCP_PROJECT}.{BQ_DATASET}.{name}"

    # Step 1: Delete existing data
    if delete_filter:
        try:
            bq.query(f"DELETE FROM `{table_ref}` WHERE {delete_filter}").result()
            log.info(f"  Cleared {name} ({delete_filter})")
        except Exception as e:
            log.warning(f"  Could not clear {name}: {e}")

    # Step 2: Batch load job
    try:
        job_config = bigquery.LoadJobConfig(
            schema=SCHEMAS[name],
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        load_job = bq.load_table_from_json(rows, table_ref, job_config=job_config)
        load_job.result()
        log.info(f"  ✅ {len(rows):,} rows → {name}")
    except Exception as e:
        log.error(f"  Load job failed [{name}]: {e}")

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    log.info("🍎 Apple App Store Connect → BigQuery v2 (14 tables)")
    log.info(f"   Sales lookback:   {SALES_LOOKBACK_DAYS} days")
    log.info(f"   Finance lookback: {FINANCE_LOOKBACK_MONTHS} months")

    bq = get_bq()
    ensure_dataset(bq)
    for t in SCHEMAS:
        ensure_table(bq, t)

    start, end = get_sales_date_range()
    sales_filter = f"date BETWEEN '{start}' AND '{end}'"  # report date column
    sub_filter   = f"date BETWEEN '{start}' AND '{end}'"
    evt_filter   = f"event_date BETWEEN '{start}' AND '{end}'"

    log.info("── Sales Reports ──")
    load_to_bq(bq, "sales_daily",              fetch_sales_daily(),              sales_filter)
    load_to_bq(bq, "subscription_daily",       fetch_subscription_daily(),       sub_filter)
    load_to_bq(bq, "subscription_event_daily", fetch_subscription_event_daily(), evt_filter)
    load_to_bq(bq, "subscriber_daily",         fetch_subscriber_daily(),         evt_filter)

    log.info("── Finance Reports ──")
    finance_rows = fetch_finance_monthly()
    # FIX v2: Delete by each report_month before loading — no duplicate months
    today = date.today()
    for i in range(1, FINANCE_LOOKBACK_MONTHS + 1):
        report_month = (today - relativedelta(months=i)).strftime("%Y-%m")
        try:
            bq.query(
                f"DELETE FROM `{GCP_PROJECT}.{BQ_DATASET}.finance_monthly` "
                f"WHERE report_month = '{report_month}'"
            ).result()
            log.info(f"  Cleared finance_monthly for {report_month}")
        except Exception as e:
            log.warning(f"  Could not clear finance_monthly {report_month}: {e}")
    # Load all finance rows in one batch
    load_to_bq(bq, "finance_monthly", finance_rows)  # no delete_filter — already cleared above

    log.info("── Analytics Reports ──")
    apps = get_all_apps()
    if apps:
        analytics = fetch_all_analytics(apps)
        for table_name, rows in analytics.items():
            if not rows:
                log.info(f"  No rows for {table_name}")
                continue
            # FIX v2: Delete date range found in fetched data before loading
            dates = [r.get("date") for r in rows if r.get("date")]
            if dates:
                min_d, max_d = min(dates), max(dates)
                analytics_filter = f"date BETWEEN '{min_d}' AND '{max_d}'"
                load_to_bq(bq, table_name, rows, analytics_filter)
            else:
                load_to_bq(bq, table_name, rows)
    else:
        log.warning("  No apps found — skipping analytics")

    log.info("✅ Apple App Store Connect sync v2 complete! 14 tables.")

if __name__ == "__main__":
    main()

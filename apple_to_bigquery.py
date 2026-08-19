"""
================================================================================
  apps_dim  —  App Store Connect  →  BigQuery
================================================================================
  Ye module `apple_to_bigquery.py` mein PASTE karna hai.

  🔑 KYA KARTA HAI
     App Store Connect ke /v1/apps endpoint se saare apps ki list leta hai:
         apple_id  ·  bundle_id  ·  sku  ·  name
     aur BigQuery mein `<dataset>.apps_dim` table banata hai.

  🔴 MASLA JO YE HAL KARTA HAI
     app_master_v2 mein 6 iOS apps par `ios_bundle_id` KHAALI hai —
     kyunke sales/analytics report bundle_id deti hi nahi.
     Sirf /v1/apps deta hai.

     Halat (2026-08-18):
       terafort.apple_console_terafort_us.apps_dim  →  hai, magar 26 JULY se BAND
       terafort.apple_store_data.apps_dim (Spartans) →  BANTI HI NAHI

  ⚠️ API KEY KA ROLE — SAB SE AHEM
     /v1/apps ke liye key ka role "App Manager" / "Admin" / "Developer"
     hona chahiye. Agar key sirf "Sales and Reports" par hai to
     403 Forbidden aayega — reports chalti rahengi magar apps_dim nahi banegi.

     Ye function 403 par SAAF error deta hai, chup-chaap fail nahi hota.

  📍 KAHAN LAGAYEIN
     main() mein, sales/finance sync se PEHLE (ya baad mein — koi farq nahi):
         sync_apps_dim(token, bq, project, dataset)
================================================================================
"""
import sys
import time

import requests
from google.cloud import bigquery

ASC_BASE = "https://api.appstoreconnect.apple.com/v1"


def fetch_apps(token: str, timeout: int = 60) -> list:
    """
    /v1/apps se saare apps — pagination ke saath.

    Har app par milta hai:
        id                     -> apple_id  (misaal: "6783663881")
        attributes.bundleId    -> com.sp.aivoicechanger.sound.effects
        attributes.sku         -> developer ki apni SKU
        attributes.name        -> app ka naam
        attributes.primaryLocale
    """
    apps, url = [], f"{ASC_BASE}/apps?limit=200"
    page = 0

    while url:
        page += 1
        r = requests.get(url, headers={"Authorization": f"Bearer {token}"},
                         timeout=timeout)

        # ── 🔴 role ka masla — saaf batayein, chup na rahein ──
        if r.status_code == 403:
            raise PermissionError(
                "403 Forbidden on /v1/apps.\n"
                "   App Store Connect key ka ROLE kaafi nahi hai.\n"
                "   'Sales and Reports' role reports to de deta hai, magar\n"
                "   apps ki list NAHI parh sakta.\n"
                "   → App Store Connect → Users and Access → Integrations\n"
                "     → Keys → key ka role 'App Manager' ya 'Admin' karein\n"
                "     (ya nayi key banayein)."
            )
        if r.status_code == 401:
            raise PermissionError(
                "401 Unauthorized on /v1/apps — JWT token ghalat ya expire.\n"
                "   KEY_ID / ISSUER_ID / PRIVATE_KEY check karein."
            )
        r.raise_for_status()

        j = r.json()
        for a in j.get("data", []):
            at = a.get("attributes", {}) or {}
            apps.append({
                "apple_id":       str(a.get("id") or ""),
                "bundle_id":      at.get("bundleId"),
                "sku":            at.get("sku"),
                "name":           at.get("name"),
                "primary_locale": at.get("primaryLocale"),
            })

        url = (j.get("links") or {}).get("next")
        if url:
            time.sleep(0.5)          # rate limit se bachne ke liye
        if page > 50:                # bhaagti hui loop se hifazat
            print("⚠️  50 se zyada page — ruk rahe hain", flush=True)
            break

    return apps


def sync_apps_dim(token: str, bq: bigquery.Client,
                  project: str, dataset: str) -> int:
    """
    apps_dim table banata/refresh karta hai.

    ⚠️ ATOMIC: pehle temp table mein load, phir TRANSACTION mein swap.
       Beech mein fail ho to PURANI TABLE SALAMAT rehti hai.
       (v1 mein DELETE-then-load tha — us se data gum ho sakta tha.)
    """
    print("── apps_dim ────────────────────────────────────────", flush=True)

    try:
        apps = fetch_apps(token)
    except PermissionError as exc:
        # 🔴 role ka masla — poori pipeline na roken, magar SAAF batayein
        print(f"🔴 apps_dim SKIP: {exc}", file=sys.stderr, flush=True)
        return 0

    if not apps:
        print("⚠️  /v1/apps se 0 apps mile — table ko haath nahi lagaya",
              flush=True)
        return 0

    target = f"{project}.{dataset}.apps_dim"
    tmp    = f"{project}.{dataset}._tmp_apps_dim"

    schema = [
        bigquery.SchemaField("apple_id",       "STRING"),
        bigquery.SchemaField("bundle_id",      "STRING"),
        bigquery.SchemaField("sku",            "STRING"),
        bigquery.SchemaField("name",           "STRING"),
        bigquery.SchemaField("primary_locale", "STRING"),
        bigquery.SchemaField("_loaded_at",     "TIMESTAMP"),
    ]
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()
    for a in apps:
        a["_loaded_at"] = now

    # 1. temp mein load
    bq.load_table_from_json(
        apps, tmp,
        job_config=bigquery.LoadJobConfig(
            schema=schema, write_disposition="WRITE_TRUNCATE"),
    ).result()

    try:
        # 2. atomic swap
        try:
            bq.get_table(target)
            bq.query(f"""
                BEGIN TRANSACTION;
                  DELETE FROM `{target}` WHERE TRUE;
                  INSERT INTO `{target}`
                    (apple_id, bundle_id, sku, name, primary_locale, _loaded_at)
                  SELECT apple_id, bundle_id, sku, name, primary_locale, _loaded_at
                  FROM `{tmp}`;
                COMMIT TRANSACTION;
            """).result()
        except Exception:
            bq.query(f"CREATE TABLE `{target}` AS SELECT * FROM `{tmp}`").result()
            print(f"   🆕 table bani: {target}", flush=True)
    finally:
        bq.query(f"DROP TABLE IF EXISTS `{tmp}`").result()

    n_bundle = sum(1 for a in apps if a.get("bundle_id"))
    print(f"   ✅ {len(apps):,} apps → apps_dim "
          f"(bundle_id bhara: {n_bundle}/{len(apps)})", flush=True)
    return len(apps)

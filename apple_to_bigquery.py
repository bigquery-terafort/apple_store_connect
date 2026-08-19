          INSERT INTO `{table_ref}` SELECT * FROM `{stg_ref}`;
        COMMIT TRANSACTION;
    """).result()
    log.info(f"  ✅ {len(rows):,} rows → {name} (atomic)")

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    log.info("🍎 Apple App Store Connect → BigQuery v3 (14 tables)")
    log.info(f"   Dataset:          {BQ_DATASET}")
    log.info(f"   Sales lookback:   {SALES_LOOKBACK_DAYS} days")
    log.info(f"   Finance lookback: {FINANCE_LOOKBACK_MONTHS} months")

    bq = get_bq()
    ensure_dataset(bq)
    for t in SCHEMAS:
        ensure_table(bq, t)

    start, end = get_sales_date_range()
    sales_filter = f"date BETWEEN '{start}' AND '{end}'"
    sub_filter   = f"date BETWEEN '{start}' AND '{end}'"
    evt_filter   = f"event_date BETWEEN '{start}' AND '{end}'"

    log.info("── Sales Reports ──")
    load_to_bq(bq, "sales_daily",              fetch_sales_daily(),              sales_filter)
    load_to_bq(bq, "subscription_daily",       fetch_subscription_daily(),       sub_filter)
    load_to_bq(bq, "subscription_event_daily", fetch_subscription_event_daily(), evt_filter)
    load_to_bq(bq, "subscriber_daily",         fetch_subscriber_daily(),         evt_filter)

    log.info("── Finance Reports ──")
    finance_rows = fetch_finance_monthly()
    if finance_rows:
        # 🛡️ v3: months ki LIST se DELETE (range nahi), aur atomic load_to_bq se
        months = sorted({r["report_month"] for r in finance_rows if r.get("report_month")})
        month_list = ",".join(f"'{m}'" for m in months)
        load_to_bq(bq, "finance_monthly", finance_rows,
                   f"report_month IN ({month_list})")
    else:
        log.warning("  No finance rows — nothing deleted, nothing loaded")

    log.info("── Analytics Reports ──")
    apps = get_all_apps()
    if not apps:
        log.error("🚨 No apps found — skipping analytics entirely "
                  "(existing data preserved).")
        sys.exit(1)

    analytics, failures = fetch_all_analytics(apps)

    # 🛡️ GUARD 1: adhoora fetch = DELETE bilkul nahi.
    #    Yehi guard na hone ki wajah se Jan/Feb/Mar/May aur June ke 18 din gaye.
    if failures:
        log.error(f"🚨 {failures} fetch failure(s) — analytics rows are "
                  f"INCOMPLETE. Skipping delete+load entirely so existing "
                  f"data is preserved. Fix access and re-run.")
        sys.exit(1)

    for table_name, rows in analytics.items():
        if not rows:
            log.info(f"  No rows for {table_name}")
            continue

        key_fields = ANALYTICS_DEDUP_KEYS.get(table_name)
        if key_fields:
            before = len(rows)
            rows = dedup_rows(rows, key_fields)
            if len(rows) < before:
                log.info(f"  Deduped {table_name}: {before} → {len(rows)} rows")

        # 🛡️ GUARD 2: range NAHI — sirf wahi din jo asal mein aaye hain.
        #    `BETWEEN min AND max` beech ke un dinon ko bhi uda deta tha jo
        #    is baar nahi aaye. Aadha saal isi ek lafz se gaya.
        days = sorted({str(r["date"])[:10] for r in rows if r.get("date")})
        if not days:
            log.warning(f"  {table_name}: no usable dates — skipping")
            continue

        day_list = ",".join(f"'{d}'" for d in days)
        log.info(f"  {table_name}: replacing {len(days)} day(s) "
                 f"({days[0]} … {days[-1]})")
        load_to_bq(bq, table_name, rows, f"date IN ({day_list})")

    log.info("✅ Apple App Store Connect sync v3 complete! 14 tables.")

if __name__ == "__main__":
    main()

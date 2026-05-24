import dagster as dg
import requests
from datetime import datetime


@dg.asset(required_resource_keys={"currency_rates_db"})
def exchange_rates(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    rates_db = context.resources.currency_rates_db

    last_ts = rates_db.get_last_timestamp()
    if last_ts:
        start = last_ts.strftime("%Y-%m-%d")
    else:
        start = "2023-06-01"

    end = datetime.now().strftime("%Y-%m-%d")

    if start >= end:
        context.log.info("Exchange rates are up to date")
        return dg.MaterializeResult(metadata={"rates_inserted": 0})

    url = f"https://api.frankfurter.app/{start}..{end}?from=EUR&to=GBP"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    }

    resp = requests.get(url, headers=headers, timeout=30)
    data = resp.json()

    inserted = 0
    for date_str, rates in data.get("rates", {}).items():
        rate = rates.get("GBP")
        if rate:
            ts = f"{date_str} 00:00:00"
            if rates_db.insert_rate("EUR", "GBP", rate, ts):
                inserted += 1

    context.log.info(f"Inserted {inserted} exchange rates from {start} to {end}")
    return dg.MaterializeResult(metadata={"rates_inserted": inserted})
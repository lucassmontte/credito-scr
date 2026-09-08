"""
Fetch the Selic target rate (Meta Selic, Bacen SGS series 432) and load
monthly end-of-month values into BigQuery as marts.dim_taxa_selic.

ASSUMPTION FLAGGED (see decisions.md): series 432 is the Copom-set target
rate. This has NOT been verified against a real API call from the agent's
side (network sandbox cannot reach api.bcb.gov.br). The script prints a
preview and pauses for confirmation before loading anything — check the
printed values look like plausible Selic rates (roughly 2%-15% a.a.
across 2023-2025) before confirming.

Usage:
    python fetch_selic.py

Requires:
    pip install requests google-cloud-bigquery
"""

import requests
from datetime import datetime
from google.cloud import bigquery

PROJECT_ID = "credito-scr"
TABLE_ID = f"{PROJECT_ID}.marts.dim_taxa_selic"

# Bacen SGS API: series 432 = Meta Selic definida pelo Copom (% a.a.)
SGS_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados?formato=json&dataInicial=01/01/2023&dataFinal=31/12/2026"


def fetch_selic_daily():
    print(f"Fetching {SGS_URL} ...")
    response = requests.get(SGS_URL, timeout=60)
    response.raise_for_status()
    data = response.json()
    print(f"  Got {len(data)} daily records")
    return data


def to_month_end_values(daily_records):
    """
    Bacen SGS returns daily records like {"data": "02/01/2023", "valor": "13.75"}.
    Reduce to one value per month: the last available record on or before
    the last calendar day of each month, matched against our data_base
    convention (last day of month, from Day 2's parsing).
    """
    parsed = []
    for rec in daily_records:
        d = datetime.strptime(rec["data"], "%d/%m/%Y").date()
        v = float(rec["valor"].replace(",", "."))
        parsed.append((d, v))
    parsed.sort()

    month_end = {}
    for d, v in parsed:
        key = (d.year, d.month)
        # keep overwriting — last record seen per month wins, since sorted ascending
        month_end[key] = v

    from calendar import monthrange
    rows = []
    for (year, month), value in sorted(month_end.items()):
        last_day = monthrange(year, month)[1]
        rows.append({
            "data_base": f"{year:04d}-{month:02d}-{last_day:02d}",
            "selic_meta_aa": value,
        })
    return rows


def main():
    daily = fetch_selic_daily()
    monthly_rows = to_month_end_values(daily)

    print("\nPreview (first 6 and last 6 months):")
    for row in monthly_rows[:6]:
        print(f"  {row['data_base']}  selic_meta_aa = {row['selic_meta_aa']}")
    print("  ...")
    for row in monthly_rows[-6:]:
        print(f"  {row['data_base']}  selic_meta_aa = {row['selic_meta_aa']}")

    confirm = input(
        "\nDo these look like plausible Selic rates (roughly 2%-15% a.a.)? "
        "Type 'yes' to load into BigQuery, anything else to abort: "
    )
    if confirm.strip().lower() != "yes":
        print("Aborted — no data loaded. Adjust the script and re-check before retrying.")
        return

    client = bigquery.Client(project=PROJECT_ID)
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("data_base", "DATE"),
            bigquery.SchemaField("selic_meta_aa", "FLOAT64"),
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    load_job = client.load_table_from_json(monthly_rows, TABLE_ID, job_config=job_config)
    load_job.result()
    print(f"\nLoaded {len(monthly_rows)} rows into {TABLE_ID}")


if __name__ == "__main__":
    main()

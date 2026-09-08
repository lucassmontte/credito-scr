"""
Cloud Function (2nd gen, HTTP-triggered), meant to be called monthly by
Cloud Scheduler.

Downloads only the target month's CSV from Bacen's annual ZIP, uploads it
to GCS if not already present (idempotent), then submits a Dataflow job
scoped to ONLY that new file — so WRITE_APPEND on staging.scr_data stays
correct (appends one new month, not a reprocess of everything).

Correction from an earlier draft: launching Dataflow from here does NOT
require a Flex Template. This function installs apache-beam[gcp] directly
and submits the job the same way the manual `python beam_pipeline.py
--runner=DataflowRunner` runs did throughout this project — submitting a
job is fast (seconds); it runs asynchronously on Dataflow's own workers,
not inside this function. See decisions.md.

ASSUMPTION (flagged, not verified — cannot reach bcb.gov.br from this
agent's sandbox to confirm): Bacen publishes a given month's SCR.data
roughly 30-45 days after month end. Scheduled for day 20 of each month,
targeting the PREVIOUS month, as a safety margin — not a confirmed
publication date.

Requires (requirements.txt):
    functions-framework
    requests
    google-cloud-storage
    apache-beam[gcp]
"""

import csv
import io
import re
import zipfile
from calendar import monthrange
from datetime import date

import functions_framework
import requests
from google.cloud import storage

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition

PROJECT_ID = "credito-scr"
BUCKET_NAME = "credito-scr-raw"
BCB_URL_TEMPLATE = "https://www.bcb.gov.br/pda/desig/scrdata_{year}.zip"
STAGING_TABLE = f"{PROJECT_ID}:staging.scr_data"
TEMP_LOCATION = f"gs://{BUCKET_NAME}/tmp"
REGION = "southamerica-east1"

# Same schema/parsing as beam_pipeline.py (Day 2/3) — kept in sync
# manually. A future improvement would be sharing this as a common module
# instead of duplicating it between the Cloud Shell script and this
# function.
COLUMNS = [
    "data_base", "uf", "segmento", "cliente", "cnae_ocupacao", "porte",
    "modalidade", "submodalidade", "origem", "indexador",
    "numero_de_operacoes",
    "a_vencer_ate_90_dias", "a_vencer_de_91_ate_360_dias",
    "a_vencer_de_361_ate_1080_dias", "a_vencer_de_1081_ate_1800_dias",
    "a_vencer_de_1801_ate_5400_dias", "a_vencer_acima_de_5400_dias",
    "carteira_a_vencer",
    "vencido_de_15_ate_90_dias", "vencido_acima_de_90_dias",
    "carteira_vencida", "carteira_ativa",
    "carteira_inadimplencia", "ativo_problematico",
]
NUMERIC_COLUMNS = COLUMNS[10:]

BQ_SCHEMA = {
    "fields": [
        {"name": "data_base", "type": "DATE"},
        {"name": "uf", "type": "STRING"},
        {"name": "segmento", "type": "STRING"},
        {"name": "cliente", "type": "STRING"},
        {"name": "cnae_ocupacao", "type": "STRING"},
        {"name": "porte", "type": "STRING"},
        {"name": "modalidade", "type": "STRING"},
        {"name": "submodalidade", "type": "STRING"},
        {"name": "origem", "type": "STRING"},
        {"name": "indexador", "type": "STRING"},
        {"name": "numero_de_operacoes", "type": "INT64"},
        {"name": "a_vencer_ate_90_dias", "type": "FLOAT64"},
        {"name": "a_vencer_de_91_ate_360_dias", "type": "FLOAT64"},
        {"name": "a_vencer_de_361_ate_1080_dias", "type": "FLOAT64"},
        {"name": "a_vencer_de_1081_ate_1800_dias", "type": "FLOAT64"},
        {"name": "a_vencer_de_1801_ate_5400_dias", "type": "FLOAT64"},
        {"name": "a_vencer_acima_de_5400_dias", "type": "FLOAT64"},
        {"name": "carteira_a_vencer", "type": "FLOAT64"},
        {"name": "vencido_de_15_ate_90_dias", "type": "FLOAT64"},
        {"name": "vencido_acima_de_90_dias", "type": "FLOAT64"},
        {"name": "carteira_vencida", "type": "FLOAT64"},
        {"name": "carteira_ativa", "type": "FLOAT64"},
        {"name": "carteira_inadimplencia", "type": "FLOAT64"},
        {"name": "ativo_problematico", "type": "FLOAT64"},
        {"name": "source_file", "type": "STRING"},
    ]
}


def previous_month(today: date) -> tuple[int, int]:
    if today.month == 1:
        return today.year - 1, 12
    return today.year, today.month - 1


def parse_brl_number(raw: str) -> float:
    raw = raw.strip()
    if raw == "" or raw.upper() in ("NA", "NULL", "-"):
        return 0.0
    return float(raw.replace(".", "").replace(",", "."))


def parse_data_base(raw: str) -> str:
    raw = raw.strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", raw):
        return raw
    raise ValueError(f"Unrecognized data_base format: {raw!r}")


class ParseScrDataRow(beam.DoFn):
    def process(self, element, source_file):
        line = element.strip()
        if not line:
            return
        try:
            fields = next(csv.reader([line], delimiter=";", quotechar='"'))
        except StopIteration:
            return
        if len(fields) != len(COLUMNS):
            return
        row = dict(zip(COLUMNS, fields))
        try:
            row["data_base"] = parse_data_base(row["data_base"])
            row["numero_de_operacoes"] = int(row["numero_de_operacoes"] or 0)
            for col in NUMERIC_COLUMNS[1:]:
                row[col] = parse_brl_number(row[col])
            for col in COLUMNS[:10]:
                row[col] = row[col].strip()
        except (ValueError, KeyError):
            return
        row["source_file"] = source_file
        yield row


def is_header(line: str) -> bool:
    stripped = line.strip().lstrip("\ufeff").lstrip('"')
    return stripped.lower().startswith("data_base") or stripped.lower().startswith("data-base")


def submit_dataflow_job(input_pattern: str, job_name: str):
    """Builds and submits the Beam pipeline to DataflowRunner. Returns
    immediately after submission — does not wait for the job to finish."""
    options = PipelineOptions(
        runner="DataflowRunner",
        project=PROJECT_ID,
        region=REGION,
        temp_location=TEMP_LOCATION,
        staging_location=f"gs://{BUCKET_NAME}/staging",
        job_name=job_name,
        num_workers=1,
        max_num_workers=2,
        machine_type="e2-standard-2",
    )

    p = beam.Pipeline(options=options)
    lines = (
        p
        | "MatchFiles" >> fileio.MatchFiles(input_pattern)
        | "ReadMatches" >> fileio.ReadMatches()
        | "ReadLines" >> beam.FlatMap(
            lambda f: [(line, f.metadata.path) for line in f.read_utf8().splitlines()]
        )
    )
    (
        lines
        | "FilterHeader" >> beam.Filter(lambda x: not is_header(x[0]))
        | "ParseRows" >> beam.FlatMap(lambda x: ParseScrDataRow().process(x[0], source_file=x[1]))
        | "WriteToBigQuery" >> WriteToBigQuery(
            STAGING_TABLE,
            schema=BQ_SCHEMA,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            method="FILE_LOADS",
            custom_gcs_temp_location=TEMP_LOCATION,
        )
    )
    result = p.run()
    # Deliberately NOT calling result.wait_until_finish() — submission is
    # what this function needs to do; the job runs async on Dataflow.
    print(f"Submitted Dataflow job '{job_name}', job ID: {result.job_id()}")
    return result.job_id()


@functions_framework.http
def run_monthly_ingestion(request):
    target_year, target_month = previous_month(date.today())
    target_filename = f"scrdata_{target_year}{target_month:02d}.csv"
    blob_path = f"raw/scr_data/year={target_year}/{target_filename}"

    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_path)

    if blob.exists():
        msg = f"{blob_path} already loaded, nothing to do."
        print(msg)
        return (msg, 200)

    url = BCB_URL_TEMPLATE.format(year=target_year)
    print(f"Downloading {url} to find {target_filename} ...")
    response = requests.get(url, timeout=300)
    response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if target_filename not in names:
            msg = (
                f"{target_filename} not found in {url} yet "
                f"(found: {names[-3:]} ...). Not published yet, exiting cleanly."
            )
            print(msg)
            return (msg, 200)
        with zf.open(target_filename) as f:
            data = f.read()

    blob.upload_from_string(data, content_type="text/csv")
    print(f"Uploaded {blob_path} ({len(data) / 1_000_000:.1f} MB)")

    input_pattern = f"gs://{BUCKET_NAME}/{blob_path}"
    job_name = f"scr-data-monthly-{target_year}{target_month:02d}"
    job_id = submit_dataflow_job(input_pattern, job_name)

    return (f"Loaded {blob_path} and submitted Dataflow job {job_name} ({job_id})", 200)
"""
Beam pipeline: SCR.data raw CSVs (GCS) -> staging table (BigQuery).
"""

import argparse
import csv
import logging
import re

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition

PROJECT_ID = "credito-scr"
STAGING_TABLE = f"{PROJECT_ID}:staging.scr_data"
TEMP_LOCATION = "gs://credito-scr-raw/tmp"

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


def parse_brl_number(raw: str) -> float:
    raw = raw.strip()
    if raw == "" or raw.upper() in ("NA", "NULL", "-"):
        return 0.0
    cleaned = raw.replace(".", "").replace(",", ".")
    return float(cleaned)


def parse_data_base(raw: str) -> str:
    raw = raw.strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", raw):
        return raw
    if len(raw) == 6 and raw.isdigit():
        year, month = raw[:4], raw[4:6]
        return f"{year}-{month}-01"
    if len(raw) == 8 and raw.isdigit():
        year, month, day = raw[:4], raw[4:6], raw[6:8]
        return f"{year}-{month}-{day}"
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
            logging.warning(
                "Skipping malformed row (%d fields, expected %d) in %s: %r",
                len(fields), len(COLUMNS), source_file, line[:200],
            )
            return

        row = dict(zip(COLUMNS, fields))
        try:
            row["data_base"] = parse_data_base(row["data_base"])
            row["numero_de_operacoes"] = int(row["numero_de_operacoes"] or 0)
            for col in NUMERIC_COLUMNS[1:]:
                row[col] = parse_brl_number(row[col])
            for col in COLUMNS[:10]:
                row[col] = row[col].strip()
        except (ValueError, KeyError) as e:
            logging.warning("Skipping row with parse error in %s: %s | %r", source_file, e, line[:200])
            return

        row["source_file"] = source_file
        yield row


def is_header(line: str) -> bool:
    stripped = line.strip().lstrip('\ufeff').lstrip('"')
    return stripped.lower().startswith("data_base") or stripped.lower().startswith("data-base")


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="GCS glob for input CSVs")
    parser.add_argument("--limit", type=int, default=None,
                         help="If set, only print the first N parsed rows and do not write to BigQuery")
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(
        pipeline_args,
        project=PROJECT_ID,
        temp_location=TEMP_LOCATION,
        region="southamerica-east1",
    )

    with beam.Pipeline(options=options) as p:
        lines = (
            p
            | "MatchFiles" >> fileio.MatchFiles(known_args.input)
            | "ReadMatches" >> fileio.ReadMatches()
            | "ReadLines" >> beam.FlatMap(
                lambda f: [(line, f.metadata.path) for line in f.read_utf8().splitlines()]
            )
        )

        parsed = (
            lines
            | "FilterHeader" >> beam.Filter(lambda x: not is_header(x[0]))
            | "ParseRows" >> beam.FlatMap(
                lambda x: ParseScrDataRow().process(x[0], source_file=x[1])
            )
        )

        if known_args.limit:
            (
                parsed
                | "Limit" >> beam.combiners.Sample.FixedSizeGlobally(known_args.limit)
                | "FlattenSample" >> beam.FlatMap(lambda rows: rows)
                | "PrintSample" >> beam.Map(print)
            )
        else:
            (
                parsed
                | "WriteToBigQuery" >> WriteToBigQuery(
                    STAGING_TABLE,
                    schema=BQ_SCHEMA,
                    write_disposition=BigQueryDisposition.WRITE_APPEND,
                    create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                    method="FILE_LOADS",
                    custom_gcs_temp_location=TEMP_LOCATION,
                )
            )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()

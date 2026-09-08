"""
Ingest SCR.data annual ZIPs from Banco Central into the GCS raw layer.

Source: https://www.bcb.gov.br/pda/desig/scrdata_{YEAR}.zip
Each ZIP contains one CSV per competencia (month), semicolon-delimited.

Usage:
    python ingest_raw.py

Requires:
    pip install google-cloud-storage requests
    gcloud auth application-default login   (or a service account key)
"""

import io
import os
import tempfile
import zipfile
import requests
from google.cloud import storage

PROJECT_ID = "credito-scr"
BUCKET_NAME = "credito-scr-raw"
YEARS = [2023, 2024, 2025, 2026]  # agent default — see decisions.md, change here if you want more history

BCB_URL_TEMPLATE = "https://www.bcb.gov.br/pda/desig/scrdata_{year}.zip"


def download_year_zip(year: int) -> bytes:
    url = BCB_URL_TEMPLATE.format(year=year)
    print(f"Downloading {url} ...")
    response = requests.get(url, timeout=300)
    response.raise_for_status()
    print(f"  {len(response.content) / 1_000_000:.1f} MB downloaded")
    return response.content


def upload_csvs_from_zip(zip_bytes: bytes, year: int, bucket: storage.Bucket) -> list[str]:
    uploaded = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csv_names = sorted(n for n in zf.namelist() if n.lower().endswith(".csv"))
        print(f"  Found {len(csv_names)} CSV files inside scrdata_{year}.zip")
        for name in csv_names:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
                with zf.open(name) as f:
                    while True:
                        chunk = f.read(8 * 1024 * 1024)
                        if not chunk:
                            break
                        tmp.write(chunk)
                tmp_path = tmp.name

            size_mb = os.path.getsize(tmp_path) / 1_000_000
            blob_path = f"raw/scr_data/year={year}/{name}"
            blob = bucket.blob(blob_path)

            if blob.exists():
                print(f"    Skipping {blob_path}, already in bucket")
                uploaded.append(blob_path)
                os.remove(tmp_path)
                continue

            blob.chunk_size = 8 * 1024 * 1024
            try:
                blob.upload_from_filename(tmp_path, content_type="text/csv", timeout=600)
                uploaded.append(blob_path)
                print(f"    Uploaded gs://{bucket.name}/{blob_path} ({size_mb:.1f} MB)")
            finally:
                os.remove(tmp_path)
    return uploaded


def main():
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)

    if not bucket.exists():
        raise SystemExit(
            f"Bucket gs://{BUCKET_NAME} does not exist. Create it first — see step-by-step guide, step 2."
        )

    all_uploaded = []
    for year in YEARS:
        zip_bytes = download_year_zip(year)
        uploaded = upload_csvs_from_zip(zip_bytes, year, bucket)
        all_uploaded.extend(uploaded)

    print(f"\nDone. {len(all_uploaded)} files uploaded to gs://{BUCKET_NAME}/raw/scr_data/")


if __name__ == "__main__":
    main()

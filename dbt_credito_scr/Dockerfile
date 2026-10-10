FROM python:3.12-slim

WORKDIR /dbt

RUN pip install --no-cache-dir dbt-bigquery==1.9.* dbt-core==1.12.5

COPY . /dbt

RUN dbt deps --profiles-dir /dbt

ENV DBT_PROFILES_DIR=/dbt

ENTRYPOINT ["sh", "-c", "dbt source freshness && dbt build"]

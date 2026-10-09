FROM python:3.12-slim

WORKDIR /dbt

RUN pip install --no-cache-dir dbt-bigquery==1.9.*

COPY . /dbt

RUN dbt deps --profiles-dir /dbt

ENV DBT_PROFILES_DIR=/dbt

ENTRYPOINT ["sh", "-c", "dbt source freshness --profiles-dir /dbt && dbt build --profiles-dir /dbt"]

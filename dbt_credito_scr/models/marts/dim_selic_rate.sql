-- Thin passthrough: translates the externally-loaded dim_taxa_selic
-- (populated by fetch_selic.py, outside dbt) into the marts layer's
-- English naming convention. No transformation, just naming consistency.

select
    data_base as reference_date,
    selic_meta_aa as selic_target_rate_annual
from {{ source('raw_selic', 'dim_taxa_selic') }}

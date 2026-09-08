select distinct
    reference_date,
    extract(year from reference_date) as year,
    extract(month from reference_date) as month,
    extract(quarter from reference_date) as quarter
from {{ ref('stg_scr_data') }}

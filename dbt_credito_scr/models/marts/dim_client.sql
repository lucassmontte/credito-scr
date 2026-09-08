select
    row_number() over (order by client_type, client_size) as client_id,
    client_type,
    client_size
from (
    select distinct client_type, client_size
    from {{ ref('stg_scr_data') }}
)

select distinct state
from {{ ref('stg_scr_data') }}
where state is not null and state != ''

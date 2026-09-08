select
    row_number() over (order by economic_activity) as economic_activity_id,
    economic_activity
from (
    select distinct economic_activity
    from {{ ref('stg_scr_data') }}
    where economic_activity is not null and economic_activity != ''
)

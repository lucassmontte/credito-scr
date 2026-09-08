select
    row_number() over (order by credit_modality, credit_submodality) as modality_id,
    credit_modality,
    credit_submodality
from (
    select distinct credit_modality, credit_submodality
    from {{ ref('stg_scr_data') }}
)

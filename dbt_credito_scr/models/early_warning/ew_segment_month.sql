-- Early warning, step 1: segment × month grain.
-- Segment = state × credit modality × client type × client size.
-- Economic activity (CNAE/occupation) is deliberately left out of the grain:
-- adding it multiplies segments by ~30 and leaves most of them too small
-- for a default rate that isn't dominated by noise. See decisions.md.

{{ config(materialized='table') }}

select
    date_trunc(f.reference_date, month)  as month_start,
    f.state,
    m.credit_modality,
    c.client_type,
    c.client_size,
    sum(f.active_portfolio)          as active_portfolio,
    sum(f.default_portfolio)         as default_portfolio,
    sum(f.overdue_15_to_90_balance)  as overdue_15_to_90_balance,
    sum(f.number_of_operations)      as number_of_operations

from {{ ref('fact_credit_operation') }} f
join {{ ref('dim_credit_modality') }} m on f.modality_id = m.modality_id
join {{ ref('dim_client') }} c          on f.client_id = c.client_id

group by 1, 2, 3, 4, 5

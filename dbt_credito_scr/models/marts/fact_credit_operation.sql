{{
    config(
        materialized='table',
        partition_by={'field': 'reference_date', 'data_type': 'date'},
        cluster_by=['state', 'modality_id']
    )
}}

select
    s.reference_date,
    s.state,
    m.modality_id,
    s.institution_segment,
    c.client_id,
    e.economic_activity_id,
    s.number_of_operations,
    s.outstanding_balance,
    s.overdue_balance,
    s.active_portfolio,
    s.default_portfolio,
    s.problem_assets,
    safe_divide(s.default_portfolio, nullif(s.active_portfolio, 0)) as default_rate

from {{ ref('stg_scr_data') }} s
left join {{ ref('dim_credit_modality') }} m
    on s.credit_modality = m.credit_modality and s.credit_submodality = m.credit_submodality
left join {{ ref('dim_client') }} c
    on s.client_type = c.client_type and s.client_size = c.client_size
left join {{ ref('dim_economic_activity') }} e
    on s.economic_activity = e.economic_activity

{{ config(materialized='table') }}

select
    d.reference_date,
    d.year,
    d.month,
    f.state,
    coalesce(nullif(e.economic_activity, 'Outros'), 'Other / unclassified activity') as sector,
    sel.selic_target_rate_annual,
    sum(f.active_portfolio) as active_portfolio_total,
    sum(f.default_portfolio) as default_portfolio_total,
    safe_divide(
        sum(f.default_portfolio),
        nullif(sum(f.active_portfolio), 0)
    ) as default_rate,
    sum(f.number_of_operations) as number_of_operations

from {{ ref('fact_credit_operation') }} f
join {{ ref('dim_date') }} d
    on f.reference_date = d.reference_date
join {{ ref('dim_economic_activity') }} e
    on f.economic_activity_id = e.economic_activity_id
left join {{ ref('dim_selic_rate') }} sel
    on date_trunc(f.reference_date, month) = date_trunc(sel.reference_date, month)

group by
    f.state, sector, d.reference_date, d.year, d.month, sel.selic_target_rate_annual
order by
    f.state, sector, d.reference_date

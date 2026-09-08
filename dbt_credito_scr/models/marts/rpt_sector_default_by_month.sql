-- Reporting mart, purpose-built for the Looker Studio dashboard.
-- Pre-joins and pre-aggregates fact + dimensions + Selic rate so the BI
-- tool only needs to connect to ONE table — no manual joins inside Looker
-- Studio, which handles multi-table joins poorly on the free tier.

{{ config(materialized='table') }}

select
    d.reference_date,
    d.year,
    d.month,
    e.economic_activity as sector,
    sel.selic_target_rate_annual,
    sum(f.active_portfolio) as active_portfolio_total,
    sum(f.default_portfolio) as default_portfolio_total,
    safe_divide(sum(f.default_portfolio), nullif(sum(f.active_portfolio), 0)) as default_rate,
    sum(f.number_of_operations) as number_of_operations

from {{ ref('fact_credit_operation') }} f
join {{ ref('dim_date') }} d on f.reference_date = d.reference_date
join {{ ref('dim_economic_activity') }} e on f.economic_activity_id = e.economic_activity_id
left join {{ ref('dim_selic_rate') }} sel on f.reference_date = sel.reference_date

group by sector, d.reference_date, d.year, d.month, sel.selic_target_rate_annual
order by sector, d.reference_date

-- Filtered companion to rpt_sector_default_by_month: only the top 8
-- sectors by total credit exposure across the whole period, so the
-- Looker Studio time series chart shows a readable sample instead of 30
-- overlapping lines. "Top" is ranked by active_portfolio_total (market
-- size), not by default_rate — this keeps the sample statistically
-- meaningful (small sectors have noisy, volatile default rates) while
-- still showing default_rate as the metric of interest.

{{ config(materialized='table') }}

with sector_totals as (
    select
        sector,
        sum(active_portfolio_total) as total_exposure
    from {{ ref('rpt_sector_default_by_month') }}
    group by sector
),

top_sectors as (
    select sector
    from sector_totals
    order by total_exposure desc
    limit 8
)

select r.*
from {{ ref('rpt_sector_default_by_month') }} r
join top_sectors t using (sector)
order by r.sector, r.reference_date

-- Early warning, step 2: feature table + target, one row per segment × month.
--
-- Question the model answers: "given what this segment looks like at month t,
-- will its default rate deteriorate materially by month t + H?"
-- This is a portfolio-level behavior model (segment monitoring), NOT a
-- borrower-level score: SCR.data is aggregated and has no individual borrower.
--
-- Leakage rules:
--   * every feature uses only months <= t (current month and backward lags)
--   * the target uses only month t + H
--   * rows whose t + H is not published yet have target = NULL
--     (they are the population scored for the current month)

{{ config(materialized='table') }}

{% set horizon  = var('ew_horizon_months', 6) %}
{% set min_pp   = var('ew_target_min_pp', 0.005) %}      -- absolute rise >= 0.5 p.p.
{% set min_rel  = var('ew_target_min_relative', 0.15) %} -- and relative rise >= 15%
{% set min_port = var('ew_min_portfolio_brl', 10000000) %} -- segments >= R$ 10 mi

with base as (

    select
        concat(
            coalesce(state, 'NA'), ' | ', coalesce(credit_modality, 'NA'), ' | ',
            coalesce(client_type, 'NA'), ' | ', coalesce(client_size, 'NA')
        ) as segment_id,
        *,
        safe_divide(default_portfolio, active_portfolio)        as default_rate,
        safe_divide(overdue_15_to_90_balance, active_portfolio) as early_delinquency_rate
    from {{ ref('ew_segment_month') }}
    where active_portfolio > 0

),

selic as (

    select
        date_trunc(reference_date, month) as month_start,
        avg(selic_target_rate_annual)     as selic_rate
    from {{ ref('dim_selic_rate') }}
    group by 1

),

features as (

    select
        b.segment_id,
        b.month_start,
        b.state,
        b.credit_modality,
        b.client_type,
        b.client_size,
        b.active_portfolio,
        b.default_rate,
        b.early_delinquency_rate,
        b.default_rate - l3.default_rate                     as default_rate_chg_3m,
        b.default_rate - l6.default_rate                     as default_rate_chg_6m,
        b.early_delinquency_rate - l3.early_delinquency_rate as early_delinquency_chg_3m,
        safe_divide(b.active_portfolio, l6.active_portfolio) - 1 as portfolio_growth_6m,
        safe_divide(b.active_portfolio, b.number_of_operations)  as avg_balance_per_operation,
        log10(b.active_portfolio)                            as log_active_portfolio,
        s.selic_rate,
        s.selic_rate - s6.selic_rate                         as selic_chg_6m,
        fwd.default_rate                                     as default_rate_forward

    from base b
    left join base l3
        on l3.segment_id = b.segment_id
       and l3.month_start = date_sub(b.month_start, interval 3 month)
    left join base l6
        on l6.segment_id = b.segment_id
       and l6.month_start = date_sub(b.month_start, interval 6 month)
    left join base fwd
        on fwd.segment_id = b.segment_id
       and fwd.month_start = date_add(b.month_start, interval {{ horizon }} month)
    left join selic s
        on s.month_start = b.month_start
    left join selic s6
        on s6.month_start = date_sub(b.month_start, interval 6 month)

)

select
    segment_id,
    month_start as reference_month,
    state,
    credit_modality,
    client_type,
    client_size,
    active_portfolio,
    default_rate,
    early_delinquency_rate,
    default_rate_chg_3m,
    default_rate_chg_6m,
    early_delinquency_chg_3m,
    portfolio_growth_6m,
    avg_balance_per_operation,
    log_active_portfolio,
    selic_rate,
    selic_chg_6m,
    default_rate_forward,
    case
        when default_rate_forward is null then null
        when default_rate_forward - default_rate >= {{ min_pp }}
         and default_rate_forward >= default_rate * (1 + {{ min_rel }}) then 1
        else 0
    end as target_deterioration

from features
where active_portfolio >= {{ min_port }}
  and default_rate_chg_6m is not null   -- needs 6 months of history

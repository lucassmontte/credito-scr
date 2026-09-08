-- Staging: translates the source's Portuguese column names to English.
-- This is the single translation seam in the pipeline — bronze
-- (staging.scr_data) stays faithful to the Bacen SCR.data source schema,
-- everything downstream of this model inherits English naming through
-- dbt's ref() graph. See decisions.md, "English localization decision".
--
-- Types were already parsed and cast by the Beam pipeline (Day 2) — this
-- model does not repeat that cleaning logic, only renames.

select
    data_base            as reference_date,
    uf                    as state,
    segmento              as institution_segment,
    cliente                as client_type,          -- PF / PJ
    cnae_ocupacao          as economic_activity,
    porte                as client_size,
    modalidade              as credit_modality,
    submodalidade            as credit_submodality,
    numero_de_operacoes        as number_of_operations,
    carteira_a_vencer        as outstanding_balance,
    carteira_vencida          as overdue_balance,
    carteira_ativa            as active_portfolio,
    carteira_inadimplencia      as default_portfolio,
    ativo_problematico          as problem_assets

from {{ source('raw_scr', 'scr_data') }}
where data_base is not null

-- Views behind the Looker Studio dashboard.
-- Created in the BigQuery console (not managed by dbt yet; see decisions.md).

-- 1. English labels for the scored segments (page 1: cards, map, charts, watchlist)
CREATE OR REPLACE VIEW `credito-scr.marts.ew_segment_scores_en` AS
SELECT
  reference_month,
  segment_id,
  state,
  CASE
    WHEN credit_modality LIKE 'Financiamentos rurais%'                          THEN 'Rural and agribusiness financing'
    WHEN credit_modality = 'Outros créditos'                                    THEN 'Other credit'
    WHEN credit_modality = 'Empréstimos'                                        THEN 'Loans'
    WHEN credit_modality = 'Financiamentos'                                     THEN 'Financing'
    WHEN credit_modality = 'Financiamentos imobiliários'                        THEN 'Real estate financing'
    WHEN credit_modality = 'Direitos creditórios descontados'                   THEN 'Discounted receivables'
    WHEN credit_modality = 'Adiantamentos a depositantes'                       THEN 'Advances to depositors'
    WHEN credit_modality LIKE 'Financiamentos à exporta%'                       THEN 'Export financing'
    WHEN credit_modality LIKE 'Financiamentos à importa%'                       THEN 'Import financing'
    WHEN credit_modality = 'Financiamentos com interveniência'                  THEN 'Financing with intervening party'
    WHEN credit_modality = 'Financiamentos de infraestrutura e desenvolvimento' THEN 'Infrastructure and development financing'
    WHEN credit_modality = 'Financiamentos de títulos e valores mobiliários'    THEN 'Securities financing'
    WHEN credit_modality = 'Operações de arrendamento'                          THEN 'Leasing'
    WHEN credit_modality = 'Outros'                                             THEN 'Others'
    ELSE credit_modality
  END AS credit_modality,
  CASE client_type WHEN 'PF' THEN 'Individuals' WHEN 'PJ' THEN 'Businesses' ELSE client_type END AS client_type,
  CASE
    WHEN client_size = 'Sem rendimento'       THEN 'No income'
    WHEN client_size = 'Indisponível'         THEN 'Not available'
    WHEN client_size = 'Até 1 salário mínimo' THEN 'Up to 1 min. wage'
    WHEN client_size LIKE 'Mais de 1 a 2%'    THEN '1–2 min. wages'
    WHEN client_size LIKE 'Mais de 2 a 3%'    THEN '2–3 min. wages'
    WHEN client_size LIKE 'Mais de 3 a 5%'    THEN '3–5 min. wages'
    WHEN client_size LIKE 'Mais de 5 a 10%'   THEN '5–10 min. wages'
    WHEN client_size LIKE 'Mais de 10 a 20%'  THEN '10–20 min. wages'
    WHEN client_size LIKE 'Acima de 20%'      THEN 'Over 20 min. wages'
    WHEN client_size = 'Micro'                THEN 'Micro'
    WHEN client_size = 'Pequeno'              THEN 'Small'
    WHEN client_size = 'Médio'                THEN 'Medium'
    WHEN client_size = 'Grande'               THEN 'Large'
    ELSE client_size
  END AS client_size,
  active_portfolio,
  default_rate,
  early_delinquency_rate,
  score,
  CASE risk_band WHEN 'Alto' THEN 'High' WHEN 'Médio' THEN 'Medium' WHEN 'Baixo' THEN 'Low' ELSE risk_band END AS risk_band,
  model_version,
  scored_at
FROM `credito-scr.marts.ew_segment_scores`;

-- 2. Delinquency history of each current risk band (page 1: trend chart)
-- Rates are weighted by active portfolio. The 6 empty months after the last real month
-- (is_forecast_window = TRUE) are filtered out in the chart; kept here for a future forecast view.
CREATE OR REPLACE VIEW `credito-scr.marts.ew_flagged_trend` AS
WITH hist AS (
  SELECT
    f.reference_month,
    s.risk_band,
    CAST(SAFE_DIVIDE(SUM(f.default_rate * f.active_portfolio), SUM(f.active_portfolio)) AS FLOAT64)           AS default_rate,
    CAST(SAFE_DIVIDE(SUM(f.early_delinquency_rate * f.active_portfolio), SUM(f.active_portfolio)) AS FLOAT64) AS early_delinquency_rate,
    CAST(SUM(f.active_portfolio) AS FLOAT64) AS active_portfolio,
    FALSE AS is_forecast_window
  FROM `credito-scr.marts.ew_features` f
  JOIN `credito-scr.marts.ew_segment_scores_en` s USING (segment_id)
  GROUP BY 1, 2
),
future AS (
  SELECT
    m AS reference_month,
    b AS risk_band,
    CAST(NULL AS FLOAT64) AS default_rate,
    CAST(NULL AS FLOAT64) AS early_delinquency_rate,
    CAST(NULL AS FLOAT64) AS active_portfolio,
    TRUE AS is_forecast_window
  FROM (SELECT MAX(reference_month) AS mx FROM hist),
       UNNEST(GENERATE_DATE_ARRAY(DATE_ADD(mx, INTERVAL 1 MONTH), DATE_ADD(mx, INTERVAL 6 MONTH), INTERVAL 1 MONTH)) AS m,
       UNNEST(['High', 'Medium', 'Low']) AS b
)
SELECT * FROM hist
UNION ALL
SELECT * FROM future;

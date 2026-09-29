# Early warning — resultados (2026-09-29)

Horizonte: 6 meses | Treino: até 2025-02 | Teste OOT: 2025-08 a 2026-01

Taxa de deterioração — treino 21.7%, teste 28.8%

## Métricas out-of-time

|                                |   ROC AUC |   Gini |     KS |   PR-AUC |
|:-------------------------------|----------:|-------:|-------:|---------:|
| Regressão logística (baseline) |    0.5615 | 0.123  | 0.0861 |   0.3535 |
| Gradient boosting              |    0.7216 | 0.4432 | 0.3301 |   0.4853 |

## Decis — Gradient boosting

|   decil |   segmentos |   deterioraram |   taxa |   captura_acumulada |   lift |
|--------:|------------:|---------------:|-------:|--------------------:|-------:|
|       1 |        1365 |            757 |  0.555 |               0.193 |  1.926 |
|       2 |        1364 |            611 |  0.448 |               0.348 |  1.555 |
|       3 |        1364 |            513 |  0.376 |               0.479 |  1.306 |
|       4 |        1364 |            492 |  0.361 |               0.604 |  1.253 |
|       5 |        1365 |            497 |  0.364 |               0.73  |  1.264 |
|       6 |        1364 |            406 |  0.298 |               0.834 |  1.034 |
|       7 |        1364 |            317 |  0.232 |               0.914 |  0.807 |
|       8 |        1364 |            195 |  0.143 |               0.964 |  0.496 |
|       9 |        1364 |            109 |  0.08  |               0.992 |  0.277 |
|      10 |        1365 |             32 |  0.023 |               1     |  0.081 |

## Importância por permutação

|                           |   queda_de_AUC |
|:--------------------------|---------------:|
| early_delinquency_rate    |         0.1698 |
| default_rate              |         0.0442 |
| avg_balance_per_operation |         0.0245 |
| early_delinquency_chg_3m  |         0.0122 |
| log_active_portfolio      |         0.0108 |
| default_rate_chg_3m       |         0.0044 |
| portfolio_growth_6m       |         0.0041 |
| default_rate_chg_6m       |         0.0014 |
| selic_rate                |         0      |
| selic_chg_6m              |        -0.0098 |

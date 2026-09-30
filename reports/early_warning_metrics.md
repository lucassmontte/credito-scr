# Early warning — resultados (2026-09-29)

Horizonte: 6 meses | Treino: até 2025-02 | Teste OOT: 2025-08 a 2026-01

Taxa de deterioração — treino 21.7%, teste 28.8%

## Métricas out-of-time

|                                |   ROC AUC |   Gini |     KS |   PR-AUC |
|:-------------------------------|----------:|-------:|-------:|---------:|
| Regressão logística (baseline) |    0.5615 |  0.123 | 0.0861 |   0.3535 |
| Gradient boosting              |    0.7195 |  0.439 | 0.3276 |   0.484  |

## Decis — Gradient boosting

|   decil |   segmentos |   deterioraram |   taxa |   captura_acumulada |   lift |
|--------:|------------:|---------------:|-------:|--------------------:|-------:|
|       1 |        1365 |            740 |  0.542 |               0.188 |  1.882 |
|       2 |        1364 |            623 |  0.457 |               0.347 |  1.586 |
|       3 |        1364 |            488 |  0.358 |               0.471 |  1.242 |
|       4 |        1364 |            509 |  0.373 |               0.601 |  1.296 |
|       5 |        1365 |            499 |  0.366 |               0.728 |  1.269 |
|       6 |        1364 |            406 |  0.298 |               0.831 |  1.034 |
|       7 |        1364 |            327 |  0.24  |               0.914 |  0.832 |
|       8 |        1364 |            196 |  0.144 |               0.964 |  0.499 |
|       9 |        1364 |            117 |  0.086 |               0.994 |  0.298 |
|      10 |        1365 |             24 |  0.018 |               1     |  0.061 |

## Importância por permutação

|                           |   queda_de_AUC |
|:--------------------------|---------------:|
| early_delinquency_rate    |         0.1732 |
| default_rate              |         0.0413 |
| avg_balance_per_operation |         0.0271 |
| early_delinquency_chg_3m  |         0.0113 |
| log_active_portfolio      |         0.0105 |
| portfolio_growth_6m       |         0.0042 |
| default_rate_chg_3m       |         0.0031 |
| default_rate_chg_6m       |         0.0016 |
| selic_rate                |         0      |
| selic_chg_6m              |        -0.0103 |

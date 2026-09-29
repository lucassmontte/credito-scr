"""
Early warning de inadimplência por segmento — treino, validação e scoring.

Lê `marts.ew_features` (dbt), treina dois modelos, valida out-of-time,
escreve os scores do mês mais recente em `marts.ew_segment_scores` e gera
`reports/early_warning_metrics.md`.

Uso:
    python scripts/train_early_warning.py                 # BigQuery
    python scripts/train_early_warning.py --csv amostra.csv --no-write   # teste local

Decisões (detalhes em decisions.md):
  * validação out-of-time: teste = últimos N meses rotulados; treino = só meses
    cujo alvo (t + H) termina ANTES do primeiro mês de teste, sem sobreposição
  * baseline de regressão logística antes do gradient boosting
  * métricas de crédito: KS e Gini, além de ROC AUC e PR-AUC
  * o modelo final é retreinado com todos os meses rotulados antes do scoring
"""
import argparse
import datetime as dt
import os

import numpy as np
import pandas as pd
from scipy.stats import ks_2samp
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.inspection import permutation_importance
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

PROJECT = "credito-scr"
FEATURES_TABLE = f"{PROJECT}.marts.ew_features"
SCORES_TABLE = f"{PROJECT}.marts.ew_segment_scores"
TARGET = "target_deterioration"
FEATURES = [
    "default_rate",
    "early_delinquency_rate",
    "default_rate_chg_3m",
    "default_rate_chg_6m",
    "early_delinquency_chg_3m",
    "portfolio_growth_6m",
    "avg_balance_per_operation",
    "log_active_portfolio",
    "selic_rate",
    "selic_chg_6m",
]
SEGMENT_COLS = ["segment_id", "state", "credit_modality", "client_type", "client_size"]
MODEL_VERSION = "ew-v1"


def load(csv_path):
    if csv_path:
        df = pd.read_csv(csv_path)
    else:
        from google.cloud import bigquery
        df = bigquery.Client(project=PROJECT).query(f"SELECT * FROM `{FEATURES_TABLE}`").to_dataframe()
    df["reference_month"] = pd.to_datetime(df["reference_month"])
    return df


def split_oot(labeled, horizon, n_test):
    months = sorted(labeled["reference_month"].unique())
    test_start = months[-n_test]
    limit = test_start - pd.DateOffset(months=horizon)
    train = labeled[labeled["reference_month"] <= limit]
    test = labeled[labeled["reference_month"] >= test_start]
    return train, test, limit, test_start


def build_models():
    return {
        "Regressão logística (baseline)": make_pipeline(
            SimpleImputer(strategy="median"),
            StandardScaler(),
            LogisticRegression(class_weight="balanced", max_iter=2000),
        ),
        "Gradient boosting": HistGradientBoostingClassifier(
            max_depth=4, learning_rate=0.05, max_iter=300,
            min_samples_leaf=50, class_weight="balanced", random_state=42,
        ),
    }


def metrics(y, p):
    auc = roc_auc_score(y, p)
    return {
        "ROC AUC": auc,
        "Gini": 2 * auc - 1,
        "KS": ks_2samp(p[y == 1], p[y == 0]).statistic,
        "PR-AUC": average_precision_score(y, p),
    }


def decile_table(y, p):
    t = pd.DataFrame({"y": y, "p": p})
    t["decil"] = pd.qcut(t["p"].rank(method="first", ascending=False), 10, labels=range(1, 11))
    g = t.groupby("decil", observed=True).agg(segmentos=("y", "size"), deterioraram=("y", "sum"))
    g["taxa"] = g["deterioraram"] / g["segmentos"]
    g["captura_acumulada"] = g["deterioraram"].cumsum() / g["deterioraram"].sum()
    g["lift"] = g["taxa"] / y.mean()
    return g


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", help="lê features de CSV em vez do BigQuery")
    ap.add_argument("--horizon", type=int, default=6, help="deve bater com a var ew_horizon_months do dbt")
    ap.add_argument("--n-test-months", type=int, default=6)
    ap.add_argument("--no-write", action="store_true", help="não grava scores no BigQuery")
    args = ap.parse_args()

    df = load(args.csv)
    labeled = df[df[TARGET].notna()].copy()
    labeled[TARGET] = labeled[TARGET].astype(int)

    train, test, limit, test_start = split_oot(labeled, args.horizon, args.n_test_months)
    print(f"Linhas totais: {len(df):,} | rotuladas: {len(labeled):,} | segmentos: {df['segment_id'].nunique():,}")
    print(f"Treino: {train['reference_month'].min():%Y-%m} a {limit:%Y-%m} "
          f"({len(train):,} linhas, taxa de deterioração {train[TARGET].mean():.1%})")
    print(f"Teste OOT: {test_start:%Y-%m} a {test['reference_month'].max():%Y-%m} "
          f"({len(test):,} linhas, taxa de deterioração {test[TARGET].mean():.1%})")

    results, probs, fitted = {}, {}, {}
    for name, model in build_models().items():
        model.fit(train[FEATURES], train[TARGET])
        p = model.predict_proba(test[FEATURES])[:, 1]
        results[name], probs[name], fitted[name] = metrics(test[TARGET].values, p), p, model

    res = pd.DataFrame(results).T
    print("\n== Métricas out-of-time ==\n" + res.round(4).to_string())

    best = res["KS"].idxmax()
    deciles = decile_table(test[TARGET].values, probs[best])
    print(f"\n== Decis — {best} ==\n" + deciles.round(3).to_string())

    imp = permutation_importance(fitted[best], test[FEATURES], test[TARGET],
                                 scoring="roc_auc", n_repeats=5, random_state=42)
    importance = (pd.Series(imp.importances_mean, index=FEATURES)
                  .sort_values(ascending=False).rename("queda_de_AUC"))
    print("\n== Importância por permutação (queda de AUC no teste) ==\n" + importance.round(4).to_string())

    # Modelo final: retreina com todos os meses rotulados e pontua o mês mais recente
    final = build_models()[best].fit(labeled[FEATURES], labeled[TARGET])
    latest = df["reference_month"].max()
    to_score = df[df["reference_month"] == latest].copy()
    to_score["score"] = final.predict_proba(to_score[FEATURES])[:, 1]
    cuts = np.quantile(probs[best], [0.7, 0.9])  # faixas calibradas no teste OOT
    to_score["risk_band"] = np.select(
        [to_score["score"] >= cuts[1], to_score["score"] >= cuts[0]], ["Alto", "Médio"], "Baixo")
    to_score["model_version"] = MODEL_VERSION
    to_score["scored_at"] = pd.Timestamp.now(tz="UTC")
    out = to_score[["reference_month", *SEGMENT_COLS, "active_portfolio", "default_rate",
                    "early_delinquency_rate", "score", "risk_band", "model_version", "scored_at"]]
    out = out.sort_values("score", ascending=False)
    print(f"\nScoring de {latest:%Y-%m}: {len(out):,} segmentos | "
          f"Alto: {(out['risk_band'] == 'Alto').sum()} | Médio: {(out['risk_band'] == 'Médio').sum()}")
    print(out.head(10)[["segment_id", "default_rate", "early_delinquency_rate", "score", "risk_band"]]
          .to_string(index=False))

    if not args.no_write:
        from google.cloud import bigquery
        client = bigquery.Client(project=PROJECT)
        job = client.load_table_from_dataframe(
            out, SCORES_TABLE,
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"))
        job.result()
        print(f"Scores gravados em {SCORES_TABLE}")

    os.makedirs("reports", exist_ok=True)
    with open("reports/early_warning_metrics.md", "w") as f:
        f.write(f"# Early warning — resultados ({dt.date.today():%Y-%m-%d})\n\n")
        f.write(f"Horizonte: {args.horizon} meses | Treino: até {limit:%Y-%m} | "
                f"Teste OOT: {test_start:%Y-%m} a {test['reference_month'].max():%Y-%m}\n\n")
        f.write(f"Taxa de deterioração — treino {train[TARGET].mean():.1%}, teste {test[TARGET].mean():.1%}\n\n")
        f.write("## Métricas out-of-time\n\n" + res.round(4).to_markdown() + "\n\n")
        f.write(f"## Decis — {best}\n\n" + deciles.round(3).to_markdown() + "\n\n")
        f.write("## Importância por permutação\n\n" + importance.round(4).to_frame().to_markdown() + "\n")
    print("Relatório: reports/early_warning_metrics.md")


if __name__ == "__main__":
    main()

"""Сравнение новой модели с текущей прод-моделью на свежем месяце."""

from __future__ import annotations

import logging

import pandas as pd

from pipeline.registry import candidate_paths, meta_path, model_path, read_json, read_latest_version, write_json
from pipeline.settings import LABEL

logger = logging.getLogger(__name__)

# на сколько challenger может уступить по PR-AUC и все равно пройти (0 = строго не хуже)
TOLERANCE = 0.0


def score_model(path, threshold: float, test: pd.DataFrame) -> dict:
    """Считает PR-AUC, ROC-AUC и F1 (при пороге модели) на test."""
    from catboost import CatBoostClassifier
    from sklearn.metrics import average_precision_score, f1_score, roc_auc_score

    model = CatBoostClassifier()
    model.load_model(str(path))
    x = test[list(model.feature_names_)].copy()
    y = test[LABEL].astype(int)
    for idx in model.get_cat_feature_indices():
        column = x.columns[idx]
        x[column] = x[column].astype(str)
    proba = model.predict_proba(x)[:, 1]
    metrics = {"test_rows": int(len(y)), "test_frauds": int(y.sum()),
               "f1": float(f1_score(y, proba > threshold, zero_division=0))}
    if y.nunique() == 2:
        metrics["pr_auc"] = float(average_precision_score(y, proba))
        metrics["roc_auc"] = float(roc_auc_score(y, proba))
    return metrics


def evaluate_against_champion(train_info: dict, params: dict | None = None) -> dict:
    """Решает, публиковать ли challenger: сравнивает его с моделью из latest.txt на test-месяце.

    Обе модели скорят один и тот же test-месяц (на нем ни одна из них не училась —
    у прод-модели это месяц «из будущего»). Основная метрика — PR-AUC
    (average precision): классы сильно несбалансированы, и ROC-AUC слабо различает модели.
    Публикуем, если PR-AUC challenger >= PR-AUC champion - TOLERANCE, либо если
    прод-модели еще нет, либо DAG запущен с conf {"force_publish": true}.
    Если в test-месяце нет фрода, PR-AUC не определен и модель не публикуется.

    Читает: test_features.parquet, кандидата и прод-модель из каталога моделей.
    Пишет: candidates/model_<version>.decision.json. Ничего не публикует,
    поэтому повтор задачи безопасен (решение перезаписывается тем же результатом).
    """
    test = pd.read_parquet(train_info["test_features_path"])
    version = train_info["version"]
    cand_cbm, cand_json = candidate_paths(version)
    challenger = score_model(cand_cbm, read_json(cand_json)["threshold"], test)

    champion_version = read_latest_version()
    champion = None
    if champion_version and model_path(champion_version).exists():
        champion_meta = read_json(meta_path(champion_version))
        champion = score_model(model_path(champion_version), champion_meta["threshold"], test)

    force = bool((params or {}).get("force_publish", False))
    if champion is None:
        publish, reason = True, "прод-модели еще нет"
    elif "pr_auc" not in challenger:
        publish, reason = False, "в test-месяце нет фрода, сравнить модели нельзя"
    elif challenger["pr_auc"] >= champion["pr_auc"] - TOLERANCE:
        publish, reason = True, "PR-AUC challenger не ниже, чем у прод-модели"
    else:
        publish, reason = False, "PR-AUC challenger ниже, чем у прод-модели"
    if force and not publish:
        publish, reason = True, f"force_publish=true ({reason})"

    decision = {
        "version": version,
        "test_month": train_info["test_month"],
        "champion_version": champion_version,
        "challenger": challenger,
        "champion": champion,
        "publish": publish,
        "reason": reason,
    }
    write_json(cand_json.with_name(f"model_{version}.decision.json"), decision)
    logger.info("Решение: publish=%s (%s). challenger=%s, champion %s=%s",
                publish, reason, challenger, champion_version, champion)
    return {**train_info, "publish": publish, "reason": reason, "test_metrics": challenger}

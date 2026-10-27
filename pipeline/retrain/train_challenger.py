"""Обучение новой модели (challenger) на train-части окна."""

import logging
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from pipeline.registry import candidate_paths, write_json
from pipeline.settings import CANDIDATES_DIR, LABEL

logger = logging.getLogger(__name__)

VAL_SHARE = 0.2


def fit_model(train: pd.DataFrame):
    """Учит CatBoost и подбирает порог по F1.

    train должен быть отсортирован по времени: последние VAL_SHARE строк идут
    в валидацию для подбора порога, чтобы test-месяц не участвовал в обучении
    и честно использовался только для сравнения с прод-моделью.
    Ранней остановки нет: фрода в валидации единицы, и она срабатывала бы на шуме.
    Возвращает (model, threshold, val_metrics).
    """
    from catboost import CatBoostClassifier
    from sklearn.metrics import average_precision_score, f1_score

    x, y = train.drop(columns=[LABEL]), train[LABEL].astype(int)
    cat_features = [c for c in x.columns if x[c].dtype == object]
    cut = int(len(x) * (1 - VAL_SHARE))
    x_tr, y_tr, x_val, y_val = x.iloc[:cut], y.iloc[:cut], x.iloc[cut:], y.iloc[cut:]

    model = CatBoostClassifier(
        iterations=300,
        depth=5,
        learning_rate=0.05,
        l2_leaf_reg=5,
        auto_class_weights="Balanced",
        random_seed=42,
        allow_writing_files=False,
        thread_count=2,
        verbose=100,
    )
    model.fit(x_tr, y_tr, cat_features=cat_features)

    threshold, val_metrics = 0.5, {"val_rows": int(len(y_val)), "val_frauds": int(y_val.sum())}
    if y_val.sum() > 0:
        proba = model.predict_proba(x_val)[:, 1]
        grid = np.arange(0.05, 0.96, 0.01)
        f1s = [f1_score(y_val, proba > t, zero_division=0) for t in grid]
        threshold = round(float(grid[int(np.argmax(f1s))]), 2)
        val_metrics.update(val_pr_auc=float(average_precision_score(y_val, proba)), val_f1=float(max(f1s)))
    return model, threshold, val_metrics


def train_challenger(features_info: dict) -> dict:
    """Обучает challenger и сохраняет его в candidates/ (в прод он еще не публикуется).

    Читает: work/<YYYYMM>/train_features.parquet.
    Пишет: candidates/model_<version>.cbm и model_<version>.json (порог, метрики валидации).
    Версия = <месяц батча>_<UTC время>, повтор задачи создаст нового кандидата
    с другой версией, но не трогает ни latest.txt, ни окно обучения.
    """
    train = pd.read_parquet(features_info["train_features_path"])
    model, threshold, val_metrics = fit_model(train)

    month = features_info["batch_month"][:7].replace("-", "")
    version = f"{month}_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"
    CANDIDATES_DIR.mkdir(parents=True, exist_ok=True)
    cbm_path, json_path = candidate_paths(version)
    model.save_model(str(cbm_path))
    meta = {
        "version": version,
        "threshold": threshold,
        "train_months": features_info["train_months"],
        "test_month": features_info["test_month"],
        "train_rows": int(len(train)),
        **val_metrics,
    }
    write_json(json_path, meta)
    logger.info("Кандидат %s сохранен, порог %.2f, валидация %s", version, threshold, val_metrics)
    return {**features_info, "version": version, "candidate_path": str(cbm_path)}

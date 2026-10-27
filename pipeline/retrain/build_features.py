"""Построение признаков той же функцией run_preproc, что и в онлайн-скоринге."""

import logging
import sys

import pandas as pd

from pipeline.settings import FRAUD_SRC, LABEL

logger = logging.getLogger(__name__)


def _run_preproc():
    if FRAUD_SRC not in sys.path:
        sys.path.insert(0, FRAUD_SRC)
    from preprocessing import run_preproc
    return run_preproc


def build_features(split_info: dict) -> dict:
    """Применяет run_preproc из fraud_detector/src к train и test, сохраняя порядок по времени.

    Читает: work/<YYYYMM>/train_raw.parquet, test_raw.parquet.
    Пишет: work/<YYYYMM>/train_features.parquet, test_features.parquet (признаки + target).
    Используется тот же код, что и в сервисе скоринга, поэтому признаки на обучении
    и в проде совпадают. Повтор безопасен: файлы перезаписываются тем же результатом.
    """
    run_preproc = _run_preproc()
    result = dict(split_info)
    for part in ("train", "test"):
        raw = pd.read_parquet(split_info[f"{part}_raw_path"])
        features = run_preproc(raw)
        features[LABEL] = raw[LABEL].to_numpy()
        path = f"{split_info['work_dir']}/{part}_features.parquet"
        features.to_parquet(path, index=False)
        result[f"{part}_features_path"] = path
        logger.info("%s: признаки %s", part, features.shape)
    return result

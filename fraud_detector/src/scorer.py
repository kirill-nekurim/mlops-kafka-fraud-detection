import json
import logging
import os
import shutil
from pathlib import Path

import pandas as pd
from catboost import CatBoostClassifier
from metrics import ACTIVE, LAST_RELOAD, LOAD_ERRORS, RELOADS

logger = logging.getLogger(__name__)

# общий с Airflow каталог моделей: model_<version>.cbm + model_<version>.json, текущая версия в latest.txt
MODELS_DIR = Path(os.getenv('MODELS_DIR', '/models'))
LATEST = MODELS_DIR / 'latest.txt'
# стартовая модель из образа, пока DAG ничего не опубликовал
BUNDLED_DIR = Path(os.getenv('BUNDLED_MODELS_DIR', './models'))
INITIAL_VERSION = 'initial'

_model = None
_version = None
_threshold = None


def ensure_bootstrap():
    """Если в каталоге моделей еще нет latest.txt, кладем туда модель из образа как версию initial"""
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    if LATEST.exists() and LATEST.read_text().strip():
        return
    shutil.copyfile(BUNDLED_DIR / 'model.cbm', MODELS_DIR / f'model_{INITIAL_VERSION}.cbm')
    shutil.copyfile(BUNDLED_DIR / 'model.json', MODELS_DIR / f'model_{INITIAL_VERSION}.json')
    LATEST.write_text(INITIAL_VERSION + '\n')
    logger.info('Bootstrap: в %s положена стартовая модель %s', MODELS_DIR, INITIAL_VERSION)


def _current_model():
    """Перечитывает latest.txt и подгружает модель, если версия поменялась (без перезапуска контейнера)"""
    global _model, _version, _threshold
    ensure_bootstrap()
    version = LATEST.read_text().strip()
    if version != _version:
        model = CatBoostClassifier()
        model.load_model(str(MODELS_DIR / f'model_{version}.cbm'))
        with open(MODELS_DIR / f'model_{version}.json') as f:
            threshold = json.load(f)['threshold']
        if _version is not None:
            ACTIVE.labels(_version).set(0)
        _model, _version, _threshold = model, version, threshold
        ACTIVE.labels(version).set(1)
        RELOADS.labels(version).inc()
        LAST_RELOAD.labels(version).set_to_current_time()
        logger.info('Загружена модель %s, порог %.2f', version, threshold)
    return _model, _version, _threshold


def make_pred(df):
    """Возвращает DataFrame со score и fraud_flag и версию модели, которой он посчитан"""
    try:
        model, version, threshold = _current_model()
    except Exception:
        LOAD_ERRORS.inc()
        # если новую модель прочитать не удалось, продолжаем скорить прежней
        if _model is None:
            raise
        logger.exception('Не удалось загрузить новую модель, скорим версией %s', _version)
        model, version, threshold = _model, _version, _threshold

    scores = model.predict_proba(df[list(model.feature_names_)])[:, 1]
    return pd.DataFrame({
        'score': scores,
        'fraud_flag': (scores > threshold).astype(int),
    }), version

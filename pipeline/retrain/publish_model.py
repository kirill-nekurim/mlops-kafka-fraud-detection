"""Публикация модели в каталог, из которого читает сервис скоринга."""

import logging
import os
import shutil

from pipeline.registry import (candidate_paths, meta_path, model_path, read_json, read_latest_version,
                               write_atomic, write_json)
from pipeline.settings import LATEST_POINTER, MODELS_DIR

logger = logging.getLogger(__name__)


def publish_model(decision_info: dict) -> dict:
    """Если challenger прошел проверку, кладет его в MODELS_DIR и переключает latest.txt.

    Читает: candidates/model_<version>.cbm/.json.
    Пишет: model_<version>.cbm, model_<version>.json и latest.txt в MODELS_DIR.
    Файл модели копируется во временный файл и переименовывается (os.replace), а latest.txt
    обновляется последним — сервис скоринга никогда не увидит недописанную модель.
    Повтор безопасен: копирование и запись указателя дают тот же результат.
    Если модель не прошла проверку, ничего не публикуется, прод продолжает скорить прежней версией.
    """
    version = decision_info["version"]
    if not decision_info["publish"]:
        logger.info("Модель %s не публикуется: %s. В проде остается %s",
                    version, decision_info["reason"], read_latest_version())
        return {**decision_info, "published_version": None}

    cand_cbm, cand_json = candidate_paths(version)
    target = model_path(version)
    tmp = MODELS_DIR / f".model_{version}.cbm.tmp"
    shutil.copyfile(cand_cbm, tmp)
    os.replace(tmp, target)
    meta = read_json(cand_json)
    meta["test_metrics"] = decision_info["test_metrics"]
    meta["publish_reason"] = decision_info["reason"]
    write_json(meta_path(version), meta)
    write_atomic(LATEST_POINTER, version + "\n")
    logger.info("Опубликована модель %s -> %s", version, target)
    return {**decision_info, "published_version": version}

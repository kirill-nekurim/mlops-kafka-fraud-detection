"""Реестр моделей в общем каталоге MODELS_DIR (он же смонтирован в fraud_detector как /models).

Опубликованная модель: model_<version>.cbm + model_<version>.json (порог и метрики).
Текущая версия для скоринга записана в latest.txt. Кандидаты лежат в candidates/.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from pipeline.settings import CANDIDATES_DIR, LATEST_POINTER, MODELS_DIR


def model_path(version: str, directory: Path = MODELS_DIR) -> Path:
    return directory / f"model_{version}.cbm"


def meta_path(version: str, directory: Path = MODELS_DIR) -> Path:
    return directory / f"model_{version}.json"


def candidate_paths(version: str) -> tuple[Path, Path]:
    return model_path(version, CANDIDATES_DIR), meta_path(version, CANDIDATES_DIR)


def read_latest_version() -> str | None:
    """Версия, которой сейчас скорит сервис (None, если latest.txt еще не создан)."""
    if not LATEST_POINTER.exists():
        return None
    return LATEST_POINTER.read_text(encoding="utf-8").strip() or None


def write_atomic(path: Path, text: str) -> None:
    """Пишет файл через временный файл и os.replace, чтобы читатель не увидел его наполовину."""
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def write_json(path: Path, payload: dict) -> None:
    write_atomic(path, json.dumps(payload, ensure_ascii=False, indent=2))


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))

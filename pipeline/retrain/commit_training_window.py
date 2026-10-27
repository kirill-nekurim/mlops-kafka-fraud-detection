"""Фиксация нового окна обучения и сдвиг указателя месяца."""

import logging
import os
from datetime import date
from pathlib import Path

from pipeline.db import get_conn, set_training_state
from pipeline.settings import CURRENT_WINDOW, PENDING_WINDOW

logger = logging.getLogger(__name__)


def commit_training_window(publish_info: dict) -> str:
    """Последняя задача: делает pending-окно текущим и сдвигает next_batch_month.

    Выполняется только после того, как модель сохранена (кандидат в candidates/ и,
    если прошел проверку, опубликованная версия). Если любая предыдущая задача упала,
    сюда не доходим — указатель не двигается, и повторный прогон возьмет тот же месяц.
    Читает: pending_window.parquet. Пишет: current_window.parquet, training_state.
    Повтор безопасен: если pending уже переименован, окно второй раз не сдвигается,
    а training_state выставляется в явные значения от batch_month (не инкрементом).
    """
    pending = Path(PENDING_WINDOW)
    if pending.exists():
        os.replace(pending, CURRENT_WINDOW)
        logger.info("Окно обучения обновлено: %s", CURRENT_WINDOW)
    else:
        logger.info("pending-окна нет: оно уже было зафиксировано прошлой попыткой")

    conn = get_conn()
    try:
        set_training_state(conn, date.fromisoformat(publish_info["batch_month"]))
    finally:
        conn.close()
    return publish_info["published_version"] or "not_published"

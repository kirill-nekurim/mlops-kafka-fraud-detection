"""Дозапись батча нового месяца в размеченную витрину."""

import logging
from datetime import date

import pandas as pd

from pipeline.db import get_conn, replace_month

logger = logging.getLogger(__name__)


def append_labeled_batch(batch_info: dict) -> dict:
    """Дописывает батч в labeled_transactions.

    Читает: supply/supply_<YYYYMM>.parquet. Пишет: labeled_transactions.
    Запись идет в одной транзакции «удалить строки этого месяца + COPY батча».
    Месяц батча новый (после максимума витрины), поэтому удаляются только строки
    от предыдущей попытки этой же задачи — повтор не дублирует данные.
    """
    frame = pd.read_parquet(batch_info["batch_path"])
    conn = get_conn()
    try:
        replace_month(conn, frame, date.fromisoformat(batch_info["batch_month"]))
    finally:
        conn.close()
    logger.info("В витрину записан месяц %s: %d строк", batch_info["batch_month"], len(frame))
    return batch_info

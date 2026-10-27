"""Извлечение очередного месяца из размеченной витрины."""

import logging

from airflow.exceptions import AirflowSkipException

from pipeline.db import LABELED_COLUMNS, get_conn, month_after, read_sql, read_training_state
from pipeline.settings import BATCHES_DIR

logger = logging.getLogger(__name__)


def extract_month_batch() -> dict:
    """Выгружает из labeled_transactions месяц next_batch_month из training_state.

    Читает: training_state, labeled_transactions.
    Пишет: batches/batch_<YYYYMM>.parquet.
    Повтор безопасен: training_state не меняется, файл батча просто перезаписывается
    теми же строками. Если месяца в витрине еще нет, задача (и весь прогон) — skipped.
    """
    BATCHES_DIR.mkdir(parents=True, exist_ok=True)
    conn = get_conn()
    try:
        state = read_training_state(conn)
        month = state["next_batch_month"]
        frame = read_sql(
            conn,
            f"SELECT {', '.join(LABELED_COLUMNS)} FROM labeled_transactions "
            "WHERE transaction_time >= %s AND transaction_time < %s ORDER BY transaction_time",
            (month, month_after(month)),
        )
    finally:
        conn.close()

    if frame.empty:
        raise AirflowSkipException(f"В витрине нет данных за {month}: новый месяц еще не поставлен")

    path = BATCHES_DIR / f"batch_{month:%Y%m}.parquet"
    frame.to_parquet(path, index=False)
    logger.info("Окно %s — %s, извлечен месяц %s: %d строк",
                state["window_start"], state["window_end"], month, len(frame))
    return {"batch_month": month.isoformat(), "batch_path": str(path)}

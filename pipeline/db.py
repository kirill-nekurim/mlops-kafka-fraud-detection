"""Подключение к Postgres, состояние окна обучения и запись в размеченную витрину."""

import io
import logging
import os
from datetime import date

import pandas as pd
import psycopg2
from dateutil.relativedelta import relativedelta

from pipeline.settings import WINDOW_MONTHS

logger = logging.getLogger(__name__)

LABELED_COLUMNS = [
    "transaction_time", "merch", "cat_id", "amount", "name_1", "name_2", "gender", "street",
    "one_city", "us_state", "post_code", "lat", "lon", "population_city", "jobs",
    "merchant_lat", "merchant_lon", "target",
]


def get_conn():
    """Открывает соединение с базой fraud (параметры из POSTGRES_* переменных окружения)."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "fraud"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
    )


def read_sql(conn, sql: str, params=None) -> pd.DataFrame:
    """Выполняет SELECT и возвращает DataFrame (без предупреждений pandas про DBAPI)."""
    with conn.cursor() as cur:
        cur.execute(sql, params)
        columns = [c.name for c in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=columns)


def month_after(month_start: date) -> date:
    """Первое число следующего месяца (исключающая граница месяца month_start)."""
    return month_start + relativedelta(months=1)


def read_training_state(conn) -> dict:
    """Читает границы окна и месяц следующего батча из training_state (строка id = 1)."""
    with conn.cursor() as cur:
        cur.execute("SELECT window_start, window_end, next_batch_month FROM training_state WHERE id = 1")
        row = cur.fetchone()
    if row is None:
        raise RuntimeError("training_state пуст: сначала должен отработать data_loader")
    return {"window_start": row[0], "window_end": row[1], "next_batch_month": row[2]}


def set_training_state(conn, batch_month: date) -> None:
    """Записывает окно, которое заканчивается batch_month, и следующий месяц батча.

    Значения выставляются явно (а не инкрементом), поэтому повторный вызов
    с тем же batch_month ничего не меняет.
    """
    window_start = batch_month - relativedelta(months=WINDOW_MONTHS - 1)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE training_state
            SET window_start = %s, window_end = %s, next_batch_month = %s, updated_at = now()
            WHERE id = 1
            """,
            (window_start, batch_month, month_after(batch_month)),
        )
    conn.commit()
    logger.info("training_state: окно %s — %s, следующий месяц %s",
                window_start, batch_month, month_after(batch_month))


def replace_month(conn, frame: pd.DataFrame, month_start: date) -> None:
    """В одной транзакции удаляет строки месяца month_start и записывает frame через COPY.

    Так повторная запись того же батча не дублирует строки.
    """
    payload = frame.loc[:, LABELED_COLUMNS].copy()
    payload["transaction_time"] = pd.to_datetime(payload["transaction_time"])
    buffer = io.StringIO()
    payload.to_csv(buffer, index=False, header=False, date_format="%Y-%m-%d %H:%M:%S")
    buffer.seek(0)
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM labeled_transactions WHERE transaction_time >= %s AND transaction_time < %s",
            (month_start, month_after(month_start)),
        )
        cur.copy_expert(
            f"COPY labeled_transactions ({', '.join(LABELED_COLUMNS)}) FROM STDIN WITH (FORMAT CSV)",
            buffer,
        )
    conn.commit()

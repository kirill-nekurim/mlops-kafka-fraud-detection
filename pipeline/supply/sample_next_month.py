"""Сэмплирование строк витрины и перенос их в следующий календарный месяц."""

import calendar
import logging

import pandas as pd

from pipeline.db import LABELED_COLUMNS, get_conn, read_sql
from pipeline.settings import SUPPLY_BATCH_SIZE, SUPPLY_DIR

logger = logging.getLogger(__name__)


def shift_into_month(frame: pd.DataFrame, month_start: pd.Timestamp) -> pd.DataFrame:
    """Переносит transaction_time в месяц month_start, сохраняя день, час и минуту.

    Дни, которых нет в целевом месяце (31-е в 30-дневном месяце), обрезаются до последнего дня.
    """
    shifted = frame.copy()
    times = pd.to_datetime(shifted["transaction_time"])
    last_day = calendar.monthrange(month_start.year, month_start.month)[1]
    shifted["transaction_time"] = pd.to_datetime({
        "year": month_start.year,
        "month": month_start.month,
        "day": times.dt.day.clip(upper=last_day),
        "hour": times.dt.hour,
        "minute": times.dt.minute,
    })
    return shifted


def sample_next_month() -> dict:
    """Готовит батч нового месяца: SUPPLY_BATCH_SIZE случайных строк витрины со сдвинутым временем.

    Новый месяц — следующий после max(transaction_time) в витрине. Сэмплируем из последних
    12 месяцев, чтобы батч был похож на свежие данные. random_state зависит от месяца,
    поэтому повтор задачи до записи в витрину дает тот же самый батч.
    Читает: labeled_transactions. Пишет: supply/supply_<YYYYMM>.parquet. Витрину не меняет.
    """
    SUPPLY_DIR.mkdir(parents=True, exist_ok=True)
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT max(transaction_time) FROM labeled_transactions")
            max_time = cur.fetchone()[0]
        if max_time is None:
            raise RuntimeError("Витрина labeled_transactions пуста: должен отработать data_loader")
        month_start = (pd.Timestamp(max_time).to_period("M") + 1).to_timestamp()
        source_from = month_start - pd.DateOffset(months=12)
        # сортировка по всем колонкам делает порядок строк, а значит и выборку, воспроизводимой
        frame = read_sql(
            conn,
            f"SELECT {', '.join(LABELED_COLUMNS)} FROM labeled_transactions "
            f"WHERE transaction_time >= %s ORDER BY {', '.join(LABELED_COLUMNS)}",
            (source_from.to_pydatetime(),),
        )
    finally:
        conn.close()

    batch = frame.sample(n=min(SUPPLY_BATCH_SIZE, len(frame)),
                         random_state=month_start.year * 100 + month_start.month)
    batch = shift_into_month(batch, month_start)
    path = SUPPLY_DIR / f"supply_{month_start:%Y%m}.parquet"
    batch.to_parquet(path, index=False)
    logger.info("Батч %s: %d строк (фрод %d) из %d строк за последние 12 месяцев",
                f"{month_start:%Y-%m}", len(batch), int(batch["target"].sum()), len(frame))
    return {"batch_month": month_start.date().isoformat(), "batch_path": str(path), "rows": int(len(batch))}

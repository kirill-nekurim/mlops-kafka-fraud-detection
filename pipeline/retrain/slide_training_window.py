"""Сдвиг окна обучения на один месяц."""

import logging
from datetime import date

import pandas as pd

from pipeline.settings import CURRENT_WINDOW, PENDING_WINDOW, WINDOW_MONTHS

logger = logging.getLogger(__name__)


def slide_training_window(batch_info: dict) -> dict:
    """Добавляет новый месяц к текущему окну и отбрасывает месяцы, вышедшие за WINDOW_MONTHS.

    Читает: current_window.parquet (текущее окно) и parquet батча.
    Пишет: pending_window.parquet. current_window.parquet НЕ трогает — он заменяется
    только последней задачей после сохранения модели, поэтому повтор этой задачи
    всегда строит одно и то же окно и не сдвигает его второй раз.
    Границы окна считаются от batch_month явно, а не «выкинуть самый старый месяц»:
    так окно остается ровно WINDOW_MONTHS месяцев даже при повторах.
    """
    if not CURRENT_WINDOW.exists():
        raise FileNotFoundError(f"Нет текущего окна {CURRENT_WINDOW}: должен отработать data_loader")

    batch_month = pd.Timestamp(date.fromisoformat(batch_info["batch_month"]))
    window_start = batch_month - pd.DateOffset(months=WINDOW_MONTHS - 1)

    current = pd.read_parquet(CURRENT_WINDOW)
    batch = pd.read_parquet(batch_info["batch_path"])
    combined = pd.concat([current, batch], ignore_index=True)
    combined["transaction_time"] = pd.to_datetime(combined["transaction_time"])
    in_window = (combined["transaction_time"] >= window_start) & (
        combined["transaction_time"] < batch_month + pd.DateOffset(months=1))
    # если current уже содержал этот месяц (не должно быть, но на всякий случай) — убираем дубли
    window = combined.loc[in_window].drop_duplicates().sort_values("transaction_time")

    window.to_parquet(PENDING_WINDOW, index=False)
    months = window["transaction_time"].dt.to_period("M").value_counts().sort_index()
    logger.info("Новое окно %s — %s, строк по месяцам: %s",
                window_start.date(), batch_month.date(), months.to_dict())
    return {"batch_month": batch_info["batch_month"], "window_path": str(PENDING_WINDOW)}

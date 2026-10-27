"""Очистка окна обучения перед построением признаков (собственный препроцессинг)."""

import logging

import pandas as pd

from pipeline.settings import LABEL, WORK_DIR

logger = logging.getLogger(__name__)

AMOUNT_MAX = 50_000


def clean_frame(frame: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Убирает строки, на которых нельзя учиться, и возвращает статистику удалений.

    Правила: нет метки или времени; метка не 0/1; сумма <= 0 или > AMOUNT_MAX;
    координаты клиента/мерчанта вне допустимых диапазонов; полные дубликаты.
    """
    df = frame.copy()
    df["transaction_time"] = pd.to_datetime(df["transaction_time"], errors="coerce")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    stats = {"rows_in": int(len(df))}

    masks = {
        "no_label_or_time": df[LABEL].isna() | df["transaction_time"].isna(),
        "bad_label": ~df[LABEL].isin([0, 1]),
        "bad_amount": ~df["amount"].between(0, AMOUNT_MAX, inclusive="right"),
        "bad_coords": ~(
            df["lat"].between(-90, 90) & df["merchant_lat"].between(-90, 90)
            & df["lon"].between(-180, 180) & df["merchant_lon"].between(-180, 180)
        ),
    }
    drop = pd.Series(False, index=df.index)
    for name, mask in masks.items():
        stats[name] = int((mask & ~drop).sum())
        drop |= mask
    df = df.loc[~drop]

    before = len(df)
    df = df.drop_duplicates()
    stats["duplicates"] = before - len(df)
    df[LABEL] = df[LABEL].astype(int)
    stats["rows_out"] = int(len(df))
    return df, stats


def clean_window(window_info: dict) -> dict:
    """Чистит окно из pending_window.parquet.

    Читает: pending_window.parquet.
    Пишет: work/<YYYYMM>/clean_window.parquet.
    Повтор безопасен: функция детерминированная, вход не меняется, выход перезаписывается.
    """
    out_dir = WORK_DIR / window_info["batch_month"][:7].replace("-", "")
    out_dir.mkdir(parents=True, exist_ok=True)
    cleaned, stats = clean_frame(pd.read_parquet(window_info["window_path"]))
    path = out_dir / "clean_window.parquet"
    cleaned.to_parquet(path, index=False)
    logger.info("Очистка окна: %s", stats)
    return {"batch_month": window_info["batch_month"], "clean_path": str(path),
            "work_dir": str(out_dir), "clean_stats": stats}

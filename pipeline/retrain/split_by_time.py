"""Временной train/test split окна."""

import logging
from datetime import date

import pandas as pd

logger = logging.getLogger(__name__)


def split_by_time(clean_info: dict) -> dict:
    """Делит окно по времени: все более ранние месяцы — train, месяц батча (самый свежий) — test.

    Читает: work/<YYYYMM>/clean_window.parquet.
    Пишет: work/<YYYYMM>/train_raw.parquet и test_raw.parquet.
    Повтор безопасен: разбиение определяется только batch_month, файлы перезаписываются.
    """
    frame = pd.read_parquet(clean_info["clean_path"])
    frame["transaction_time"] = pd.to_datetime(frame["transaction_time"])
    test_month = pd.Period(date.fromisoformat(clean_info["batch_month"]), freq="M")
    periods = frame["transaction_time"].dt.to_period("M")
    train, test = frame.loc[periods < test_month], frame.loc[periods == test_month]
    if train.empty or test.empty:
        raise RuntimeError(f"Пустая часть split: train={len(train)}, test={len(test)}")

    train_path = f"{clean_info['work_dir']}/train_raw.parquet"
    test_path = f"{clean_info['work_dir']}/test_raw.parquet"
    train.sort_values("transaction_time").to_parquet(train_path, index=False)
    test.to_parquet(test_path, index=False)
    train_months = sorted(periods.loc[periods < test_month].astype(str).unique())
    logger.info("train %s: %d строк (фрод %d), test %s: %d строк (фрод %d)",
                train_months, len(train), int(train["target"].sum()),
                test_month, len(test), int(test["target"].sum()))
    return {
        "batch_month": clean_info["batch_month"],
        "work_dir": clean_info["work_dir"],
        "train_raw_path": train_path,
        "test_raw_path": test_path,
        "train_months": train_months,
        "test_month": str(test_month),
    }

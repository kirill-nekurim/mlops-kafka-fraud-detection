"""Правила Data Quality для размеченной витрины. Каждое правило возвращает словарь результата."""

import pandas as pd

from pipeline.db import LABELED_COLUMNS

CRITICAL_COLUMNS = ["transaction_time", "amount", "target", "merch", "cat_id", "lat", "lon",
                    "merchant_lat", "merchant_lon"]
AMOUNT_MAX = 50_000
FRAUD_RATE_RANGE = (0.001, 0.05)
DUPLICATE_SHARE_MAX = 0.01
MIN_ROWS_PER_MONTH = 300


def _result(rule_id, name, severity, passed, observed, expected, bad_rows=0):
    return {"id": rule_id, "name": name, "severity": severity, "passed": bool(passed),
            "observed": observed, "expected": expected, "bad_rows": int(bad_rows)}


def check_schema(df: pd.DataFrame) -> dict:
    missing = [c for c in LABELED_COLUMNS if c not in df.columns]
    return _result("DQ1", "Все обязательные колонки на месте", "error",
                   not missing, f"нет колонок: {missing}" if missing else "все 18 колонок",
                   "схема как у train.csv")


def check_not_null(df: pd.DataFrame) -> dict:
    nulls = df[CRITICAL_COLUMNS].isna().sum()
    bad = int(df[CRITICAL_COLUMNS].isna().any(axis=1).sum())
    return _result("DQ2", "Нет пропусков в ключевых полях", "error", bad == 0,
                   ", ".join(f"{k}: {v}" for k, v in nulls[nulls > 0].items()) or "пропусков нет",
                   f"0 NULL в {', '.join(CRITICAL_COLUMNS)}", bad)


def check_amount_range(df: pd.DataFrame) -> dict:
    amount = pd.to_numeric(df["amount"], errors="coerce")
    bad = int((~amount.between(0, AMOUNT_MAX, inclusive="right")).sum())
    return _result("DQ3", "Сумма транзакции в допустимом диапазоне", "error", bad == 0,
                   f"min={amount.min():.2f}, max={amount.max():.2f}", f"0 < amount <= {AMOUNT_MAX}", bad)


def check_target_binary(df: pd.DataFrame) -> dict:
    bad = int((~df["target"].isin([0, 1])).sum())
    values = sorted(df["target"].dropna().unique().tolist())
    return _result("DQ4", "Метка target бинарная", "error", bad == 0,
                   f"значения: {values}", "target ∈ {0, 1}", bad)


def check_coordinates(df: pd.DataFrame) -> dict:
    ok = (df["lat"].between(-90, 90) & df["merchant_lat"].between(-90, 90)
          & df["lon"].between(-180, 180) & df["merchant_lon"].between(-180, 180))
    bad = int((~ok).sum())
    return _result("DQ5", "Координаты клиента и мерчанта валидны", "error", bad == 0,
                   f"{bad} строк вне диапазона", "lat ∈ [-90, 90], lon ∈ [-180, 180]", bad)


def check_fraud_rate(df: pd.DataFrame) -> dict:
    rate = float(df["target"].mean()) if len(df) else 0.0
    low, high = FRAUD_RATE_RANGE
    return _result("DQ6", "Доля фрода в ожидаемых пределах", "warning", low <= rate <= high,
                   f"{rate:.3%}", f"от {low:.1%} до {high:.0%}")


def check_duplicates(df: pd.DataFrame) -> dict:
    dup = int(df.duplicated().sum())
    share = dup / len(df) if len(df) else 0.0
    return _result("DQ7", "Почти нет полных дубликатов строк", "warning", share <= DUPLICATE_SHARE_MAX,
                   f"{dup} дубликатов ({share:.2%})", f"<= {DUPLICATE_SHARE_MAX:.0%}", dup)


def check_monthly_continuity(df: pd.DataFrame) -> dict:
    months = pd.to_datetime(df["transaction_time"]).dt.to_period("M").value_counts().sort_index()
    if months.empty:
        return _result("DQ8", "Месяцы идут без пропусков и не пустые", "error", False, "витрина пуста",
                       f"каждый месяц >= {MIN_ROWS_PER_MONTH} строк")
    full_range = pd.period_range(months.index.min(), months.index.max(), freq="M")
    gaps = [str(p) for p in full_range if p not in months.index]
    small = [f"{p}: {n}" for p, n in months.items() if n < MIN_ROWS_PER_MONTH]
    problems = (f"пропущены месяцы {gaps}; " if gaps else "") + (f"мало строк: {small}" if small else "")
    return _result("DQ8", "Месяцы идут без пропусков и не пустые", "warning", not gaps and not small,
                   problems or f"{len(months)} месяцев, {months.index.min()} — {months.index.max()}",
                   f"нет пропусков, каждый месяц >= {MIN_ROWS_PER_MONTH} строк")


RULES = [check_schema, check_not_null, check_amount_range, check_target_binary,
         check_coordinates, check_fraud_rate, check_duplicates, check_monthly_continuity]

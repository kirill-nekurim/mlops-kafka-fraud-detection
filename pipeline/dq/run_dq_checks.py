"""Прогон правил Data Quality по снимку витрины."""

import json
import logging
from pathlib import Path

import pandas as pd

from pipeline.dq.rules import RULES, check_schema
from pipeline.settings import DQ_WORK_DIR

logger = logging.getLogger(__name__)


def run_dq_checks(snapshot_info: dict) -> dict:
    """Применяет все правила из pipeline.dq.rules к снимку.

    Читает: dq/snapshot_<ts>.parquet. Пишет: dq/results_<ts>.json.
    Если не прошла проверка схемы, остальные правила не запускаются (им не на чем считать).
    Повтор безопасен: правила только читают снимок, результат перезаписывается.
    """
    df = pd.read_parquet(snapshot_info["snapshot_path"])
    schema = check_schema(df)
    results = [schema] if not schema["passed"] else [rule(df) for rule in RULES]

    months = pd.to_datetime(df["transaction_time"]).dt.to_period("M") if schema["passed"] else None
    monthly = (df.groupby(months)["target"].agg(["size", "mean"]).reset_index()
               .rename(columns={"transaction_time": "month", "size": "rows", "mean": "fraud_rate"})
               .assign(month=lambda m: m["month"].astype(str)).to_dict("records")) if months is not None else []

    path = Path(DQ_WORK_DIR) / f"results_{snapshot_info['snapshot_at']}.json"
    payload = {**snapshot_info, "results": results, "monthly": monthly}
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    for r in results:
        logger.info("%s %s [%s]: %s", r["id"], "OK" if r["passed"] else "FAIL", r["severity"], r["observed"])
    return {**snapshot_info, "results_path": str(path)}

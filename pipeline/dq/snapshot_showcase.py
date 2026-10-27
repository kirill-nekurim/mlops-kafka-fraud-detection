"""Снимок витрины для проверок качества."""

import logging
from datetime import datetime, timezone

from pipeline.db import LABELED_COLUMNS, get_conn, read_sql
from pipeline.settings import DQ_WORK_DIR

logger = logging.getLogger(__name__)


def snapshot_showcase() -> dict:
    """Выгружает всю витрину labeled_transactions в parquet.

    Проверки идут по снимку, а не по живой таблице: DAG поставки может дописать месяц
    посреди проверок, и разные правила увидели бы разные данные.
    Читает: labeled_transactions. Пишет: dq/snapshot_<ts>.parquet. Витрину не меняет,
    повтор просто создает свежий снимок.
    """
    DQ_WORK_DIR.mkdir(parents=True, exist_ok=True)
    snapshot_at = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    conn = get_conn()
    try:
        frame = read_sql(conn, f"SELECT {', '.join(LABELED_COLUMNS)} FROM labeled_transactions")
    finally:
        conn.close()
    # рабочие файлы прошлых прогонов не нужны: отчеты остаются в DQ_REPORTS_DIR
    for old in sorted(DQ_WORK_DIR.glob("snapshot_*.parquet"))[:-3] + sorted(DQ_WORK_DIR.glob("results_*.json"))[:-3]:
        old.unlink(missing_ok=True)
    path = DQ_WORK_DIR / f"snapshot_{snapshot_at}.parquet"
    frame.to_parquet(path, index=False)
    logger.info("Снимок витрины: %d строк -> %s", len(frame), path)
    return {"snapshot_at": snapshot_at, "snapshot_path": str(path), "rows": int(len(frame))}

"""Удаление наблюдений старше глубины хранения."""

import logging

from dateutil.relativedelta import relativedelta

from pipeline.db import get_conn
from pipeline.settings import HISTORY_MONTHS

logger = logging.getLogger(__name__)


def prune_old_months(batch_info: dict) -> dict:
    """Удаляет из labeled_transactions строки старше HISTORY_MONTHS месяцев от самого нового месяца.

    Граница = начало месяца max(transaction_time) минус HISTORY_MONTHS месяцев;
    удаляются строки строго раньше границы. Читает и пишет labeled_transactions.
    Повтор безопасен: DELETE по той же границе второй раз ничего не удалит.
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT date_trunc('month', max(transaction_time))::date FROM labeled_transactions")
            newest = cur.fetchone()[0]
            cutoff = newest - relativedelta(months=HISTORY_MONTHS)
            cur.execute("DELETE FROM labeled_transactions WHERE transaction_time < %s", (cutoff,))
            deleted = cur.rowcount
        conn.commit()
    finally:
        conn.close()
    logger.info("Глубина хранения %d мес. от %s: удалено %d строк раньше %s",
                HISTORY_MONTHS, newest, deleted, cutoff)
    return {**batch_info, "cutoff": cutoff.isoformat(), "deleted_rows": int(deleted)}

"""Итоговая проверка: падение DAG, если нарушены критичные правила."""

import logging

logger = logging.getLogger(__name__)


def assert_dq_passed(report_info: dict) -> None:
    """Падает, если не прошло хотя бы одно правило с severity=error.

    Запускается после публикации отчета, чтобы отчет был доступен и при провале.
    Ничего не читает и не пишет, поэтому повтор безопасен.
    Предупреждения (severity=warning) только логируются.
    """
    if report_info["warnings"]:
        logger.warning("Предупреждения DQ: %s", report_info["warnings"])
    if report_info["errors"]:
        raise ValueError(f"Нарушены критичные правила DQ: {report_info['errors']}, "
                         f"см. {report_info['report_path']}")
    logger.info("Критичные правила DQ пройдены, отчет: %s", report_info["report_path"])

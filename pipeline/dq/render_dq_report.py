"""HTML-отчет по результатам проверок Data Quality."""

import html
import json
import logging

from pipeline.registry import write_atomic
from pipeline.settings import DQ_REPORTS_DIR

logger = logging.getLogger(__name__)

PAGE = """<!doctype html>
<html lang="ru"><head><meta charset="utf-8"><title>DQ report {snapshot_at}</title>
<style>
 body {{ font-family: -apple-system, Segoe UI, Roboto, sans-serif; margin: 32px; color: #1f2328; }}
 table {{ border-collapse: collapse; margin: 12px 0 28px; }}
 th, td {{ border: 1px solid #d0d7de; padding: 6px 10px; text-align: left; font-size: 14px; }}
 th {{ background: #f6f8fa; }}
 .ok {{ color: #1a7f37; font-weight: 600; }} .fail {{ color: #cf222e; font-weight: 600; }}
 .warn {{ color: #9a6700; font-weight: 600; }}
 .summary {{ font-size: 18px; margin: 12px 0; }}
</style></head><body>
<h1>Data Quality: labeled_transactions</h1>
<p>Снимок витрины: {snapshot_at} UTC, строк: {rows}</p>
<p class="summary">Итог: <span class="{status_class}">{status}</span>
 — пройдено {passed} из {total} правил</p>
<h2>Правила</h2>
<table><tr><th>ID</th><th>Правило</th><th>Критичность</th><th>Результат</th>
<th>Ожидается</th><th>Наблюдается</th><th>Плохих строк</th></tr>
{rule_rows}
</table>
<h2>Объем и доля фрода по месяцам</h2>
<table><tr><th>Месяц</th><th>Строк</th><th>Доля фрода</th></tr>
{month_rows}
</table>
</body></html>
"""


def render_dq_report(check_info: dict) -> dict:
    """Собирает HTML-отчет и кладет его в каталог отчетов рядом с каталогом моделей.

    Читает: dq/results_<ts>.json.
    Пишет: dq_reports/dq_report_<ts>.html и dq_reports/latest.html (копия последнего отчета).
    Файлы пишутся атомарно (temp + rename), повтор перезаписывает тот же отчет.
    """
    payload = json.loads(open(check_info["results_path"], encoding="utf-8").read())
    results = payload["results"]
    errors = [r for r in results if not r["passed"] and r["severity"] == "error"]
    warnings = [r for r in results if not r["passed"] and r["severity"] == "warning"]
    status, status_class = (("FAILED", "fail") if errors else ("PASSED WITH WARNINGS", "warn")
                            if warnings else ("PASSED", "ok"))

    def badge(r):
        if r["passed"]:
            return '<span class="ok">OK</span>'
        return '<span class="fail">FAIL</span>' if r["severity"] == "error" else '<span class="warn">WARN</span>'

    rule_rows = "\n".join(
        f"<tr><td>{r['id']}</td><td>{html.escape(r['name'])}</td><td>{r['severity']}</td><td>{badge(r)}</td>"
        f"<td>{html.escape(str(r['expected']))}</td><td>{html.escape(str(r['observed']))}</td>"
        f"<td>{r['bad_rows']}</td></tr>" for r in results)
    month_rows = "\n".join(
        f"<tr><td>{m['month']}</td><td>{m['rows']}</td><td>{m['fraud_rate']:.3%}</td></tr>"
        for m in payload["monthly"])
    page = PAGE.format(snapshot_at=payload["snapshot_at"], rows=payload["rows"], status=status,
                       status_class=status_class, passed=sum(r["passed"] for r in results),
                       total=len(results), rule_rows=rule_rows, month_rows=month_rows)

    DQ_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report = DQ_REPORTS_DIR / f"dq_report_{payload['snapshot_at']}.html"
    write_atomic(report, page)
    write_atomic(DQ_REPORTS_DIR / "latest.html", page)
    logger.info("Отчет DQ: %s (%s)", report, status)
    return {"report_path": str(report), "errors": [r["id"] for r in errors],
            "warnings": [r["id"] for r in warnings]}

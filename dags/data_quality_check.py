"""DAG проверок качества размеченной витрины. Логика задач — в pipeline.dq."""

from datetime import datetime, timedelta

from airflow.decorators import dag, task

from pipeline.dq.assert_dq_passed import assert_dq_passed
from pipeline.dq.render_dq_report import render_dq_report
from pipeline.dq.run_dq_checks import run_dq_checks
from pipeline.dq.snapshot_showcase import snapshot_showcase


@dag(
    dag_id="data_quality_check",
    description="Раз в 10 минут проверяет витрину labeled_transactions и публикует HTML-отчет",
    schedule="*/10 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["mlops", "data-quality"],
    doc_md="""
### Data Quality

snapshot_showcase → run_dq_checks → render_dq_report → assert_dq_passed

Отчет: `artifacts/dq_reports/latest.html` на хосте (`/opt/airflow/dq_reports` в контейнере).
Правила описаны в README и в `pipeline/dq/rules.py`.
""",
)
def data_quality_check():
    snapshot = task(snapshot_showcase, task_id="snapshot_showcase")()
    checked = task(run_dq_checks, task_id="run_dq_checks")(snapshot)
    report = task(render_dq_report, task_id="render_dq_report")(checked)
    task(assert_dq_passed, task_id="assert_dq_passed")(report)


data_quality_check()

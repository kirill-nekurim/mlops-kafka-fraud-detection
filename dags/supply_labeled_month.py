"""DAG поставки нового размеченного месяца в витрину. Логика задач — в pipeline.supply."""

from datetime import datetime, timedelta

from airflow.decorators import dag, task

from pipeline.supply.append_labeled_batch import append_labeled_batch
from pipeline.supply.prune_old_months import prune_old_months
from pipeline.supply.sample_next_month import sample_next_month


@dag(
    dag_id="supply_labeled_month",
    description="Раз в 5 минут дописывает в витрину 3000 строк следующего месяца и чистит историю старше 24 месяцев",
    schedule="*/5 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["mlops", "etl"],
    doc_md="""
### Поставка размеченного месяца

sample_next_month → append_labeled_batch → prune_old_months

Берем 3000 случайных строк витрины за последние 12 месяцев, переносим их `transaction_time`
в месяц после максимального и дописываем в `labeled_transactions`. Затем удаляем строки
старше 24 месяцев от самого нового месяца.
""",
)
def supply_labeled_month():
    batch = task(sample_next_month, task_id="sample_next_month")()
    appended = task(append_labeled_batch, task_id="append_labeled_batch")(batch)
    task(prune_old_months, task_id="prune_old_months")(appended)


supply_labeled_month()

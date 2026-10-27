"""DAG переобучения модели на скользящем окне. Здесь только граф, логика задач — в pipeline.retrain."""

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param

from pipeline.retrain.build_features import build_features
from pipeline.retrain.clean_window import clean_window
from pipeline.retrain.commit_training_window import commit_training_window
from pipeline.retrain.evaluate_against_champion import evaluate_against_champion
from pipeline.retrain.extract_month_batch import extract_month_batch
from pipeline.retrain.publish_model import publish_model
from pipeline.retrain.slide_training_window import slide_training_window
from pipeline.retrain.split_by_time import split_by_time
from pipeline.retrain.train_challenger import train_challenger


@dag(
    dag_id="retrain_fraud_model",
    description="Раз в 5 минут сдвигает окно на месяц, учит CatBoost и публикует его, если он не хуже прод-модели",
    schedule="*/5 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=20),
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    params={"force_publish": Param(False, type="boolean",
                                   description="Опубликовать модель, даже если она хуже текущей")},
    tags=["mlops", "retrain"],
    doc_md="""
### Переобучение модели фрода

extract_month_batch → slide_training_window → clean_window → split_by_time → build_features →
train_challenger → evaluate_against_champion → publish_model → commit_training_window

* окно — 4 месяца: 3 ранних в train, самый свежий в test;
* модель публикуется в `/opt/airflow/models` (у сервиса скоринга это `/models`), только если
  PR-AUC на test-месяце не ниже, чем у текущей прод-модели;
* указатель месяца в `training_state` сдвигается последней задачей, после сохранения модели.
""",
)
def retrain_fraud_model():
    batch = task(extract_month_batch, task_id="extract_month_batch")()
    window = task(slide_training_window, task_id="slide_training_window")(batch)
    cleaned = task(clean_window, task_id="clean_window")(window)
    split = task(split_by_time, task_id="split_by_time")(cleaned)
    features = task(build_features, task_id="build_features")(split)
    trained = task(train_challenger, task_id="train_challenger")(features)
    decision = task(evaluate_against_champion, task_id="evaluate_against_champion")(trained)
    published = task(publish_model, task_id="publish_model")(decision)
    task(commit_training_window, task_id="commit_training_window")(published)


retrain_fraud_model()

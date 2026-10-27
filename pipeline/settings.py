"""Общие пути и параметры пайплайна. Значения можно переопределить переменными окружения."""

import os
from pathlib import Path

TRAINING_DIR = Path(os.getenv("TRAINING_DATA_DIR", "/opt/airflow/training_data"))
MODELS_DIR = Path(os.getenv("MODELS_DIR", "/opt/airflow/models"))
DQ_REPORTS_DIR = Path(os.getenv("DQ_REPORTS_DIR", "/opt/airflow/dq_reports"))
FRAUD_SRC = os.getenv("FRAUD_SRC", "/opt/airflow/fraud_src")

# файлы окна обучения
CURRENT_WINDOW = TRAINING_DIR / "current_window.parquet"
PENDING_WINDOW = TRAINING_DIR / "pending_window.parquet"
BATCHES_DIR = TRAINING_DIR / "batches"
WORK_DIR = TRAINING_DIR / "work"
SUPPLY_DIR = TRAINING_DIR / "supply"
DQ_WORK_DIR = TRAINING_DIR / "dq"

# реестр моделей: опубликованные модели лежат в MODELS_DIR, кандидаты — в candidates/
CANDIDATES_DIR = MODELS_DIR / "candidates"
LATEST_POINTER = MODELS_DIR / "latest.txt"

# окно обучения: WINDOW_MONTHS месяцев, последний из них — test
WINDOW_MONTHS = int(os.getenv("WINDOW_MONTHS", "4"))
# поставка: размер батча нового месяца и глубина хранения витрины
SUPPLY_BATCH_SIZE = int(os.getenv("SUPPLY_BATCH_SIZE", "3000"))
HISTORY_MONTHS = int(os.getenv("HISTORY_MONTHS", "24"))

LABEL = "target"

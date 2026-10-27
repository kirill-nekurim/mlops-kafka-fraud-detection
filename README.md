# Real-time Fraud Detection + переобучение в Airflow

ДЗ №3 по MLOps. Развитие сервиса из ДЗ №1: сервис скоринга по-прежнему читает транзакции
из Kafka и отдает `score` и `fraud_flag`. Поверх него поднят Apache Airflow, который по расписанию:

* дописывает в размеченную витрину новый месяц (`supply_labeled_month`);
* переобучает CatBoost на скользящем окне и публикует модель в каталог, из которого читает
  сервис скоринга, — только если новая модель не хуже текущей (`retrain_fraud_model`);
* проверяет витрину на соответствие правилам Data Quality и публикует HTML-отчет (`data_quality_check`).

Сервис скоринга подхватывает новую модель без перезапуска контейнера, а в каждом результате
скоринга есть `model_version` — и в сообщении Kafka, и в таблице `scores`.

Данные: подвыборка (19 000 строк, январь 2019 — март 2020) из train соревнования
https://www.kaggle.com/competitions/teta-ml-1-2025, лежит в `data/train.csv`. Ничего скачивать не нужно.
Основа Airflow-части — семинар https://github.com/NikitaMalykhin/mlops_realtime_fraud_detector_airflow.

## Архитектура

```
                      data/train.csv
                            │ (data_loader, один раз)
                            ▼
 ┌──────────── Postgres: labeled_transactions, training_state, scores ◄────────────┐
 │                 ▲                 │                                              │
 │  supply_labeled_month   retrain_fraud_model ──► artifacts/models/ ──┐          db_writer
 │  data_quality_check ──► artifacts/dq_reports/       (общий каталог) │              ▲
 │                     (Airflow)                                       ▼              │
 │                                                                                    │
 interface (Streamlit) ──► [transactions] ──► fraud_detector (/models) ──► [scores] ──┘
```

| Сервис | Что делает |
|---|---|
| `zookeeper`, `kafka`, `kafka-setup` | брокер и создание топиков `transactions`, `scores` |
| `kafka-ui` | просмотр топиков — http://localhost:8090 |
| `postgres` | база `fraud`: витрина `labeled_transactions`, состояние окна `training_state`, результаты `scores` |
| `data_loader` | один раз заливает `data/train.csv` в витрину и готовит стартовое окно обучения, затем завершается |
| `airflow` | Airflow 2.10 (standalone: scheduler + webserver) — http://localhost:8080, логин `admin` / `admin` |
| `fraud_detector` | читает `transactions`, препроцессинг + скоринг моделью из `/models`, пишет в `scores` |
| `db_writer` | читает `scores` и пишет в Postgres (включая `model_version`) |
| `interface` | Streamlit — http://localhost:8501: отправка CSV в Kafka и просмотр результатов |

Общий каталог моделей — `./artifacts/models` на хосте. Он смонтирован в Airflow как `/opt/airflow/models`
и в `fraud_detector` как `/models`. Рядом с ним `./artifacts/dq_reports` — HTML-отчеты Data Quality.
Все сервисы в одной сети docker compose.

## Запуск

Нужны Docker и Docker Compose v2. Свободные порты: 8080, 8090, 8501, 5432, 9095
(если 8080 занят, порт Airflow можно поменять: `AIRFLOW_PORT=8081 docker compose up --build -d`).
Docker нужно выделить хотя бы ~3–4 ГБ памяти.

```bash
git clone https://github.com/kirill-nekurim/mlops-kafka-fraud-detection.git
cd mlops-kafka-fraud-detection
git checkout hw3
docker compose up --build -d
```

Первый запуск занимает несколько минут (сборка образа Airflow с CatBoost). Проверить состояние:

```bash
docker compose ps
```

`kafka-setup` и `data_loader` должны быть в статусе `Exited (0)` — они отрабатывают один раз.
Остальные сервисы — `Up`. Airflow поднимается еще ~1 минуту после старта контейнера.

Остановка с удалением всех данных (витрина, окно обучения, состояние Airflow):

```bash
docker compose down -v
rm -rf artifacts/models/* artifacts/dq_reports/*
```

## Параметры пайплайна

Заданы в `docker-compose.yml` (блок `x-pipeline-env`):

| Параметр | Значение | Смысл |
|---|---|---|
| `WINDOW_MONTHS` | 4 | размер скользящего окна обучения: 3 месяца train + 1 месяц test |
| `INITIAL_WINDOW_START` | 2019-01-01 | стартовое окно — январь–апрель 2019, первый месяц для DAG — май 2019 |
| `SUPPLY_BATCH_SIZE` | 3000 | размер батча нового месяца (на семинаре 100 000; у нас ~1 300 строк в месяц, поэтому меньше) |
| `HISTORY_MONTHS` | 24 | глубина истории витрины — 2 года (на семинаре 3 года) |

Расписание: `retrain_fraud_model` и `supply_labeled_month` — каждые 5 минут, `data_quality_check` — каждые 10 минут.
У всех DAG `catchup=False` и `max_active_runs=1`.

## DAG

Файлы в `dags/` только собирают граф. Функции задач лежат в отдельных модулях `pipeline/`,
у каждой есть docstring: что читает, что пишет и почему повторный запуск не портит данные.

### `retrain_fraud_model` — переобучение

```
extract_month_batch → slide_training_window → clean_window → split_by_time → build_features
    → train_challenger → evaluate_against_champion → publish_model → commit_training_window
```

1. **extract_month_batch** — берет из витрины месяц `next_batch_month` из `training_state`.
   Если месяца в витрине нет, прогон помечается как skipped.
2. **slide_training_window** — добавляет месяц к текущему окну и отбрасывает месяцы старше
   `WINDOW_MONTHS`. Результат пишется в `pending_window.parquet`, текущее окно не трогается.
3. **clean_window** — собственный препроцессинг данных: убираем строки без метки/времени, с меткой не 0/1,
   с суммой ≤ 0 или > 50 000, с невалидными координатами, полные дубликаты (статистика — в логе задачи).
4. **split_by_time** — разбиение по времени: 3 ранних месяца окна — train, самый свежий месяц — test.
5. **build_features** — признаки строятся функцией `run_preproc` из `fraud_detector/src/preprocessing.py`,
   той же, что в онлайн-скоринге: час, день недели, месяц, флаги «ночь» и «выходной»,
   haversine-расстояние клиент–мерчант, логарифмы суммы и населения, категориальные признаки.
6. **train_challenger** — CatBoost (300 деревьев, глубина 5, веса классов Balanced). Последние 20 % train
   по времени — валидация для подбора порога по F1. Модель сохраняется
   в `artifacts/models/candidates/`.
7. **evaluate_against_champion** — сравнение с прод-моделью (см. ниже).
8. **publish_model** — если модель прошла проверку: копирует ее в `artifacts/models/model_<version>.cbm`
   (+ `model_<version>.json` с порогом и метриками) и атомарно переключает `latest.txt`.
9. **commit_training_window** — делает новое окно текущим и сдвигает `next_batch_month` в `training_state`.

Версия модели: `<месяц батча>_<UTC-время обучения>`, например `201905_20261027T153012`.
Стартовая модель, зашитая в образ `fraud_detector`, имеет версию `initial`
(обучена скриптом `train/train_model.py` на январе–марте 2019 тем же кодом, что и в DAG).

**Повторный запуск после ошибки.** Указатель месяца и текущее окно меняет только последняя задача —
после того как модель сохранена. Если упала любая задача раньше, повторный прогон возьмет тот же месяц
и построит то же окно. Окно считается от `batch_month` по явным границам, а `training_state`
выставляется в явные значения (не инкрементом), поэтому повтор последней задачи не сдвигает окно второй раз.

#### Проверка улучшения модели (champion / challenger)

* Обе модели — новая (challenger) и текущая из `latest.txt` (champion) — скорят **один и тот же test-месяц**.
  На нем не училась ни одна из них: challenger учился на более ранних месяцах, а для champion это месяц «из будущего».
* Основная метрика — **PR-AUC** (average precision): фрода ~0.6 %, и ROC-AUC почти не различает модели,
  а PR-AUC чувствителен к качеству именно на редком классе и не зависит от порога. ROC-AUC и F1 тоже считаются и пишутся в отчет.
* Публикуем, если `PR-AUC(challenger) >= PR-AUC(champion)`. Если хуже — модель остается кандидатом в
  `candidates/`, прод продолжает скорить прежней версией.
* Если в test-месяце нет ни одного фрода, PR-AUC не определен — модель не публикуется.
* Если прод-модели еще нет — публикуем без сравнения.
* Решение с метриками обеих моделей пишется в `artifacts/models/candidates/model_<version>.decision.json`.
* Для демонстрации можно форсировать публикацию: Trigger DAG w/ config → `{"force_publish": true}`.

Окно при этом сдвигается в любом случае: отклоненная модель тоже сохранена (в `candidates/`),
и следующий запуск обучится уже на новых данных.

### `supply_labeled_month` — поставка нового месяца

```
sample_next_month → append_labeled_batch → prune_old_months
```

* **sample_next_month** — 3000 случайных строк витрины за последние 12 месяцев, `transaction_time`
  переносится в месяц, следующий за максимальным в витрине (день/час/минута сохраняются).
  Выборка детерминирована для месяца, повтор дает тот же батч.
* **append_labeled_batch** — в одной транзакции удаляет строки этого месяца и делает COPY батча,
  поэтому повтор не дублирует строки.
* **prune_old_months** — удаляет наблюдения старше 24 месяцев от самого нового месяца.

Исторические данные заканчиваются в марте 2020, поэтому поставка начинает с апреля 2020, а переобучение
идет по своему указателю с мая 2019 и доходит до поставленных месяцев позже. Окно обучения
всегда на ~12 месяцев позади самого нового месяца, так что очистка 24 месяцев его не затрагивает.

### `data_quality_check` — качество данных (DQ)

```
snapshot_showcase → run_dq_checks → render_dq_report → assert_dq_passed
```

Проверки идут по снимку витрины `labeled_transactions`. Отчет публикуется в `artifacts/dq_reports/`
(`dq_report_<время>.html` + `latest.html` с последним отчетом). Если нарушено правило с критичностью
`error`, последняя задача падает — но уже после публикации отчета.

| ID | Правило | Критичность |
|---|---|---|
| DQ1 | Все 18 колонок схемы `train.csv` на месте | error |
| DQ2 | Нет пропусков в `transaction_time`, `amount`, `target`, `merch`, `cat_id`, координатах | error |
| DQ3 | `0 < amount ≤ 50 000` | error |
| DQ4 | `target ∈ {0, 1}` | error |
| DQ5 | Координаты клиента и мерчанта: lat ∈ [-90, 90], lon ∈ [-180, 180] | error |
| DQ6 | Доля фрода от 0.1 % до 5 % | warning |
| DQ7 | Доля полных дубликатов строк ≤ 1 % | warning |
| DQ8 | Нет пропущенных месяцев, в каждом месяце ≥ 300 строк | warning |

Пороги правил — в `pipeline/dq/rules.py`.

## Проверка

### 1. Airflow и DAG

Открыть http://localhost:8080 (`admin` / `admin`). В списке три DAG: `retrain_fraud_model`,
`supply_labeled_month`, `data_quality_check`. Они включены и запускаются по расписанию;
для ручного запуска — кнопка ▶ (Trigger DAG). На вкладке Graph видна цепочка задач.

То же из консоли:

```bash
docker compose exec airflow airflow dags list
docker compose exec airflow airflow dags trigger retrain_fraud_model
docker compose exec airflow airflow dags list-runs -d retrain_fraud_model
```

### 2. Новая модель в общем каталоге

```bash
ls -la artifacts/models artifacts/models/candidates
cat artifacts/models/latest.txt
```

До первого прогона там только `model_initial.*` и `latest.txt = initial` (кладет `fraud_detector` при старте).
После каждого успешного прогона `retrain_fraud_model` появляется кандидат в `candidates/`, а если он не хуже
прод-модели — `model_<version>.cbm` в самом каталоге и новая версия в `latest.txt`.
Почему модель опубликована или нет — в `candidates/model_<version>.decision.json` и в логе задачи
`evaluate_against_champion`. Чтобы гарантированно увидеть публикацию, запустите DAG с конфигом:

```bash
docker compose exec airflow airflow dags trigger retrain_fraud_model --conf '{"force_publish": true}'
```

Состояние окна обучения:

```bash
docker compose exec postgres psql -U postgres -d fraud -c "SELECT * FROM training_state;"
docker compose exec postgres psql -U postgres -d fraud -c \
  "SELECT date_trunc('month', transaction_time)::date AS month, count(*), round(avg(target)::numeric, 4) AS fraud_rate
   FROM labeled_transactions GROUP BY 1 ORDER BY 1;"
```

### 3. Скоринг опубликованной версией

1. Открыть http://localhost:8501, вкладка **«Отправка транзакций»**, загрузить `data/test_sample.csv`
   и нажать «Отправить в Kafka».
2. Сравнить версию в результатах с `latest.txt`:

```bash
cat artifacts/models/latest.txt
docker compose exec postgres psql -U postgres -d fraud -c \
  "SELECT model_version, count(*), max(created_at) FROM scores GROUP BY 1 ORDER BY 3 DESC;"
```

Новые транзакции скорятся версией из `latest.txt`. Сообщения в Kafka UI (http://localhost:8090 → Topics → `scores`):

```json
{"transaction_id": "0b8e4c1e-...", "score": 0.0123, "fraud_flag": 0, "model_version": "201905_20261027T153012"}
```

На вкладке **«Результаты»** в UI есть таблица версий модели, которыми посчитаны транзакции.
Лог `fraud_detector` показывает момент переключения (контейнер не перезапускается):

```bash
docker compose logs fraud_detector | grep "Загружена модель"
```

### 4. Отчет Data Quality

После прогона `data_quality_check` открыть в браузере файл `artifacts/dq_reports/latest.html`:

```bash
open artifacts/dq_reports/latest.html        # macOS
xdg-open artifacts/dq_reports/latest.html    # Linux
```

## Структура

```
├── docker-compose.yml
├── data/                     # train.csv (размеченная подвыборка) и test_sample.csv для отправки в Kafka
├── postgres/init.sql         # таблицы scores, labeled_transactions, training_state
├── data_loader/              # первичная загрузка витрины и стартового окна
├── airflow/                  # образ Airflow с CatBoost
├── dags/                     # только графы DAG
│   ├── retrain_fraud_model.py
│   ├── supply_labeled_month.py
│   └── data_quality_check.py
├── pipeline/                 # функции задач
│   ├── settings.py, db.py, registry.py
│   ├── retrain/              # задачи переобучения
│   ├── supply/               # задачи поставки месяца
│   └── dq/                   # правила DQ и HTML-отчет
├── artifacts/
│   ├── models/               # общий каталог моделей (Airflow пишет, fraud_detector читает)
│   └── dq_reports/           # HTML-отчеты Data Quality
├── train/train_model.py      # обучение стартовой модели initial
├── fraud_detector/           # сервис скоринга (app/app.py, src/preprocessing.py, src/scorer.py, models/)
├── db_writer/                # запись скоров в Postgres
└── interface/                # Streamlit UI
```

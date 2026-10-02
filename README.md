# Real-time Fraud Detection (Kafka + Docker + Postgres)

ДЗ №1 по MLOps. Сервис скоринга фродовых транзакций, который читает транзакции из Kafka,
делает препроцессинг и скоринг моделью CatBoost (только inference, CPU) и пишет результат обратно в Kafka.
Результаты дополнительно складываются в Postgres и показываются в UI.

Основа взята с семинара: https://github.com/NikitaMalykhin/mts25_mlops_hw2_real_time_fraud_detection
Данные: https://www.kaggle.com/competitions/teta-ml-1-2025

## Архитектура

```
 interface (Streamlit) ──► [transactions] ──► fraud_detector ──► [scores] ──► db_writer ──► Postgres (таблица scores)
        ▲                                                                                         │
        └──────────────────────────── вкладка «Результаты» ◄──────────────────────────────────────┘
```

| Сервис | Что делает |
|---|---|
| `zookeeper`, `kafka` | брокер сообщений |
| `kafka-setup` | создает топики `transactions` и `scores` и завершается |
| `kafka-ui` | веб-интерфейс для просмотра топиков (http://localhost:8090) |
| `fraud_detector` | читает `transactions`, препроцессинг + скоринг, пишет в `scores` |
| `db_writer` | читает `scores` и складывает в Postgres |
| `postgres` | база `fraud`, витрина `scores` создается из `postgres/init.sql` |
| `interface` | Streamlit UI (http://localhost:8501): отправка CSV в Kafka и просмотр результатов |

### fraud_detector

Этапы разнесены по отдельным скриптам:
- `app/app.py` — чтение сообщений из Kafka и отправка скора и флага фрода в топик `scores`
- `src/preprocessing.py` — препроцессинг: временные признаки (час, день недели, месяц),
  расстояние клиент–мерчант (haversine), логарифм суммы и населения, категориальные признаки как строки
- `src/scorer.py` — загрузка модели и скоринг, флаг фрода по порогу из `models/threshold.json`

Модель — легкий CatBoost (500 деревьев, глубина 6), лежит в `fraud_detector/models/model.cbm`.
Обучение делается отдельно скриптом `train/train_model.py` (в контейнере только inference).
Порог подобран по F1 на валидации.

## Запуск

Требуется Docker и Docker Compose v2.

```bash
git clone https://github.com/kirill-nekurim/mlops-kafka-fraud-detection.git
cd mlops-kafka-fraud-detection
docker compose up --build -d
```

Первый запуск занимает несколько минут (скачиваются образы и ставятся зависимости).
Проверить, что всё поднялось:

```bash
docker compose ps
```

`kafka-setup` должен быть в статусе `Exited (0)` — это нормально, он только создает топики.

## Проверка работы

1. Открыть UI: http://localhost:8501
2. На вкладке **«Отправка транзакций»** загрузить CSV формата `test.csv`
   (для теста в репозитории есть `data/test_sample.csv` — 1000 транзакций) и нажать «Отправить в Kafka».
3. Посмотреть сообщения в Kafka UI: http://localhost:8090 → Topics → `transactions` / `scores`.
   Формат сообщения в `scores`:
   ```json
   {"transaction_id": "0b8e4c1e-...", "score": 0.0123, "fraud_flag": 0}
   ```
4. На вкладке **«Результаты»** нажать **«Посмотреть результаты»** — выведутся
   10 последних транзакций с `fraud_flag = 1` и гистограмма скоров последних 100 транзакций.

Логи сервисов:

```bash
docker compose logs -f fraud_detector
docker compose logs -f db_writer
```

Данные в Postgres можно посмотреть и напрямую:

```bash
docker compose exec postgres psql -U postgres -d fraud -c "SELECT * FROM scores ORDER BY id DESC LIMIT 10;"
```

Остановка (с удалением данных базы):

```bash
docker compose down -v
```

## Обучение модели (необязательно)

Модель уже лежит в репозитории. Чтобы переобучить:

```bash
pip install catboost==1.2.8 pandas==2.2.3 scikit-learn
python train/train_model.py
```

Для обучения используется `data/train.csv` — подвыборка из train соревнования
(полный train.csv можно скачать с Kaggle и положить на его место).

## Структура

```
├── docker-compose.yml
├── data/                  # train.csv (подвыборка) и test_sample.csv для теста
├── train/train_model.py   # обучение модели
├── postgres/init.sql      # создание витрины scores
├── fraud_detector/        # сервис скоринга
│   ├── app/app.py
│   ├── src/preprocessing.py
│   ├── src/scorer.py
│   └── models/
├── db_writer/             # сервис записи скоров в Postgres
└── interface/             # Streamlit UI
```

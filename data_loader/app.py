"""
Первичная загрузка размеченной витрины.

Один раз при старте:
1. заливает data/train.csv в labeled_transactions (если таблица пуста);
2. создает строку training_state со стартовым окном обучения;
3. сохраняет стартовое окно в current_window.parquet для DAG переобучения.
Повторный запуск без `docker compose down -v` ничего не перезаписывает.
"""
import io
import logging
import os
import time
from pathlib import Path

import pandas as pd
import psycopg2

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('data_loader')

CSV_PATH = Path(os.getenv('LABELED_CSV_PATH', '/data/train.csv'))
TRAINING_DIR = Path(os.getenv('TRAINING_DATA_DIR', '/training_data'))
CURRENT_WINDOW = TRAINING_DIR / 'current_window.parquet'
WINDOW_MONTHS = int(os.getenv('WINDOW_MONTHS', '4'))
# стартовое окно: WINDOW_MONTHS месяцев начиная с INITIAL_WINDOW_START
INITIAL_WINDOW_START = pd.Timestamp(os.getenv('INITIAL_WINDOW_START', '2019-01-01'))

COLUMNS = [
    'transaction_time', 'merch', 'cat_id', 'amount', 'name_1', 'name_2', 'gender', 'street',
    'one_city', 'us_state', 'post_code', 'lat', 'lon', 'population_city', 'jobs',
    'merchant_lat', 'merchant_lon', 'target',
]


def connect():
    # постгрес может подниматься чуть дольше, поэтому пробуем несколько раз
    for _ in range(30):
        try:
            return psycopg2.connect(
                host=os.getenv('POSTGRES_HOST', 'postgres'),
                port=os.getenv('POSTGRES_PORT', '5432'),
                dbname=os.getenv('POSTGRES_DB', 'fraud'),
                user=os.getenv('POSTGRES_USER', 'postgres'),
                password=os.getenv('POSTGRES_PASSWORD', 'postgres'),
            )
        except psycopg2.OperationalError:
            logger.info('Postgres is not ready, retry...')
            time.sleep(2)
    raise RuntimeError('Could not connect to Postgres')


def load_csv(conn):
    with conn.cursor() as cur:
        cur.execute('SELECT count(*) FROM labeled_transactions')
        if cur.fetchone()[0] > 0:
            logger.info('labeled_transactions уже заполнена, CSV не загружаем')
            return
    df = pd.read_csv(CSV_PATH)[COLUMNS]
    df['transaction_time'] = pd.to_datetime(df['transaction_time'])
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False, date_format='%Y-%m-%d %H:%M:%S')
    buffer.seek(0)
    with conn.cursor() as cur:
        cur.copy_expert(f"COPY labeled_transactions ({', '.join(COLUMNS)}) FROM STDIN WITH (FORMAT CSV)", buffer)
    conn.commit()
    logger.info('Загружено %d строк, %s — %s', len(df), df.transaction_time.min(), df.transaction_time.max())


def ensure_state(conn):
    window_end = INITIAL_WINDOW_START + pd.DateOffset(months=WINDOW_MONTHS - 1)
    next_batch = window_end + pd.DateOffset(months=1)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO training_state (id, window_start, window_end, next_batch_month)
            VALUES (1, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
            """,
            (INITIAL_WINDOW_START.date(), window_end.date(), next_batch.date()),
        )
    conn.commit()


def materialize_window(conn):
    TRAINING_DIR.mkdir(parents=True, exist_ok=True)
    os.chmod(TRAINING_DIR, 0o777)
    if CURRENT_WINDOW.exists():
        logger.info('%s уже есть, стартовое окно не перезаписываем', CURRENT_WINDOW)
        return
    with conn.cursor() as cur:
        cur.execute('SELECT window_start, window_end FROM training_state WHERE id = 1')
        start, end = cur.fetchone()
        end_exclusive = (pd.Timestamp(end) + pd.DateOffset(months=1)).to_pydatetime()
        cur.execute(
            f"SELECT {', '.join(COLUMNS)} FROM labeled_transactions "
            "WHERE transaction_time >= %s AND transaction_time < %s ORDER BY transaction_time",
            (start, end_exclusive),
        )
        df = pd.DataFrame(cur.fetchall(), columns=COLUMNS)
    df.to_parquet(CURRENT_WINDOW, index=False)
    os.chmod(CURRENT_WINDOW, 0o666)
    logger.info('Стартовое окно %s — %s: %d строк -> %s', start, end, len(df), CURRENT_WINDOW)


def main():
    conn = connect()
    try:
        load_csv(conn)
        ensure_state(conn)
        materialize_window(conn)
    finally:
        conn.close()
    logger.info('Готово')


if __name__ == '__main__':
    main()

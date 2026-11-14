import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from confluent_kafka import Consumer
from prometheus_client import start_http_server

from metrics import DELAY, DURATION, DUPLICATES, ERRORS, FRAUD, LAST_SUCCESS, SCORES, WRITTEN

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(levelname)s %(message)s')
logger = logging.getLogger('db_writer')
INSERT_SQL = """
    INSERT INTO scores (transaction_id, score, fraud_flag, model_version,
                        us_state, merch, cat_id, amount, scored_at, produced_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (transaction_id) DO NOTHING
"""


def connect_db():
    while True:
        try:
            conn = psycopg2.connect(os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@postgres:5432/fraud'))
            conn.autocommit = True
            return conn
        except psycopg2.OperationalError:
            logger.warning('Postgres пока недоступен')
            time.sleep(2)


def main():
    start_http_server(int(os.getenv('METRICS_PORT', '8001')))
    conn = connect_db()
    with conn.cursor() as cur:
        cur.execute(Path('monitoring.sql').read_text())
    consumer = Consumer({
        'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
        'group.id': 'db-writer', 'auto.offset.reset': 'earliest', 'enable.auto.commit': False,
    })
    consumer.subscribe([os.getenv('KAFKA_SCORES_TOPIC', 'scores')])
    while True:
        msg = consumer.poll(1)
        if msg is None:
            continue
        if msg.error():
            ERRORS.inc()
            continue
        try:
            data = json.loads(msg.value())
            values = (data['transaction_id'], data['score'], data['fraud_flag'],
                      data.get('model_version', 'unknown'), data.get('us_state') or 'UNKNOWN',
                      data.get('merch') or 'UNKNOWN', data.get('cat_id') or 'UNKNOWN',
                      data.get('amount'), data.get('scored_at'), data.get('produced_at'))
        except (ValueError, KeyError, TypeError):
            ERRORS.inc()
            logger.exception('Некорректный результат в Kafka')
            consumer.commit(message=msg, asynchronous=False)
            continue
        while True:
            try:
                started = time.perf_counter()
                with conn.cursor() as cur:
                    cur.execute(INSERT_SQL, values)
                    inserted = cur.rowcount == 1
                DURATION.observe(time.perf_counter() - started)
                if inserted:
                    WRITTEN.inc()
                    FRAUD.inc(data['fraud_flag'])
                    SCORES.observe(data['score'])
                    LAST_SUCCESS.set_to_current_time()
                    if data.get('scored_at'):
                        delay = datetime.now(timezone.utc) - datetime.fromisoformat(data['scored_at'])
                        DELAY.observe(max(0, delay.total_seconds()))
                else:
                    DUPLICATES.inc()
                consumer.commit(message=msg, asynchronous=False)
                break
            except psycopg2.Error:
                ERRORS.inc()
                logger.exception('Запись не прошла, offset не двигаем')
                conn.close()
                time.sleep(1)
                conn = connect_db()


if __name__ == '__main__':
    main()

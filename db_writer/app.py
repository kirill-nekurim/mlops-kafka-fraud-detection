import json
import logging
import os
import time

import psycopg2
from confluent_kafka import Consumer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('db_writer')

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
SCORES_TOPIC = os.getenv('KAFKA_SCORES_TOPIC', 'scores')
DB_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@postgres:5432/fraud')

INSERT_SQL = """
    INSERT INTO scores (transaction_id, score, fraud_flag)
    VALUES (%s, %s, %s)
    ON CONFLICT (transaction_id) DO NOTHING
"""


def connect_db():
    # постгрес может подниматься чуть дольше, поэтому пробуем несколько раз
    for _ in range(30):
        try:
            return psycopg2.connect(DB_URL)
        except psycopg2.OperationalError:
            logger.info('Postgres is not ready, retry...')
            time.sleep(2)
    raise RuntimeError('Could not connect to Postgres')


def main():
    conn = connect_db()
    conn.autocommit = True

    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'db-writer',
        'auto.offset.reset': 'earliest',
    })
    consumer.subscribe([SCORES_TOPIC])

    logger.info('Reading topic "%s" and writing to Postgres...', SCORES_TOPIC)
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logger.error('Kafka error: %s', msg.error())
            continue

        try:
            data = json.loads(msg.value().decode('utf-8'))
            with conn.cursor() as cur:
                cur.execute(INSERT_SQL, (data['transaction_id'], data['score'], data['fraud_flag']))
            logger.info('Saved %s', data['transaction_id'])
        except Exception as e:
            logger.error('Error saving message: %s', e)


if __name__ == '__main__':
    main()

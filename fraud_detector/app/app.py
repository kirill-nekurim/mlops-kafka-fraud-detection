import json
import logging
import os
import sys

import pandas as pd
from confluent_kafka import Consumer, Producer

# чтобы импортировать модули из src
sys.path.append(os.path.abspath('./src'))
from preprocessing import run_preproc
from scorer import make_pred

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('fraud_detector')

# настройки берем из переменных окружения (см. docker-compose.yml)
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
TRANSACTIONS_TOPIC = os.getenv('KAFKA_TRANSACTIONS_TOPIC', 'transactions')
SCORES_TOPIC = os.getenv('KAFKA_SCORES_TOPIC', 'scores')


def main():
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'ml-scorer',
        'auto.offset.reset': 'earliest',
    })
    consumer.subscribe([TRANSACTIONS_TOPIC])
    producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})

    logger.info('Waiting for transactions in topic "%s"...', TRANSACTIONS_TOPIC)
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logger.error('Kafka error: %s', msg.error())
            continue

        try:
            # сообщение: {"transaction_id": ..., "data": {...строка из test.csv...}}
            data = json.loads(msg.value().decode('utf-8'))
            transaction_id = data['transaction_id']
            input_df = pd.DataFrame([data['data']])

            # препроцессинг + скоринг
            features = run_preproc(input_df)
            pred = make_pred(features).iloc[0]

            result = {
                'transaction_id': transaction_id,
                'score': float(pred['score']),
                'fraud_flag': int(pred['fraud_flag']),
            }
            # отправляем результат в топик scores
            producer.produce(SCORES_TOPIC, key=transaction_id, value=json.dumps(result))
            producer.poll(0)
            logger.info('Scored %s: %.4f (fraud=%d)', transaction_id, result['score'], result['fraud_flag'])
        except Exception as e:
            logger.error('Error processing message: %s', e)


if __name__ == '__main__':
    main()

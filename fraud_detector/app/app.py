import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

import pandas as pd
from confluent_kafka import Consumer, Producer
from prometheus_client import start_http_server

sys.path.append(os.path.abspath('./src'))
from preprocessing import run_preproc
from scorer import ensure_bootstrap, make_pred
from metrics import DURATION, ERRORS, FRAUD, FRAUD_RATIO, LAST_SUCCESS, PROCESSED, PUBLISH_ERRORS, SCORES

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(levelname)s %(message)s')
logger = logging.getLogger('fraud_detector')


def main():
    start_http_server(int(os.getenv('METRICS_PORT', '8000')))
    ensure_bootstrap()
    consumer = Consumer({
        'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
        'group.id': 'ml-scorer', 'auto.offset.reset': 'earliest', 'enable.auto.commit': False,
    })
    consumer.subscribe([os.getenv('KAFKA_TRANSACTIONS_TOPIC', 'transactions')])
    producer = Producer({'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
                         'enable.idempotence': True})
    processed = flagged = 0
    while True:
        msg = consumer.poll(1)
        if msg is None:
            continue
        if msg.error():
            ERRORS.inc()
            logger.error('Kafka: %s', msg.error())
            continue
        started = time.perf_counter()
        try:
            data = json.loads(msg.value())
            row = data['data']
            features = run_preproc(pd.DataFrame([row]))
            predictions, version = make_pred(features)
            pred = predictions.iloc[0]
            result = {
                'transaction_id': data['transaction_id'], 'score': float(pred['score']),
                'fraud_flag': int(pred['fraud_flag']), 'model_version': version,
                'us_state': row.get('us_state') or 'UNKNOWN',
                'merch': row.get('merch') or 'UNKNOWN', 'cat_id': row.get('cat_id') or 'UNKNOWN',
                'amount': float(row['amount']),
                'scored_at': datetime.now(timezone.utc).isoformat(),
                'produced_at': data.get('produced_at'),
            }
        except Exception:
            ERRORS.inc()
            logger.exception('Некорректная транзакция: пропускаем сообщение')
            consumer.commit(message=msg, asynchronous=False)
            continue

        # Offset фиксируем после подтверждения Kafka. При сбое повторяем тот же результат.
        while True:
            delivery = []
            try:
                producer.produce(os.getenv('KAFKA_SCORES_TOPIC', 'scores'),
                                 key=result['transaction_id'], value=json.dumps(result),
                                 on_delivery=lambda error, record, ack=delivery: ack.append(error))
                pending = producer.flush(10)
                if pending or not delivery or delivery[0] is not None:
                    raise RuntimeError(f'Kafka delivery: {delivery}')
                break
            except Exception:
                PUBLISH_ERRORS.inc()
                logger.exception('Не удалось опубликовать скор, повторяем')
                time.sleep(1)
        PROCESSED.inc()
        FRAUD.inc(result['fraud_flag'])
        processed += 1
        flagged += result['fraud_flag']
        FRAUD_RATIO.set(flagged / processed)
        SCORES.observe(result['score'])
        DURATION.observe(time.perf_counter() - started)
        LAST_SUCCESS.set_to_current_time()
        consumer.commit(message=msg, asynchronous=False)
        logger.info('Scored %s model=%s score=%.4f', result['transaction_id'], version, result['score'])


if __name__ == '__main__':
    main()

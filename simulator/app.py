"""Непрерывный поток настоящих строк CSV. MODE=fraud нужен для проверки алерта."""

import csv
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

with open(os.getenv('CSV_PATH', '/data/train.csv')) as stream:
    rows = list(csv.DictReader(stream))
mode = os.getenv('MODE', 'normal')
if mode == 'fraud':
    rows = [row for row in rows if row.get('target') == '1']
if not rows:
    raise RuntimeError('Нет строк для выбранного режима')
rate = float(os.getenv('TPS', '5'))
if rate <= 0:
    raise ValueError('TPS должен быть положительным')
rng = random.Random(42)
producer = Producer({'bootstrap.servers': 'kafka:9092', 'enable.idempotence': True})
print(f'Поток: {rate} TPS, MODE={mode}, строк={len(rows)}', flush=True)
while True:
    row = dict(rng.choice(rows))
    row.pop('target', None)  # Истинная метка не передаётся в модель.
    for column in ['amount', 'lat', 'lon', 'merchant_lat', 'merchant_lon', 'population_city']:
        row[column] = float(row[column]) if row.get(column) else None
    message = {'transaction_id': str(uuid.uuid4()), 'data': row,
               'produced_at': datetime.now(timezone.utc).isoformat()}
    producer.produce('transactions', value=json.dumps(message))
    producer.flush(5)
    time.sleep(1 / rate)

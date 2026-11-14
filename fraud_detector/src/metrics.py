"""Метрики обновляются только по реальным сообщениям и успешным загрузкам модели."""

from prometheus_client import Counter, Gauge, Histogram

PROCESSED = Counter('scorer_transactions_total', 'Successfully published scores')
FRAUD = Counter('scorer_fraud_total', 'Scores above the model threshold')
FRAUD_RATIO = Gauge('scorer_fraud_ratio', 'Fraction of fraud flags since process start, 0..1')
ERRORS = Counter('scorer_errors_total', 'Invalid messages and scoring failures')
PUBLISH_ERRORS = Counter('scorer_publish_failures_total', 'Kafka delivery failures')
DURATION = Histogram('scorer_processing_seconds', 'Preprocessing, inference and Kafka acknowledgement',
                     buckets=(.001, .005, .01, .025, .05, .1, .25, .5, 1, 2, 5))
SCORES = Histogram('scorer_score', 'Predicted probability', buckets=(0, .01, .05, .1, .2, .3, .5, .7, .9, 1))
LAST_SUCCESS = Gauge('scorer_last_success_timestamp_seconds', 'Time of the last published score')
LAST_SUCCESS.set_to_current_time()
RELOADS = Counter('scorer_model_loads_total', 'Successful model loads', ['model_version'])
ACTIVE = Gauge('scorer_model_active', 'Currently loaded model', ['model_version'])
LAST_RELOAD = Gauge('scorer_model_loaded_timestamp_seconds', 'Last successful load time', ['model_version'])
LOAD_ERRORS = Counter('scorer_model_load_failures_total', 'Failed model reloads; previous model retained')

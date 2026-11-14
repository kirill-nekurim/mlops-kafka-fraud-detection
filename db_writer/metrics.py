"""Счётчики успешных INSERT, а не попыток и повторных сообщений."""

from prometheus_client import Counter, Gauge, Histogram

WRITTEN = Counter('writer_scores_total', 'New rows committed to PostgreSQL')
FRAUD = Counter('writer_fraud_total', 'New rows with fraud_flag=1')
ERRORS = Counter('writer_errors_total', 'Write errors')
DUPLICATES = Counter('writer_duplicates_total', 'Messages already stored by transaction_id')
DURATION = Histogram('writer_insert_seconds', 'INSERT including database acknowledgement',
                     buckets=(.001, .005, .01, .025, .05, .1, .25, .5, 1, 2))
SCORES = Histogram('writer_score', 'Stored probability', buckets=(0, .01, .05, .1, .2, .3, .5, .7, .9, 1))
DELAY = Histogram('writer_delivery_delay_seconds', 'Time from scoring to database storage',
                  buckets=(.01, .05, .1, .25, .5, 1, 2, 5, 10, 30, 60))
LAST_SUCCESS = Gauge('writer_last_success_timestamp_seconds', 'Last successful INSERT time')

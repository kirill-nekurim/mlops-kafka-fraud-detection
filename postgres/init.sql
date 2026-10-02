-- витрина со скорами транзакций
CREATE TABLE IF NOT EXISTS scores (
    id             SERIAL PRIMARY KEY,
    transaction_id TEXT UNIQUE NOT NULL,
    score          DOUBLE PRECISION NOT NULL,
    fraud_flag     INTEGER NOT NULL,
    created_at     TIMESTAMP DEFAULT now()
);

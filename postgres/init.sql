-- витрина со скорами транзакций (пишет db_writer)
CREATE TABLE IF NOT EXISTS scores (
    id             SERIAL PRIMARY KEY,
    transaction_id TEXT UNIQUE NOT NULL,
    score          DOUBLE PRECISION NOT NULL,
    fraud_flag     INTEGER NOT NULL,
    model_version  TEXT NOT NULL,
    created_at     TIMESTAMP DEFAULT now()
);

-- размеченная витрина для обучения (заливает data_loader, пополняет DAG supply_labeled_month)
CREATE TABLE IF NOT EXISTS labeled_transactions (
    transaction_time TIMESTAMP NOT NULL,
    merch            TEXT,
    cat_id           TEXT,
    amount           DOUBLE PRECISION,
    name_1           TEXT,
    name_2           TEXT,
    gender           TEXT,
    street           TEXT,
    one_city         TEXT,
    us_state         TEXT,
    post_code        INTEGER,
    lat              DOUBLE PRECISION,
    lon              DOUBLE PRECISION,
    population_city  INTEGER,
    jobs             TEXT,
    merchant_lat     DOUBLE PRECISION,
    merchant_lon     DOUBLE PRECISION,
    target           SMALLINT
);
CREATE INDEX IF NOT EXISTS labeled_transactions_time_idx ON labeled_transactions (transaction_time);

-- состояние скользящего окна обучения (одна строка, id = 1)
CREATE TABLE IF NOT EXISTS training_state (
    id               INT PRIMARY KEY,
    window_start     DATE NOT NULL,
    window_end       DATE NOT NULL,
    next_batch_month DATE NOT NULL,
    updated_at       TIMESTAMP DEFAULT now()
);

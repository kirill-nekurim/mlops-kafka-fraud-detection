-- Выполняется и при первом старте PostgreSQL, и writer при старте со старым томом.
ALTER TABLE scores ADD COLUMN IF NOT EXISTS us_state TEXT NOT NULL DEFAULT 'UNKNOWN';
ALTER TABLE scores ADD COLUMN IF NOT EXISTS merch TEXT NOT NULL DEFAULT 'UNKNOWN';
ALTER TABLE scores ADD COLUMN IF NOT EXISTS cat_id TEXT NOT NULL DEFAULT 'UNKNOWN';
ALTER TABLE scores ADD COLUMN IF NOT EXISTS amount DOUBLE PRECISION;
ALTER TABLE scores ADD COLUMN IF NOT EXISTS scored_at TIMESTAMPTZ;
ALTER TABLE scores ADD COLUMN IF NOT EXISTS produced_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS scores_scored_at_idx ON scores (scored_at);
CREATE INDEX IF NOT EXISTS scores_created_at_idx ON scores (created_at DESC, id DESC);

DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'grafana_reader') THEN
        CREATE ROLE grafana_reader LOGIN PASSWORD 'grafana_local';
    END IF;
END $$;
GRANT CONNECT ON DATABASE fraud TO grafana_reader;
GRANT USAGE ON SCHEMA public TO grafana_reader;
GRANT SELECT ON scores TO grafana_reader;

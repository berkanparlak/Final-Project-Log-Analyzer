CREATE TABLE IF NOT EXISTS anomalies (
    id SERIAL PRIMARY KEY,
    log_type TEXT,
    trace_id TEXT,
    ts TEXT,
    event TEXT,
    score REAL,
    rec TEXT,
    reason TEXT,
    type TEXT
);

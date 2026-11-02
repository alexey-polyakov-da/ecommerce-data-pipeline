
-- Ecommerce Data Pipeline
-- PostgreSQL database initialization

-- Orders table
CREATE TABLE IF NOT EXISTS orders (
    order_id INTEGER PRIMARY KEY,
    order_date DATE,
    customer_id INTEGER,
    amount NUMERIC(10, 2),
    status VARCHAR(20),
    is_active BOOLEAN NOT NULL DEFAULT TRUE
);

-- ETL execution history
CREATE TABLE IF NOT EXISTS etl_run_history (
    run_id BIGSERIAL PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    status VARCHAR(20) NOT NULL DEFAULT 'running',
    rows_extracted INTEGER,
    rows_loaded INTEGER,
    error_message TEXT
);

-- =============================================================================
-- Stock Market Data Pipeline - Database Initialization
-- =============================================================================
-- This script creates the stock_prices table and supporting indexes.
-- It is executed automatically when the PostgreSQL container starts for the
-- first time (mounted via /docker-entrypoint-initdb.d/).
-- =============================================================================

-- Create the stock_prices table to hold daily OHLCV market data.
-- The UNIQUE constraint on (symbol, timestamp) makes upserts idempotent,
-- so running the pipeline multiple times will update existing rows rather
-- than creating duplicates.
CREATE TABLE IF NOT EXISTS stock_prices (
    id         SERIAL PRIMARY KEY,
    symbol     VARCHAR(20)     NOT NULL,
    timestamp  DATE            NOT NULL,
    open       NUMERIC(12, 4)  NOT NULL,
    high       NUMERIC(12, 4)  NOT NULL,
    low        NUMERIC(12, 4)  NOT NULL,
    close      NUMERIC(12, 4)  NOT NULL,
    volume     BIGINT          NOT NULL,
    created_at TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    -- Prevents duplicate rows for the same stock on the same date.
    CONSTRAINT uq_stock_prices_symbol_timestamp UNIQUE (symbol, timestamp)
);

-- Index for fast symbol-based lookups.
CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol
    ON stock_prices (symbol);

-- Index for fast date-range queries (most common access pattern).
CREATE INDEX IF NOT EXISTS idx_stock_prices_timestamp
    ON stock_prices (timestamp DESC);

-- Composite index for the most common query pattern: symbol + date range.
CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol_timestamp
    ON stock_prices (symbol, timestamp DESC);

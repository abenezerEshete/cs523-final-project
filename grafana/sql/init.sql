CREATE TABLE IF NOT EXISTS crypto_windowed (
    symbol TEXT NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    avg_price DOUBLE PRECISION NOT NULL,
    min_price DOUBLE PRECISION NOT NULL,
    max_price DOUBLE PRECISION NOT NULL,
    total_volume DOUBLE PRECISION NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, window_start)
);

CREATE INDEX IF NOT EXISTS idx_crypto_windowed_time
    ON crypto_windowed (window_start DESC);

CREATE TABLE IF NOT EXISTS crypto_moving_avg (
    symbol TEXT NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    moving_avg_price DOUBLE PRECISION NOT NULL,
    total_volume DOUBLE PRECISION NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, window_start, window_end)
);

CREATE INDEX IF NOT EXISTS idx_crypto_moving_avg_time
    ON crypto_moving_avg (window_start DESC);

CREATE TABLE IF NOT EXISTS crypto_anomalies (
    symbol TEXT NOT NULL,
    window_start TIMESTAMP NOT NULL,
    avg_price DOUBLE PRECISION NOT NULL,
    max_price DOUBLE PRECISION NOT NULL,
    min_price DOUBLE PRECISION NOT NULL,
    price_range_pct DOUBLE PRECISION NOT NULL,
    is_anomaly BOOLEAN NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, window_start)
);

CREATE INDEX IF NOT EXISTS idx_crypto_anomalies_time
    ON crypto_anomalies (window_start DESC);

CREATE INDEX IF NOT EXISTS idx_crypto_anomalies_flag
    ON crypto_anomalies (is_anomaly, window_start DESC);

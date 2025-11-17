


dim_market_type_ddl = """
CREATE TABLE dw.dim_market_type (
    market_key BIGSERIAL PRIMARY KEY,
    market_type TEXT UNIQUE NOT NULL,       -- HOSE / HNX / UPCOM
    market_name TEXT,                       -- optional
    update_time TIMESTAMPTZ,                -- from Mongo
    symbols_json JSONB,                     -- raw array of symbols from MongoDB
    effective_from DATE DEFAULT CURRENT_DATE,
    effective_to DATE,
    is_current BOOLEAN DEFAULT TRUE
);

CREATE INDEX idx_dim_market_type_type
    ON dw.dim_market_type(market_type);

CREATE INDEX idx_dim_market_type_symbols_gin
    ON dw.dim_market_type USING GIN (symbols_json);
"""
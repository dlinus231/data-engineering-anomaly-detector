USE ROLE TRAINING_ROLE;
USE WAREHOUSE FALCON_WH;
USE DATABASE FALCON_DB;
USE SCHEMA FALCON_SCHEMA;

-- Create the raw_events table
-- Stores every cryptocurrency price snapshot observed by the pipeline
CREATE TABLE IF NOT EXISTS raw_events (
    event_id          VARCHAR,     -- SHA-256 hash of id + price + timestamp, unique record identifier
    id                INTEGER,     -- CoinMarketCap internal coin ID
    name              VARCHAR,     -- Full coin name (e.g. Bitcoin)
    symbol            VARCHAR,     -- Ticker symbol (e.g. BTC)
    price             DOUBLE,      -- Current price in USD
    event_ts          TIMESTAMP,   -- Time of last price update from CoinMarketCap
    volume_24h        DOUBLE,      -- 24-hour trading volume in USD
    volume_change_24h DOUBLE,      -- % change in 24h volume
    percent_change_1h DOUBLE,      -- % price change over last hour (used for anomaly detection)
    percent_change_24h DOUBLE,     -- % price change over last 24 hours
    processing_time   TIMESTAMP,   -- Time the record was processed by Spark
    event_dt          DATE         -- Partition column (event date)
);

-- Create the raw_alerts table
-- Stores one record per detected anomaly, references raw_events via event_id
CREATE TABLE IF NOT EXISTS raw_alerts (
    alert_id          VARCHAR,     -- Unique alert identifier
    event_id          VARCHAR,     -- Foreign key referencing raw_events.event_id
    id                INTEGER,     -- CoinMarketCap internal coin ID
    event_ts          TIMESTAMP,   -- Time of the anomalous event
    curr_price        DOUBLE,      -- Price at time of alert
    past_prices       VARCHAR,     -- Serialized historical prices used in z-score computation
    past_timestamps   VARCHAR,     -- Serialized timestamps for historical window
    mean              DOUBLE,      -- Rolling mean

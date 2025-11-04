CREATE DATABASE IF NOT EXISTS aura;

CREATE TABLE IF NOT EXISTS aura.logs (
    timestamp DateTime64(9),
    id String,
    service_name String,
    level String,
    message String,
    attributes Map(String, String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (service_name, level, timestamp);
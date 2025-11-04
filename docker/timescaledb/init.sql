CREATE DATABASE aura;

\c aura

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS metrics (
    timestamp TIMESTAMPTZ NOT NULL,
    name TEXT NOT NULL,
    value DOUBLE PRECISION,
    attributes JSONB
);

SELECT create_hypertable('metrics', 'timestamp');
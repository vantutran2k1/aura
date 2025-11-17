CREATE TABLE IF NOT EXISTS aura.traces (
    timestamp DateTime64(9),
    trace_id FixedString(16),
    span_id FixedString(8),
    parent_span_id FixedString(8),
    name String,
    duration_ns UInt64,
    attributes Map(String, String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (trace_id, timestamp);
\c aura

CREATE TABLE IF NOT EXISTS alert_rules (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    query TEXT NOT NULL,
    threshold INT NOT NULL,
    interval_seconds INT NOT NULL,
    
    current_state TEXT NOT NULL DEFAULT 'OK',
    last_checked_at TIMESTAMPTZ,
    last_fired_at TIMESTAMPTZ,
    
    notification_target_type TEXT NOT NULL,
    notification_target_url TEXT NOT NULL
);
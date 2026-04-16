
-- TalentScope AI — Analytics events table
-- Tracks page views and feature usage for user validation.
-- Anonymous: session_id is client-generated, no PII.

CREATE TABLE IF NOT EXISTS analytics_events (
    id            BIGSERIAL PRIMARY KEY,
    session_id    VARCHAR(64)  NOT NULL,
    event_type    VARCHAR(50)  NOT NULL,   -- 'page_view', 'feature_use', 'error'
    event_name    VARCHAR(100) NOT NULL,   -- e.g. 'market_dashboard', 'salary_predict_clicked'
    properties    JSONB,                    -- optional structured context (role, city, etc.)
    created_at    TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_analytics_event_name ON analytics_events(event_name);
CREATE INDEX IF NOT EXISTS idx_analytics_created_at ON analytics_events(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_analytics_session    ON analytics_events(session_id);

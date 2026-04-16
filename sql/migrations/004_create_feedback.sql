-- TalentScope AI — User feedback table
-- Lightweight thumbs-up/down + optional comment per page/feature.
-- Linked to analytics_events via session_id for cohort analysis.

CREATE TABLE IF NOT EXISTS user_feedback (
    id              BIGSERIAL PRIMARY KEY,
    session_id      VARCHAR(64)  NOT NULL,
    page_name       VARCHAR(100) NOT NULL,   -- 'market_dashboard', 'salary_predictor', etc.
    rating          SMALLINT,                 -- 1 = thumbs up, -1 = thumbs down, NULL = text only
    comment         TEXT,                     -- optional free text
    context         JSONB,                    -- optional snapshot (e.g. filters applied)
    created_at      TIMESTAMP    NOT NULL DEFAULT NOW(),
    CONSTRAINT rating_range CHECK (rating IS NULL OR rating IN (-1, 1))
);

CREATE INDEX IF NOT EXISTS idx_feedback_page       ON user_feedback(page_name);
CREATE INDEX IF NOT EXISTS idx_feedback_created_at ON user_feedback(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_feedback_session    ON user_feedback(session_id);
CREATE INDEX IF NOT EXISTS idx_feedback_rating     ON user_feedback(rating);
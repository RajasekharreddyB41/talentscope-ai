-- TalentScope AI — Add OPT classification columns to clean_jobs
-- Supports the 3-signal Jobright-style OPT classifier.

ALTER TABLE clean_jobs ADD COLUMN IF NOT EXISTS opt_status VARCHAR(20) DEFAULT 'unknown';
ALTER TABLE clean_jobs ADD COLUMN IF NOT EXISTS opt_signals TEXT[];
ALTER TABLE clean_jobs ADD COLUMN IF NOT EXISTS opt_confidence VARCHAR(10) DEFAULT 'low';
ALTER TABLE clean_jobs ADD COLUMN IF NOT EXISTS h1b_sponsorship BOOLEAN;
ALTER TABLE clean_jobs ADD COLUMN IF NOT EXISTS sponsor_tier VARCHAR(10) DEFAULT 'none';
ALTER TABLE clean_jobs ADD COLUMN IF NOT EXISTS h1b_approvals INTEGER DEFAULT 0;

-- Index for fast filtering on Browse Jobs page
CREATE INDEX IF NOT EXISTS idx_clean_jobs_opt_status ON clean_jobs(opt_status);
CREATE INDEX IF NOT EXISTS idx_clean_jobs_sponsor_tier ON clean_jobs(sponsor_tier);

COMMENT ON COLUMN clean_jobs.opt_status IS 'opt_friendly | opt_unclear | not_opt_friendly | unknown';
COMMENT ON COLUMN clean_jobs.opt_confidence IS 'high | medium | low';
COMMENT ON COLUMN clean_jobs.sponsor_tier IS 'gold | silver | none — based on USCIS H-1B employer data';
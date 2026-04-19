ALTER TABLE lobbying_registrations
  ADD COLUMN IF NOT EXISTS general_issue_codes TEXT[];

ALTER TABLE lobbying_registrations
  ADD COLUMN IF NOT EXISTS specific_issues_tsv tsvector
  GENERATED ALWAYS AS (
    to_tsvector('english', coalesce(specific_issues, ''))
  ) STORED;

CREATE INDEX IF NOT EXISTS idx_specific_issues_fts
  ON lobbying_registrations USING GIN (specific_issues_tsv);

CREATE INDEX IF NOT EXISTS idx_general_issue_codes
  ON lobbying_registrations USING GIN (general_issue_codes);

CREATE TABLE IF NOT EXISTS ingestion_runs (
  id SERIAL PRIMARY KEY,
  source TEXT NOT NULL,
  last_filing_uuid TEXT,
  last_page INTEGER,
  records_processed INTEGER DEFAULT 0,
  started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  status TEXT DEFAULT 'running'
);

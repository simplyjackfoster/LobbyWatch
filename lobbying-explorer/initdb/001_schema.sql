-- Core entities
CREATE TABLE IF NOT EXISTS organizations (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  name_normalized TEXT NOT NULL UNIQUE,
  type TEXT,
  industry_code TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS legislators (
  id SERIAL PRIMARY KEY,
  bioguide_id TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL,
  party TEXT,
  state TEXT,
  chamber TEXT,
  is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS committees (
  id SERIAL PRIMARY KEY,
  committee_id TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL,
  chamber TEXT,
  subcommittee_of TEXT
);

CREATE TABLE IF NOT EXISTS lobbyists (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  name_normalized TEXT NOT NULL,
  lda_id TEXT UNIQUE
);

-- Relationships
CREATE TABLE IF NOT EXISTS lobbying_registrations (
  id SERIAL PRIMARY KEY,
  registrant_id INTEGER REFERENCES organizations(id),
  client_id INTEGER REFERENCES organizations(id),
  filing_uuid TEXT UNIQUE,
  filing_year INTEGER,
  filing_period TEXT,
  amount NUMERIC,
  issue_codes TEXT[],
  specific_issues TEXT
);

CREATE TABLE IF NOT EXISTS lobbying_lobbyists (
  registration_id INTEGER REFERENCES lobbying_registrations(id),
  lobbyist_id INTEGER REFERENCES lobbyists(id),
  PRIMARY KEY (registration_id, lobbyist_id)
);

CREATE TABLE IF NOT EXISTS contributions (
  id SERIAL PRIMARY KEY,
  contributor_org_id INTEGER REFERENCES organizations(id),
  recipient_legislator_id INTEGER REFERENCES legislators(id),
  amount NUMERIC NOT NULL,
  contribution_date DATE,
  fec_committee_id TEXT,
  cycle INTEGER
);

CREATE TABLE IF NOT EXISTS committee_memberships (
  legislator_id INTEGER REFERENCES legislators(id),
  committee_id INTEGER REFERENCES committees(id),
  role TEXT,
  PRIMARY KEY (legislator_id, committee_id)
);

CREATE TABLE IF NOT EXISTS votes (
  id SERIAL PRIMARY KEY,
  legislator_id INTEGER REFERENCES legislators(id),
  bill_id TEXT,
  bill_title TEXT,
  vote_position TEXT,
  vote_date DATE,
  congress INTEGER,
  issue_tags TEXT[]
);

CREATE INDEX IF NOT EXISTS idx_org_name_normalized ON organizations(name_normalized);
CREATE INDEX IF NOT EXISTS idx_lobbyist_name_normalized ON lobbyists(name_normalized);
CREATE INDEX IF NOT EXISTS idx_contributions_contributor ON contributions(contributor_org_id);
CREATE INDEX IF NOT EXISTS idx_contributions_recipient ON contributions(recipient_legislator_id);
CREATE INDEX IF NOT EXISTS idx_lobbying_client ON lobbying_registrations(client_id);
CREATE INDEX IF NOT EXISTS idx_lobbying_registrant ON lobbying_registrations(registrant_id);

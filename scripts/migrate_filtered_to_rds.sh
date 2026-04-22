#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: $0 <source_postgres_url> <target_postgres_url> [year_min] [congress_min]"
  exit 1
fi

SOURCE_URL="$1"
TARGET_URL="$2"
YEAR_MIN="${3:-$(date +%Y -v-1 2>/dev/null || python3 - <<'PY'
from datetime import datetime
print(datetime.utcnow().year - 1)
PY
)}"
CONGRESS_MIN="${4:-118}"
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

export PGPASSWORD=""

echo "==> Applying schema to target"
psql "${TARGET_URL}" -v ON_ERROR_STOP=1 -f "${ROOT_DIR}/pipeline/001_schema.sql"
psql "${TARGET_URL}" -v ON_ERROR_STOP=1 -f "${ROOT_DIR}/pipeline/002_fts_and_issues.sql"

echo "==> Truncating target tables"
psql "${TARGET_URL}" -v ON_ERROR_STOP=1 <<SQL
TRUNCATE TABLE
  lobbying_lobbyists,
  committee_memberships,
  lobbyist_contributions,
  co_sponsorships,
  votes,
  contributions,
  lobbying_registrations,
  lobbyists,
  committees,
  legislators,
  organizations,
  ingestion_runs
RESTART IDENTITY CASCADE;
SQL

copy_all() {
  local table="$1"
  local file="${TMP_DIR}/${table}.csv"
  echo "  - ${table}"
  psql "${SOURCE_URL}" -v ON_ERROR_STOP=1 -c "\\copy ${table} TO '${file}' CSV"
  psql "${TARGET_URL}" -v ON_ERROR_STOP=1 -c "\\copy ${table} FROM '${file}' CSV"
}

copy_filtered() {
  local table="$1"
  local where_clause="$2"
  local file="${TMP_DIR}/${table}.csv"
  echo "  - ${table} (filtered)"
  psql "${SOURCE_URL}" -v ON_ERROR_STOP=1 -c "\\copy (SELECT * FROM ${table} WHERE ${where_clause}) TO '${file}' CSV"
  psql "${TARGET_URL}" -v ON_ERROR_STOP=1 -c "\\copy ${table} FROM '${file}' CSV"
}

echo "==> Copying dimension/small tables"
copy_all organizations
copy_all legislators
copy_all committees
copy_all committee_memberships
copy_all lobbyists
copy_all lobbying_lobbyists
copy_all ingestion_runs

echo "==> Copying filtered fact tables (year >= ${YEAR_MIN}, congress >= ${CONGRESS_MIN})"
copy_filtered lobbying_registrations "filing_year >= ${YEAR_MIN}"
copy_filtered contributions "contribution_date IS NULL OR EXTRACT(YEAR FROM contribution_date) >= ${YEAR_MIN}"
copy_filtered votes "congress >= ${CONGRESS_MIN}"
copy_filtered co_sponsorships "congress >= ${CONGRESS_MIN}"
copy_filtered lobbyist_contributions "filing_year >= ${YEAR_MIN}"

echo "==> Rebuilding indexes/stats"
psql "${TARGET_URL}" -v ON_ERROR_STOP=1 -c "ANALYZE"

echo "==> Row count verification"
psql "${TARGET_URL}" -v ON_ERROR_STOP=1 -c "
SELECT 'organizations' tbl, COUNT(*) FROM organizations
UNION ALL SELECT 'legislators', COUNT(*) FROM legislators
UNION ALL SELECT 'committees', COUNT(*) FROM committees
UNION ALL SELECT 'committee_memberships', COUNT(*) FROM committee_memberships
UNION ALL SELECT 'lobbying_registrations', COUNT(*) FROM lobbying_registrations
UNION ALL SELECT 'contributions', COUNT(*) FROM contributions
UNION ALL SELECT 'votes', COUNT(*) FROM votes
UNION ALL SELECT 'co_sponsorships', COUNT(*) FROM co_sponsorships
UNION ALL SELECT 'lobbyist_contributions', COUNT(*) FROM lobbyist_contributions;
"

echo "Migration completed."

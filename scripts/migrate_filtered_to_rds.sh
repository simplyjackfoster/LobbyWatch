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
DO \$\$
DECLARE
  tbl text;
BEGIN
  FOREACH tbl IN ARRAY ARRAY[
    'lobbying_lobbyists',
    'committee_memberships',
    'co_sponsorships',
    'votes',
    'contributions',
    'lobbying_registrations',
    'lobbyists',
    'committees',
    'legislators',
    'organizations',
    'ingestion_runs'
  ]
  LOOP
    IF to_regclass('public.' || tbl) IS NOT NULL THEN
      EXECUTE format('TRUNCATE TABLE %I RESTART IDENTITY CASCADE', tbl);
    END IF;
  END LOOP;
END
\$\$;
SQL

table_exists() {
  local db_url="$1"
  local table="$2"
  local exists
  exists=$(psql "${db_url}" -tA -v ON_ERROR_STOP=1 -c "SELECT to_regclass('public.${table}') IS NOT NULL;")
  [[ "${exists}" == "t" ]]
}

common_columns_csv() {
  local table="$1"
  local src_cols tgt_cols col csv
  src_cols=$(psql "${SOURCE_URL}" -tA -v ON_ERROR_STOP=1 -c "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='${table}' ORDER BY ordinal_position;")
  tgt_cols=$(psql "${TARGET_URL}" -tA -v ON_ERROR_STOP=1 -c "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='${table}' AND is_generated = 'NEVER' ORDER BY ordinal_position;")
  csv=""
  while IFS= read -r col; do
    [[ -z "${col}" ]] && continue
    if grep -qx "${col}" <<< "${src_cols}"; then
      if [[ -n "${csv}" ]]; then
        csv="${csv}, "
      fi
      csv="${csv}\"${col}\""
    fi
  done <<< "${tgt_cols}"
  echo "${csv}"
}

copy_all() {
  local table="$1"
  local file="${TMP_DIR}/${table}.csv"
  if ! table_exists "${SOURCE_URL}" "${table}"; then
    echo "  - ${table} (skip: missing in source)"
    return
  fi
  if ! table_exists "${TARGET_URL}" "${table}"; then
    echo "  - ${table} (skip: missing in target)"
    return
  fi
  local cols
  cols=$(common_columns_csv "${table}")
  if [[ -z "${cols}" ]]; then
    echo "  - ${table} (skip: no common columns)"
    return
  fi
  echo "  - ${table}"
  psql "${SOURCE_URL}" -v ON_ERROR_STOP=1 -c "\\copy (SELECT ${cols} FROM ${table}) TO '${file}' CSV"
  psql "${TARGET_URL}" -v ON_ERROR_STOP=1 -c "\\copy ${table} (${cols}) FROM '${file}' CSV"
}

copy_filtered() {
  local table="$1"
  local where_clause="$2"
  local file="${TMP_DIR}/${table}.csv"
  if ! table_exists "${SOURCE_URL}" "${table}"; then
    echo "  - ${table} (skip: missing in source)"
    return
  fi
  if ! table_exists "${TARGET_URL}" "${table}"; then
    echo "  - ${table} (skip: missing in target)"
    return
  fi
  local cols
  cols=$(common_columns_csv "${table}")
  if [[ -z "${cols}" ]]; then
    echo "  - ${table} (skip: no common columns)"
    return
  fi
  echo "  - ${table} (filtered)"
  psql "${SOURCE_URL}" -v ON_ERROR_STOP=1 -c "\\copy (SELECT ${cols} FROM ${table} WHERE ${where_clause}) TO '${file}' CSV"
  psql "${TARGET_URL}" -v ON_ERROR_STOP=1 -c "\\copy ${table} (${cols}) FROM '${file}' CSV"
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
UNION ALL SELECT 'votes', COUNT(*) FROM votes;
"

echo "Migration completed."

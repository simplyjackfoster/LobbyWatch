# Data Quality, Pipeline Automation & Trust UX

**Date:** 2026-04-25
**Status:** Approved

## Problem

Two related gaps:

1. **Pipeline gap**: The Lambda worker ingests fresh data into Postgres every 12 hours, but the SQLite export and GitHub Release upload are manual steps. There is no automated path from "ingest complete" to "users can get updated data."

2. **Trust gap**: Users have no signal about where data comes from or how old it is. The risk is that data looks hallucinated or unverifiable.

---

## Architecture: Automated Export Pipeline

### Current flow

```
EventBridge (12h) → SQS (ingest-queue) → Worker Lambda → Postgres
```

SQLite export and GitHub Release upload are manual thereafter.

### New flow

```
EventBridge (12h)
  → SQS (ingest-queue)
    → Worker Lambda (ingest into Postgres)
          │ on success
          ▼
        SQS (export-queue)
          → Export Lambda → GitHub Releases (lobbywatch.db.zst)
```

SQS is used (not SNS or direct invocation) because:
- Consistent with the existing pipeline pattern
- Message persists and retries automatically if Export Lambda fails
- DLQ depth is a CloudWatch metric — trivial to alarm on
- Gives visibility into export failures without extra config

### Worker Lambda changes (`lambda_worker.py`)

At the end of `_run_scheduled_ingest`, on success:
1. Write coverage stats to a new `_pipeline_meta` Postgres table:
   - `last_ingest_at` (timestamp)
   - `lda_coverage_through` (date — derived from max `filing_year`/`filing_period` in `lobbying_registrations`)
   - `congress_coverage_through` (date — derived from max `vote_date` or legislator sync timestamp)
2. Publish `{ "task": "export_and_release" }` to the export SQS queue.

### New Export Lambda (`lambda_export.py`)

Triggered by SQS export-queue. Steps:

1. Read Postgres → build SQLite (reuses core logic from `export_sqlite.py`, extracted into a shared function)
2. Compress with zstd level 9 (not level 22 — level 22 is too CPU-intensive for Lambda; level 9 gives good compression at ~10x the speed)
3. Upload `lobbywatch.db.zst` to GitHub Releases via GitHub API — creates a new release tagged `data-YYYY-MM-DD` and marks it as the latest release. This preserves the existing `lobbywatch update` URL (`releases/latest/download/lobbywatch.db.zst`) without change.
4. Write `last_exported_at` to `_pipeline_meta` in Postgres

GitHub PAT stored in SSM as `/lobbywatch/prod/github_pat`. IAM role for Export Lambda gets `ssm:GetParameter` on that path.

### `export_sqlite.py` changes

Core export logic extracted from `main()` into an importable `build_and_compress(pg_conn, output_path, level=22)` function. The `main()` script and the Lambda both call this function. `export_sqlite.py` retains level 22 for manual/local use (slower but max compression); the Lambda uses level 9.

### `_pipeline_meta` table (Postgres)

```sql
CREATE TABLE _pipeline_meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);
```

Keys: `last_ingest_at`, `last_exported_at`, `lda_coverage_through`, `congress_coverage_through`.

### Terraform additions

- New Lambda function: `lobbywatch-prod-export`
- New SQS queue: `lobbywatch-prod-export-queue` with DLQ
- SQS event source mapping for the Export Lambda
- SSM parameter: `/lobbywatch/prod/github_pat` (value set manually once)
- CloudWatch alarm on DLQ depth > 0 → SNS email notification

### `_meta` table changes (SQLite export)

Two new keys added at export time, populated from `_pipeline_meta`:
- `lda_coverage_through`
- `congress_coverage_through`

---

## API: `/meta/data-status`

New endpoint in `main.py`:

```
GET /meta/data-status
```

Reads from `_pipeline_meta` and returns:

```json
{
  "last_exported_at": "2026-04-25T06:12:00",
  "last_ingest_at": "2026-04-25T06:00:00",
  "lda_coverage_through": "2026-03-31",
  "congress_coverage_through": "2026-04-24"
}
```

Returns 200 with null values if `_pipeline_meta` has no rows yet (first deploy). No auth required — same as `/health`.

---

## Frontend Trust Signals

### Ambient layer (always visible)

**Masthead timestamp**: The existing `"LIVE DATA"` dot and text is replaced by an `"Updated [X days ago]"` string once `GET /meta/data-status` resolves on app load. While loading, shows the existing dot. On error, falls back to the current static "LIVE DATA" text — no broken UI.

**Masthead sources line**: `"Senate LDA · FEC · Congress.gov"` becomes a clickable element that navigates to `/about-data`. No visual change other than cursor and nav handler — present but not intrusive.

**InfoPanel source badges**: Data fields in `InfoPanel.jsx` get small clickable `↗` source links that open the canonical government record in a new tab:

| Data | Badge | URL pattern |
|------|-------|-------------|
| Lobbying registration | `LDA ↗` | `https://lda.gov/filings/{filing_uuid}/` |
| Legislator | `Congress.gov ↗` | `https://www.congress.gov/member/{bioguide_id}` |
| Contribution | `FEC ↗` | `https://www.fec.gov/data/committee/{fec_committee_id}/` |
| Bill / vote | `Congress.gov ↗` | Constructable from `bill_id` — exact format to be verified against DB during implementation (likely `https://www.congress.gov/bill/{congress}th-congress/{chamber}-bill/{number}`) |
| Lobbyist | `LDA ↗` | `https://lda.gov/lobbyists/{lda_id}/` |

Badges are monospace pill style, muted, consistent with the existing aesthetic. Users can click through to verify the raw government record directly.

### About the Data page (`/about-data`)

New `AboutData.jsx` page, same pattern as `Developers.jsx`. Linked from the masthead sources line. Sections:

1. **What is LobbyWatch?** — one factual paragraph
2. **Data Sources** — three blocks (LDA, FEC, Congress.gov): what the source is, what LobbyWatch pulls from it, and a link to the official API documentation
3. **Coverage & Freshness** — live data from `/meta/data-status`: last updated, LDA coverage through, Congress data through. Shows "checking..." while loading.
4. **Methodology notes** — how orgs are deduplicated, what "amount" means (LDA-reported spend), known gaps (FEC data lags ~30 days, some older LDA filings have no spend reported)

`App.jsx` gets a new `about-data` route and an additional utility link in the masthead utility bar alongside "Developers / CLI Docs".

---

## CLI: Enhanced `lobbywatch status`

`lobbywatch status` reads `lda_coverage_through` and `congress_coverage_through` from the `_meta` table (same query path as `exported_at`) and includes them in output:

```json
{
  "ok": true,
  "exported_at": "2026-04-25T06:12:00",
  "lda_coverage_through": "2026-03-31",
  "congress_coverage_through": "2026-04-24",
  "db_path": "~/.lobbywatch/lobbywatch.db"
}
```

No new commands. No structural changes to `db.py` — keys are read from `_meta` the same way `exported_at` already is. Keys are omitted from output if not present in the database (older bundled snapshot).

---

## Key constraints

- Export Lambda zstd level 9 (not 22) to stay well within 15-min Lambda timeout
- GitHub PAT set manually in SSM — not in Terraform state
- `/meta/data-status` returns gracefully with null values if `_pipeline_meta` is empty
- Frontend masthead degrades gracefully if `/meta/data-status` fails
- CLI `status` omits new keys silently for older snapshots (no breaking change)

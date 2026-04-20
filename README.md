# LobbyWatch

LobbyWatch is a full-stack data + visualization application that helps users explore lobbying influence networks around organizations, legislators, committees, and issue areas.

It combines:
- Senate LDA data (lobbying filings, issues, registrants, lobbyists)
- OpenFEC data (campaign finance receipts/disbursements)
- Congress.gov data (members, committees, votes)

## Prerequisites

- Docker Desktop
- Python 3.11+
- Node.js 18+
- API keys:
1. Senate LDA API key: [https://lda.gov/api/](https://lda.gov/api/)
2. OpenFEC API key (Data.gov): [https://api.data.gov/signup/](https://api.data.gov/signup/)
3. Congress.gov API key: [https://api.congress.gov/sign-up/](https://api.congress.gov/sign-up/)

## Project Structure

- `pipeline/`: DB migrations and ingestion scripts
- `backend/`: FastAPI API server
- `frontend/`: React + Vite client
- `docker-compose.yml`: local multi-service orchestration

## Setup (Exact Order)

1. Clone the repo.

2. Copy environment file and fill all values:
```bash
cp .env.example .env
```
Set at minimum:
- `DATABASE_URL`
- `LDA_API_KEY`
- `FEC_API_KEY`
- `CONGRESS_API_KEY`

3. Start PostgreSQL:
```bash
docker compose up -d db
```

4. Apply migrations:
```bash
psql "$DATABASE_URL" -f pipeline/001_schema.sql
psql "$DATABASE_URL" -f pipeline/002_fts_and_issues.sql
```

5. Install pipeline dependencies:
```bash
pip install -r pipeline/requirements.txt
```

6. Run ingestion in order:
```bash
python3 pipeline/ingest_congress.py
python3 pipeline/ingest_lda.py --years 2023 2024 2025
python3 pipeline/dedup_orgs.py
python3 pipeline/ingest_fec.py --cycles 2024 --min-amount 1000
python3 pipeline/ingest_lda_contributions.py --years 2023 2024 2025
python3 pipeline/rebuild_indexes.py
```

7. New analysis endpoints:
```bash
GET /analysis/betrayal-index?issue_code=HLTH
GET /analysis/revolving-door?agency=FDA
GET /analysis/foreign-influence?country=CN
```

8. Start full stack:
```bash
docker compose up --build
```

9. Open the app:
- Frontend: `http://localhost:5173`
- Backend API: `http://localhost:8000`

The LDA API migrated from `lda.senate.gov` to `lda.gov` in 2026.  
All pipeline code uses `lda.gov/api/v1/` as of this version.

## Ingestion Run Tracking

The pipeline writes progress to `ingestion_runs`:
- `source`: `lda`, `congress`, `fec`
- `last_page`: last completed API page
- `records_processed`: running total
- `status`: `running`, `complete`, `failed`

Useful checks:
```sql
SELECT id, source, status, last_page, records_processed, started_at, completed_at
FROM ingestion_runs
ORDER BY id DESC
LIMIT 20;
```

```sql
SELECT source, status, COUNT(*)
FROM ingestion_runs
GROUP BY source, status
ORDER BY source, status;
```

## Known Limitations

- Contribution-to-organization matching is approximate and normalization-based.
- LDA `specific_issues` text quality is inconsistent and may be sparse/noisy.
- By default ingestion scope is focused on recent years; no data before 2019 is ingested unless you expand run parameters.

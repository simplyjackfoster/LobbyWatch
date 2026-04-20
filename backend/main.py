from typing import Optional

from fastapi import Depends, FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from sqlalchemy.orm import Session

from graph import get_entity_summary, get_issue_graph, get_legislator_graph, get_organization_graph
from models import SessionLocal
from search import search_entities

app = FastAPI(title="LobbyWatch API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


AGENCY_ISSUE_MAP = {
    "FDA": {"HLTH"},
    "HHS": {"HLTH"},
    "CMS": {"HLTH"},
    "NIH": {"HLTH"},
    "EPA": {"ENV", "ENRG"},
    "SEC": {"FIN"},
    "FTC": {"FIN", "TRAD"},
    "DOD": {"DEF"},
    "DOE": {"ENRG"},
    "USDA": {"AGR"},
    "DOT": {"TRN", "TRAD"},
    "FCC": {"TEC", "TRAD"},
}


def compute_issue_relevance(
    agency: Optional[str],
    issue_codes: set[str],
    issue_code_filter: Optional[str],
) -> float:
    relevance = 1.0
    if issue_code_filter and issue_code_filter in issue_codes:
        relevance += 0.5
    if agency:
        mapped = AGENCY_ISSUE_MAP.get(agency.upper(), set())
        if mapped and issue_codes.intersection(mapped):
            relevance += 0.5
    return relevance


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/search")
def search(q: str = Query(..., min_length=1), db: Session = Depends(get_db)):
    return search_entities(db, q=q)


@app.get("/meta/issue-codes")
def issue_codes(db: Session = Depends(get_db)):
    rows = db.execute(
        text(
            """
            SELECT DISTINCT code
            FROM lobbying_registrations,
            LATERAL unnest(general_issue_codes) AS code
            WHERE code IS NOT NULL AND code <> ''
            ORDER BY code
            LIMIT 200
            """
        )
    ).all()
    return {"issue_codes": [r.code for r in rows]}


@app.get("/graph/organization/{id}")
def graph_org(
    id: int,
    year_min: Optional[int] = None,
    year_max: Optional[int] = None,
    issue_code: Optional[str] = None,
    min_contribution: Optional[float] = None,
    node_limit: int = Query(default=50, ge=10, le=200),
    db: Session = Depends(get_db),
):
    return get_organization_graph(
        db,
        org_id=id,
        year_min=year_min,
        year_max=year_max,
        issue_code=issue_code,
        min_contribution=min_contribution,
        max_nodes=node_limit,
    )


@app.get("/graph/legislator/{bioguide_id}")
def graph_legislator(
    bioguide_id: str,
    year_min: Optional[int] = None,
    year_max: Optional[int] = None,
    min_contribution: Optional[float] = None,
    node_limit: int = Query(default=50, ge=10, le=200),
    db: Session = Depends(get_db),
):
    return get_legislator_graph(
        db,
        bioguide_id=bioguide_id,
        year_min=year_min,
        year_max=year_max,
        min_contribution=min_contribution,
        max_nodes=node_limit,
    )


@app.get("/graph/issue")
def graph_issue(
    q: str = Query(..., min_length=2),
    year_min: Optional[int] = None,
    year_max: Optional[int] = None,
    min_contribution: Optional[float] = None,
    node_limit: int = Query(default=50, ge=10, le=200),
    db: Session = Depends(get_db),
):
    return get_issue_graph(
        db,
        q=q,
        year_min=year_min,
        year_max=year_max,
        min_contribution=min_contribution,
        max_nodes=node_limit,
    )


@app.get("/entity/{entity_type}/{entity_id}/summary")
def entity_summary(entity_type: str, entity_id: str, db: Session = Depends(get_db)):
    return get_entity_summary(db, entity_type=entity_type, entity_id=entity_id)


@app.get("/analysis/betrayal-index")
def betrayal_index(
    issue_code: str = Query(default="HLTH"),
    min_contribution: int = Query(default=10000),
    contribution_window_days: int = Query(default=365),
    db: Session = Depends(get_db),
):
    legislators = db.execute(
        text(
            """
            SELECT DISTINCT
              l.id AS legislator_id,
              l.name,
              l.party,
              l.state
            FROM legislators l
            JOIN co_sponsorships cs ON cs.legislator_id = l.id
            """
        )
    ).all()

    findings = []
    max_contrib = 0.0

    for leg in legislators:
        bills = db.execute(
            text(
                """
                SELECT bill_id, bill_title
                FROM co_sponsorships
                WHERE legislator_id = :legislator_id
                """
            ),
            {"legislator_id": leg.legislator_id},
        ).all()
        co_count = len(bills)
        if co_count == 0:
            continue

        contrib_rows = db.execute(
            text(
                """
                SELECT o.name, SUM(c.amount) AS amount
                FROM contributions c
                JOIN organizations o ON o.id = c.contributor_org_id
                JOIN lobbying_registrations lr
                  ON (lr.client_id = o.id OR lr.registrant_id = o.id)
                JOIN co_sponsorships cs ON cs.legislator_id = c.recipient_legislator_id
                WHERE c.recipient_legislator_id = :legislator_id
                  AND :issue_code = ANY(lr.general_issue_codes)
                  AND c.contribution_date >= cs.introduced_date
                  AND c.contribution_date <= cs.introduced_date + (:window_days::text || ' days')::interval
                GROUP BY o.name
                ORDER BY amount DESC
                """
            ),
            {
                "legislator_id": leg.legislator_id,
                "issue_code": issue_code.upper(),
                "window_days": contribution_window_days,
            },
        ).all()
        total_contrib = float(sum(float(r.amount or 0) for r in contrib_rows))
        if total_contrib <= min_contribution:
            continue

        negative_votes = db.execute(
            text(
                """
                SELECT bill_id, vote_position AS position, vote_date::text AS date
                FROM votes
                WHERE legislator_id = :legislator_id
                  AND vote_position IN ('Nay', 'Not Voting')
                  AND bill_id IN (
                    SELECT bill_id
                    FROM co_sponsorships
                    WHERE legislator_id = :legislator_id
                  )
                ORDER BY vote_date DESC
                """
            ),
            {"legislator_id": leg.legislator_id},
        ).all()
        if not negative_votes:
            continue

        max_contrib = max(max_contrib, total_contrib)
        findings.append(
            {
                "legislator": {"name": leg.name, "party": leg.party, "state": leg.state},
                "co_sponsored_bills": [{"bill_id": b.bill_id, "title": b.bill_title} for b in bills[:10]],
                "contributions_after_cosponsor": total_contrib,
                "contributing_orgs": [{"name": r.name, "amount": float(r.amount or 0)} for r in contrib_rows[:10]],
                "negative_votes": [{"bill_id": v.bill_id, "position": v.position, "date": v.date} for v in negative_votes[:10]],
                "_co_count": co_count,
                "_neg_count": len(negative_votes),
            }
        )

    for f in findings:
        normalized = (f["contributions_after_cosponsor"] / max_contrib) if max_contrib > 0 else 0
        f["betrayal_score"] = normalized * (f["_neg_count"] / max(f["_co_count"], 1))
        del f["_co_count"]
        del f["_neg_count"]

    findings.sort(key=lambda x: x["betrayal_score"], reverse=True)
    return {"findings": findings}


@app.get("/analysis/revolving-door")
def revolving_door(
    agency: Optional[str] = None,
    issue_code: Optional[str] = None,
    limit: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    issue_filter = issue_code.upper() if issue_code else None

    rows = db.execute(
        text(
            """
            SELECT
              l.id AS lobbyist_id,
              l.name AS lobbyist_name,
              l.lda_id AS lda_id,
              l.covered_positions AS covered_positions,
              lr.id AS registration_id,
              lr.general_issue_codes AS general_issue_codes,
              reg_org.name AS registrant_name,
              cli_org.name AS client_name
            FROM lobbyists l
            JOIN lobbying_lobbyists ll ON ll.lobbyist_id = l.id
            JOIN lobbying_registrations lr ON lr.id = ll.registration_id
            LEFT JOIN organizations reg_org ON reg_org.id = lr.registrant_id
            LEFT JOIN organizations cli_org ON cli_org.id = lr.client_id
            WHERE l.has_covered_position = TRUE
              AND (CAST(:issue_code AS text) IS NULL OR CAST(:issue_code AS text) = ANY(lr.general_issue_codes))
            """
        ),
        {"issue_code": issue_filter},
    ).all()

    grouped = {}
    for row in rows:
        positions = row.covered_positions or []
        if agency and not any(agency.lower() in str(position or "").lower() for position in positions):
            continue

        item = grouped.setdefault(
            row.lobbyist_id,
            {
                "lobbyist": {"name": row.lobbyist_name, "lda_id": row.lda_id},
                "prior_positions": positions,
                "registrant_counts": {},
                "clients": set(),
                "issue_codes": set(),
                "registration_ids": set(),
            },
        )
        item["registration_ids"].add(row.registration_id)
        if row.registrant_name:
            item["registrant_counts"][row.registrant_name] = item["registrant_counts"].get(row.registrant_name, 0) + 1
        if row.client_name:
            item["clients"].add(row.client_name)
        for code in row.general_issue_codes or []:
            if code:
                item["issue_codes"].add(code)

    findings = []
    max_raw_score = 0.0
    for item in grouped.values():
        filing_count = len(item["registration_ids"])
        issue_codes = set(item["issue_codes"])
        issue_relevance = compute_issue_relevance(agency, issue_codes, issue_filter)
        raw_score = float(filing_count) * float(issue_relevance)
        max_raw_score = max(max_raw_score, raw_score)

        current_registrant = None
        if item["registrant_counts"]:
            current_registrant = max(item["registrant_counts"], key=item["registrant_counts"].get)

        findings.append(
            {
                "lobbyist": item["lobbyist"],
                "prior_positions": item["prior_positions"],
                "current_registrant": current_registrant,
                "clients": sorted(item["clients"])[:8],
                "issue_codes": sorted(issue_codes)[:10],
                "filing_count": filing_count,
                "_raw_score": raw_score,
            }
        )

    findings.sort(key=lambda x: x["_raw_score"], reverse=True)
    normalized = []
    divisor = max_raw_score or 1.0
    for row in findings[:limit]:
        score = row["_raw_score"] / divisor
        normalized.append(
            {
                "lobbyist": row["lobbyist"],
                "prior_positions": row["prior_positions"],
                "current_registrant": row["current_registrant"],
                "clients": row["clients"],
                "issue_codes": row["issue_codes"],
                "filing_count": row["filing_count"],
                "revolving_door_score": round(score, 2),
            }
        )
    return {"findings": normalized}


@app.get("/analysis/foreign-influence")
def foreign_influence(
    country: Optional[str] = None,
    issue_code: Optional[str] = None,
    limit: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    issue_filter = issue_code.upper() if issue_code else None
    country_filter = country.upper() if country else None

    rows = db.execute(
        text(
            """
            SELECT
              lr.id AS registration_id,
              lr.client_id AS client_id,
              cli_org.name AS client_name,
              lr.foreign_entity_names AS foreign_entity_names,
              lr.foreign_entity_countries AS foreign_entity_countries,
              lr.general_issue_codes AS general_issue_codes
            FROM lobbying_registrations lr
            LEFT JOIN organizations cli_org ON cli_org.id = lr.client_id
            WHERE lr.has_foreign_entity = TRUE
              AND (
                CAST(:country AS text) IS NULL
                OR EXISTS (
                    SELECT 1
                    FROM unnest(COALESCE(lr.foreign_entity_countries, ARRAY[]::text[])) c
                    WHERE UPPER(c) = CAST(:country AS text)
                )
              )
              AND (CAST(:issue_code AS text) IS NULL OR CAST(:issue_code AS text) = ANY(lr.general_issue_codes))
            """
        ),
        {"country": country_filter, "issue_code": issue_filter},
    ).all()

    grouped = {}
    for row in rows:
        client_key = row.client_id or f"client:{row.client_name or 'unknown'}"
        item = grouped.setdefault(
            client_key,
            {
                "organization": {"id": row.client_id, "name": row.client_name or "Unknown client"},
                "registration_ids": set(),
                "foreign_entities": set(),
                "foreign_countries": set(),
                "issue_codes": set(),
            },
        )
        item["registration_ids"].add(row.registration_id)
        for name in row.foreign_entity_names or []:
            if name:
                item["foreign_entities"].add(name)
        for code in row.foreign_entity_countries or []:
            if code:
                item["foreign_countries"].add(code)
        for issue in row.general_issue_codes or []:
            if issue:
                item["issue_codes"].add(issue)

    findings = []
    for item in grouped.values():
        committees_targeted = []
        org_id = item["organization"]["id"]
        if org_id:
            committee_rows = db.execute(
                text(
                    """
                    SELECT DISTINCT c.name
                    FROM contributions contrib
                    JOIN committee_memberships cm ON cm.legislator_id = contrib.recipient_legislator_id
                    JOIN committees c ON c.id = cm.committee_id
                    WHERE contrib.contributor_org_id = :org_id
                    ORDER BY c.name
                    LIMIT 8
                    """
                ),
                {"org_id": org_id},
            ).all()
            committees_targeted = [r.name for r in committee_rows if r.name]

        findings.append(
            {
                "organization": item["organization"],
                "foreign_entities": sorted(item["foreign_entities"])[:20],
                "foreign_countries": sorted(item["foreign_countries"])[:20],
                "issue_codes": sorted(item["issue_codes"])[:12],
                "committees_targeted": committees_targeted,
                "filing_count": len(item["registration_ids"]),
            }
        )

    findings.sort(key=lambda x: x["filing_count"], reverse=True)
    return {"findings": findings[:limit]}

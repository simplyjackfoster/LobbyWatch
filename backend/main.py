import os
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

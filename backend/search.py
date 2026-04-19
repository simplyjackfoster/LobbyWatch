from sqlalchemy import any_, func, or_, select
from sqlalchemy.orm import Session

from models import Legislator, LobbyingRegistration, Organization


def search_entities(db: Session, q: str, limit: int = 20):
    q_norm = q.upper().strip()
    ilike_q = f"%{q}%"

    org_rows = db.execute(
        select(Organization.id, Organization.name, Organization.type)
        .where(Organization.name_normalized.ilike(f"%{q_norm}%"))
        .limit(limit)
    ).all()

    leg_rows = db.execute(
        select(Legislator.bioguide_id, Legislator.name, Legislator.party, Legislator.state)
        .where(Legislator.name.ilike(ilike_q))
        .limit(limit)
    ).all()

    issue_rows = db.execute(
        select(LobbyingRegistration.specific_issues, LobbyingRegistration.general_issue_codes)
        .where(
            or_(
                LobbyingRegistration.specific_issues_tsv.op("@@")(func.plainto_tsquery("english", q)),
                q_norm == any_(LobbyingRegistration.general_issue_codes),
            )
        )
        .limit(limit)
    ).all()

    results = []
    for row in org_rows:
        results.append({"id": row.id, "type": "organization", "name": row.name, "subtype": row.type})
    for row in leg_rows:
        results.append(
            {
                "id": row.bioguide_id,
                "type": "legislator",
                "name": row.name,
                "party": row.party,
                "state": row.state,
            }
        )
    seen_issue = set()
    for row in issue_rows:
        issue = row.specific_issues
        if issue and issue not in seen_issue:
            seen_issue.add(issue)
            results.append({"id": issue[:64], "type": "issue", "name": issue[:120]})
        for code in row.general_issue_codes or []:
            if code == q_norm and code not in seen_issue:
                seen_issue.add(code)
                results.append({"id": f"code-{code}", "type": "issue", "name": code})

    return {"results": results[:limit]}

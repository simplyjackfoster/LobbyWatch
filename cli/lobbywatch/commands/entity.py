import json as _json


def get_org_summary(conn, org_id: int) -> dict:
    empty = {
        "id": org_id,
        "name": None,
        "type": "organization",
        "total_lobbying_spend": 0,
        "active_years": [],
        "top_issue_codes": [],
        "top_lobbyists": [],
        "top_recipient_legislators": [],
        "total_contributions": 0,
        "filing_count": 0,
    }

    org = conn.execute(
        "SELECT id, name, type FROM organizations WHERE id = ?", (org_id,)
    ).fetchone()
    if not org:
        return empty

    rows = conn.execute(
        "SELECT id, amount, filing_year, general_issue_codes "
        "FROM lobbying_registrations WHERE client_id = ? OR registrant_id = ?",
        (org_id, org_id),
    ).fetchall()

    filing_count = len(rows)
    total_spend = sum(float(r["amount"] or 0) for r in rows)
    active_years = sorted({r["filing_year"] for r in rows if r["filing_year"] is not None})

    code_counts: dict[str, int] = {}
    for row in rows:
        for code in _json.loads(row["general_issue_codes"] or "[]"):
            code_counts[code] = code_counts.get(code, 0) + 1
    top_codes = sorted(code_counts, key=lambda code: code_counts[code], reverse=True)[:3]

    reg_ids = [r["id"] for r in rows]
    top_lobbyists = []
    if reg_ids:
        ph = ",".join("?" for _ in reg_ids)
        top_lobbyists = [
            {"name": r["name"], "filings": r["filings"]}
            for r in conn.execute(
                f"SELECT lo.name, COUNT(ll.registration_id) AS filings "
                f"FROM lobbying_lobbyists ll "
                f"JOIN lobbyists lo ON lo.id = ll.lobbyist_id "
                f"WHERE ll.registration_id IN ({ph}) "
                f"GROUP BY lo.name "
                f"ORDER BY filings DESC "
                f"LIMIT 5",
                reg_ids,
            )
        ]

    recipient_rows = conn.execute(
        "SELECT l.name, l.bioguide_id, SUM(c.amount) AS total "
        "FROM contributions c "
        "JOIN legislators l ON l.id = c.recipient_legislator_id "
        "WHERE c.contributor_org_id = ? "
        "GROUP BY l.id "
        "ORDER BY total DESC "
        "LIMIT 5",
        (org_id,),
    ).fetchall()

    top_recipient_legislators = [
        {
            "name": row["name"],
            "bioguide_id": row["bioguide_id"],
            "total_received": float(row["total"] or 0),
        }
        for row in recipient_rows
    ]
    total_contributions = sum(r["total_received"] for r in top_recipient_legislators)

    return {
        "id": org["id"],
        "name": org["name"],
        "type": "organization",
        "total_lobbying_spend": total_spend,
        "active_years": active_years,
        "top_issue_codes": top_codes,
        "top_lobbyists": top_lobbyists,
        "top_recipient_legislators": top_recipient_legislators,
        "total_contributions": total_contributions,
        "filing_count": filing_count,
    }


def get_legislator_summary(conn, entity_id: str) -> dict:
    empty = {
        "bioguide_id": entity_id,
        "name": None,
        "party": None,
        "state": None,
        "chamber": None,
        "committees": [],
        "top_contributing_orgs": [],
        "total_contributions_received": 0,
        "orgs_lobbying_committee_jurisdiction": 0,
    }

    leg = conn.execute(
        "SELECT id, bioguide_id, name, party, state, chamber "
        "FROM legislators WHERE bioguide_id = ?",
        (entity_id,),
    ).fetchone()
    if not leg:
        return empty

    committees = [
        {"name": row["cname"], "role": row["role"]}
        for row in conn.execute(
            "SELECT c.name AS cname, cm.role "
            "FROM committee_memberships cm "
            "JOIN committees c ON c.id = cm.committee_id "
            "WHERE cm.legislator_id = ?",
            (leg["id"],),
        )
    ]

    top_org_rows = conn.execute(
        "SELECT o.name, SUM(c.amount) AS total "
        "FROM contributions c "
        "JOIN organizations o ON o.id = c.contributor_org_id "
        "WHERE c.recipient_legislator_id = ? "
        "GROUP BY o.name "
        "ORDER BY total DESC "
        "LIMIT 5",
        (leg["id"],),
    ).fetchall()

    total_received = float(
        conn.execute(
            "SELECT COALESCE(SUM(amount), 0) "
            "FROM contributions "
            "WHERE recipient_legislator_id = ?",
            (leg["id"],),
        ).fetchone()[0]
    )

    cm_ids = [
        row[0]
        for row in conn.execute(
            "SELECT committee_id FROM committee_memberships WHERE legislator_id = ?",
            (leg["id"],),
        )
    ]
    orgs_jurisdiction = 0
    if cm_ids:
        ph = ",".join("?" for _ in cm_ids)
        peer_ids = [
            row[0]
            for row in conn.execute(
                f"SELECT DISTINCT legislator_id "
                f"FROM committee_memberships "
                f"WHERE committee_id IN ({ph})",
                cm_ids,
            )
        ]
        if peer_ids:
            ph2 = ",".join("?" for _ in peer_ids)
            orgs_jurisdiction = conn.execute(
                f"SELECT COUNT(DISTINCT contributor_org_id) "
                f"FROM contributions "
                f"WHERE recipient_legislator_id IN ({ph2})",
                peer_ids,
            ).fetchone()[0]

    return {
        "id": leg["id"],
        "bioguide_id": leg["bioguide_id"],
        "name": leg["name"],
        "party": leg["party"],
        "state": leg["state"],
        "chamber": leg["chamber"],
        "committees": committees,
        "top_contributing_orgs": [
            {"name": row["name"], "total_contributed": float(row["total"] or 0)}
            for row in top_org_rows
        ],
        "total_contributions_received": total_received,
        "orgs_lobbying_committee_jurisdiction": orgs_jurisdiction,
    }


def get_committee_summary(conn, committee_id: str) -> dict:
    empty = {
        "id": None,
        "name": None,
        "chamber": None,
        "member_count": 0,
        "members": [],
        "top_issue_codes": [],
        "active_lobbying_orgs": 0,
    }

    committee_text = str(committee_id)
    if committee_text.isdigit():
        com = conn.execute(
            "SELECT id, name, chamber, committee_id FROM committees WHERE id = ?",
            (int(committee_text),),
        ).fetchone()
    else:
        com = conn.execute(
            "SELECT id, name, chamber, committee_id FROM committees WHERE committee_id = ?",
            (committee_text,),
        ).fetchone()

    if not com:
        return empty

    members = [
        {"name": row["name"], "party": row["party"], "role": row["role"]}
        for row in conn.execute(
            "SELECT l.name, l.party, cm.role "
            "FROM committee_memberships cm "
            "JOIN legislators l ON l.id = cm.legislator_id "
            "WHERE cm.committee_id = ?",
            (com["id"],),
        )
    ]

    member_leg_ids = [
        row[0]
        for row in conn.execute(
            "SELECT legislator_id FROM committee_memberships WHERE committee_id = ?",
            (com["id"],),
        )
    ]

    top_codes: list[str] = []
    active_orgs = 0
    if member_leg_ids:
        ph = ",".join("?" for _ in member_leg_ids)
        code_counts: dict[str, int] = {}
        for row in conn.execute(
            f"SELECT lr.general_issue_codes "
            f"FROM lobbying_registrations lr "
            f"JOIN contributions c ON c.contributor_org_id = lr.client_id "
            f"WHERE c.recipient_legislator_id IN ({ph}) "
            f"LIMIT 500",
            member_leg_ids,
        ):
            for code in _json.loads(row[0] or "[]"):
                code_counts[code] = code_counts.get(code, 0) + 1
        top_codes = sorted(code_counts, key=lambda code: code_counts[code], reverse=True)[:3]
        active_orgs = conn.execute(
            "SELECT COUNT(DISTINCT client_id) FROM lobbying_registrations"
        ).fetchone()[0]

    return {
        "id": com["id"],
        "name": com["name"],
        "chamber": com["chamber"],
        "member_count": len(members),
        "members": members,
        "top_issue_codes": top_codes,
        "active_lobbying_orgs": active_orgs,
    }

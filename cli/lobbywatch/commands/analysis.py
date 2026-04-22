import json as _json


def betrayal_index(
    conn,
    issue_code: str = "HLTH",
    min_contribution: int = 10000,
    contribution_window_days: int = 365,
) -> dict:
    issue_upper = issue_code.upper()
    legislators = conn.execute(
        "SELECT DISTINCT l.id AS lid, l.name, l.party, l.state "
        "FROM legislators l "
        "JOIN co_sponsorships cs ON cs.legislator_id = l.id"
    ).fetchall()

    findings = []
    max_contrib = 0.0

    for leg in legislators:
        bills = conn.execute(
            "SELECT bill_id, bill_title FROM co_sponsorships WHERE legislator_id = ?",
            (leg["lid"],),
        ).fetchall()
        co_count = len(bills)
        if co_count == 0:
            continue

        contrib_rows = conn.execute(
            """
            SELECT o.name, SUM(c.amount) AS amount
            FROM contributions c
            JOIN organizations o ON o.id = c.contributor_org_id
            JOIN lobbying_registrations lr ON (lr.client_id = o.id OR lr.registrant_id = o.id)
            JOIN co_sponsorships cs ON cs.legislator_id = c.recipient_legislator_id
            WHERE c.recipient_legislator_id = ?
              AND EXISTS (
                SELECT 1 FROM json_each(lr.general_issue_codes) je WHERE UPPER(je.value) = ?
              )
              AND c.contribution_date >= cs.introduced_date
              AND c.contribution_date <= date(cs.introduced_date, '+' || ? || ' days')
            GROUP BY o.name
            ORDER BY amount DESC
            """,
            (leg["lid"], issue_upper, contribution_window_days),
        ).fetchall()

        total_contrib = sum(float(row["amount"] or 0) for row in contrib_rows)
        if total_contrib <= min_contribution:
            continue

        negative_votes = conn.execute(
            """
            SELECT bill_id, vote_position AS position, vote_date AS date
            FROM votes
            WHERE legislator_id = ?
              AND vote_position IN ('Nay', 'Not Voting')
              AND bill_id IN (
                SELECT bill_id FROM co_sponsorships WHERE legislator_id = ?
              )
            ORDER BY vote_date DESC
            """,
            (leg["lid"], leg["lid"]),
        ).fetchall()

        if not negative_votes:
            continue

        max_contrib = max(max_contrib, total_contrib)
        findings.append(
            {
                "legislator": {
                    "name": leg["name"],
                    "party": leg["party"],
                    "state": leg["state"],
                },
                "co_sponsored_bills": [
                    {"bill_id": row["bill_id"], "title": row["bill_title"]}
                    for row in bills[:10]
                ],
                "contributions_after_cosponsor": total_contrib,
                "contributing_orgs": [
                    {"name": row["name"], "amount": float(row["amount"] or 0)}
                    for row in contrib_rows[:10]
                ],
                "negative_votes": [
                    {"bill_id": row["bill_id"], "position": row["position"], "date": row["date"]}
                    for row in negative_votes[:10]
                ],
                "_co_count": co_count,
                "_neg_count": len(negative_votes),
            }
        )

    divisor = max_contrib or 1.0
    for finding in findings:
        normalized = finding["contributions_after_cosponsor"] / divisor
        finding["betrayal_score"] = round(
            normalized * (finding["_neg_count"] / max(finding["_co_count"], 1)), 4
        )
        del finding["_co_count"]
        del finding["_neg_count"]

    findings.sort(key=lambda item: item["betrayal_score"], reverse=True)
    return {"findings": findings}


def revolving_door(conn, agency: str = None, issue_code: str = None, limit: int = 50) -> dict:
    issue_upper = issue_code.upper() if issue_code else None
    params = []
    filters = "WHERE l.has_covered_position = 1"
    if issue_upper:
        filters += (
            " AND EXISTS (SELECT 1 FROM json_each(lr.general_issue_codes) je "
            "WHERE UPPER(je.value) = ?)"
        )
        params.append(issue_upper)

    rows = conn.execute(
        f"SELECT l.id AS lid, l.name AS lname, l.lda_id, l.covered_positions, "
        f"lr.id AS rid, lr.general_issue_codes, "
        f"reg_org.name AS registrant_name, cli_org.name AS client_name "
        f"FROM lobbyists l "
        f"JOIN lobbying_lobbyists ll ON ll.lobbyist_id = l.id "
        f"JOIN lobbying_registrations lr ON lr.id = ll.registration_id "
        f"LEFT JOIN organizations reg_org ON reg_org.id = lr.registrant_id "
        f"LEFT JOIN organizations cli_org ON cli_org.id = lr.client_id "
        f"{filters}",
        params,
    ).fetchall()

    grouped: dict = {}
    for row in rows:
        positions = _json.loads(row["covered_positions"] or "[]")
        if agency and not any(agency.lower() in str(position).lower() for position in positions):
            continue

        item = grouped.setdefault(
            row["lid"],
            {
                "lobbyist": {"name": row["lname"], "lda_id": row["lda_id"]},
                "prior_positions": positions,
                "registrant_counts": {},
                "clients": set(),
                "issue_codes": set(),
                "registration_ids": set(),
            },
        )
        item["registration_ids"].add(row["rid"])
        if row["registrant_name"]:
            item["registrant_counts"][row["registrant_name"]] = (
                item["registrant_counts"].get(row["registrant_name"], 0) + 1
            )
        if row["client_name"]:
            item["clients"].add(row["client_name"])
        for code in _json.loads(row["general_issue_codes"] or "[]"):
            if code:
                item["issue_codes"].add(code)

    findings = []
    max_raw = 0.0
    for item in grouped.values():
        filing_count = len(item["registration_ids"])
        issue_codes = item["issue_codes"]
        relevance = 1.0
        if issue_upper and issue_upper in issue_codes:
            relevance += 0.5
        raw_score = float(filing_count) * relevance
        max_raw = max(max_raw, raw_score)
        current = (
            max(item["registrant_counts"], key=item["registrant_counts"].get)
            if item["registrant_counts"]
            else None
        )

        findings.append(
            {
                "lobbyist": item["lobbyist"],
                "prior_positions": item["prior_positions"],
                "current_registrant": current,
                "clients": sorted(item["clients"])[:8],
                "issue_codes": sorted(issue_codes)[:10],
                "filing_count": filing_count,
                "_raw": raw_score,
            }
        )

    findings.sort(key=lambda item: item["_raw"], reverse=True)
    divisor = max_raw or 1.0
    normalized = []
    for row in findings[:limit]:
        normalized.append(
            {
                **{k: v for k, v in row.items() if k != "_raw"},
                "revolving_door_score": round(row["_raw"] / divisor, 2),
            }
        )
    return {"findings": normalized}


def foreign_influence(conn, country: str = None, issue_code: str = None, limit: int = 50) -> dict:
    issue_upper = issue_code.upper() if issue_code else None
    country_upper = country.upper() if country else None

    params = []
    filters = "WHERE lr.has_foreign_entity = 1"
    if country_upper:
        filters += (
            " AND EXISTS (SELECT 1 FROM json_each(lr.foreign_entity_countries) je "
            "WHERE UPPER(je.value) = ?)"
        )
        params.append(country_upper)
    if issue_upper:
        filters += (
            " AND EXISTS (SELECT 1 FROM json_each(lr.general_issue_codes) je "
            "WHERE UPPER(je.value) = ?)"
        )
        params.append(issue_upper)

    rows = conn.execute(
        f"SELECT lr.id AS rid, lr.client_id, cli_org.name AS client_name, "
        f"lr.foreign_entity_names, lr.foreign_entity_countries, lr.general_issue_codes "
        f"FROM lobbying_registrations lr "
        f"LEFT JOIN organizations cli_org ON cli_org.id = lr.client_id "
        f"{filters}",
        params,
    ).fetchall()

    grouped: dict = {}
    for row in rows:
        key = row["client_id"] or f"client:{row['client_name'] or 'unknown'}"
        item = grouped.setdefault(
            key,
            {
                "organization": {
                    "id": row["client_id"],
                    "name": row["client_name"] or "Unknown",
                },
                "registration_ids": set(),
                "foreign_entities": set(),
                "foreign_countries": set(),
                "issue_codes": set(),
            },
        )
        item["registration_ids"].add(row["rid"])
        for name in _json.loads(row["foreign_entity_names"] or "[]"):
            if name:
                item["foreign_entities"].add(name)
        for country_name in _json.loads(row["foreign_entity_countries"] or "[]"):
            if country_name:
                item["foreign_countries"].add(country_name)
        for code in _json.loads(row["general_issue_codes"] or "[]"):
            if code:
                item["issue_codes"].add(code)

    findings = []
    for item in grouped.values():
        org_id = item["organization"]["id"]
        committees_targeted = []
        if org_id:
            committees_targeted = [
                row[0]
                for row in conn.execute(
                    "SELECT DISTINCT c.name "
                    "FROM contributions contrib "
                    "JOIN committee_memberships cm "
                    "ON cm.legislator_id = contrib.recipient_legislator_id "
                    "JOIN committees c ON c.id = cm.committee_id "
                    "WHERE contrib.contributor_org_id = ? "
                    "ORDER BY c.name "
                    "LIMIT 8",
                    (org_id,),
                )
                if row[0]
            ]

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

    findings.sort(key=lambda item: item["filing_count"], reverse=True)
    return {"findings": findings[:limit]}

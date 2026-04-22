# cli/lobbywatch/commands/search.py


def search_entities(conn, q: str, limit: int = 20) -> dict:
    q_lower = q.lower().strip()
    pattern = f"%{q_lower}%"
    results = []

    # Organizations: case-insensitive name match
    for row in conn.execute(
        "SELECT id, name, type FROM organizations "
        "WHERE LOWER(name_normalized) LIKE ? LIMIT ?",
        (pattern, limit),
    ):
        results.append({"id": row["id"], "type": "organization", "name": row["name"], "subtype": row["type"]})

    # Legislators
    for row in conn.execute(
        "SELECT bioguide_id, name, party, state FROM legislators "
        "WHERE LOWER(name) LIKE ? LIMIT ?",
        (pattern, limit),
    ):
        results.append({
            "id": row["bioguide_id"],
            "type": "legislator",
            "name": row["name"],
            "party": row["party"],
            "state": row["state"],
        })

    # Issues: FTS5 full-text search
    seen = set()
    try:
        for row in conn.execute(
            "SELECT f.registration_id, f.specific_issues "
            "FROM issues_fts f WHERE issues_fts MATCH ? LIMIT ?",
            (q, limit),
        ):
            text = row["specific_issues"]
            if text and text not in seen:
                seen.add(text)
                results.append({"id": text[:64], "type": "issue", "name": text[:120]})
    except Exception:
        pass

    # Issues: exact issue code match
    q_upper = q.upper().strip()
    for row in conn.execute(
        "SELECT DISTINCT je.value FROM lobbying_registrations r, "
        "json_each(r.general_issue_codes) je "
        "WHERE UPPER(je.value) = ? LIMIT ?",
        (q_upper, limit),
    ):
        code = row[0]
        if code and f"code-{code}" not in seen:
            seen.add(f"code-{code}")
            results.append({"id": f"code-{code}", "type": "issue", "name": code})

    return {"results": results[:limit]}

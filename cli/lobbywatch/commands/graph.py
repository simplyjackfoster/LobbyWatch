import json as _json


def _fmt_amount(v) -> str:
    try:
        v = float(v)
    except (TypeError, ValueError):
        return ""
    if v >= 1_000_000:
        return f"${v/1_000_000:.1f}M"
    if v >= 1_000:
        return f"${v/1_000:.0f}k"
    return f"${v:.0f}"


class GraphBuilder:
    def __init__(self, max_nodes: int):
        self.max_nodes = max_nodes
        self.nodes: dict = {}
        self._edge_keys: set = set()
        self.edges: list = []

    def add_node(self, node_id: str, label: str, node_type: str, **attrs) -> bool:
        if node_id in self.nodes:
            self.nodes[node_id].update(attrs)
            return True
        if len(self.nodes) >= self.max_nodes:
            return False
        self.nodes[node_id] = {"id": node_id, "label": label, "type": node_type, **attrs}
        return True

    def add_edge(self, source: str, target: str, edge_type: str, **attrs) -> None:
        key = (source, target, edge_type)
        if key in self._edge_keys:
            return
        self._edge_keys.add(key)
        self.edges.append({"source": source, "target": target, "type": edge_type, **attrs})

    def build(self) -> dict:
        return {
            "nodes": list(self.nodes.values()),
            "edges": self.edges,
            "truncated": len(self.nodes) >= self.max_nodes,
        }


def get_org_graph(conn, org_id: int, year_min=None, year_max=None,
                  issue_code=None, node_limit: int = 50) -> dict:
    g = GraphBuilder(node_limit)
    row = conn.execute(
        "SELECT id, name, type FROM organizations WHERE id = ?", (org_id,)
    ).fetchone()
    if not row:
        return {"nodes": [], "edges": [], "truncated": False}

    g.add_node(f"org-{row['id']}", row["name"], "organization", subtype=row["type"])

    firm_limit = max(1, (node_limit - 1) // 2)
    leg_limit = max(1, (node_limit - 1) - firm_limit)

    params = [org_id]
    filters = ""
    if year_min:
        filters += " AND r.filing_year >= ?"
        params.append(year_min)
    if year_max:
        filters += " AND r.filing_year <= ?"
        params.append(year_max)
    if issue_code:
        filters += " AND EXISTS (SELECT 1 FROM json_each(r.general_issue_codes) je WHERE UPPER(je.value) = UPPER(?))"
        params.append(issue_code)
    params.append(firm_limit)

    for firm in conn.execute(
        f"SELECT r.registrant_id, o.name AS rname, "
        f"COALESCE(SUM(r.amount),0) AS total, COUNT(r.id) AS cnt "
        f"FROM lobbying_registrations r JOIN organizations o ON r.registrant_id = o.id "
        f"WHERE r.client_id = ? {filters} "
        f"GROUP BY r.registrant_id, o.name ORDER BY total DESC LIMIT ?",
        params,
    ):
        nid = f"org-{firm['registrant_id']}"
        if firm["registrant_id"] == org_id:
            continue
        if g.add_node(nid, firm["rname"], "organization", subtype="firm"):
            g.add_edge(f"org-{org_id}", nid, "hired_firm",
                       filing_count=firm["cnt"],
                       amount=float(firm["total"]),
                       amount_label=_fmt_amount(firm["total"]))

    for leg in conn.execute(
        "SELECT c.recipient_legislator_id AS leg_id, l.name, l.party, l.state, l.bioguide_id, "
        "SUM(c.amount) AS total FROM contributions c "
        "JOIN legislators l ON l.id = c.recipient_legislator_id "
        "WHERE c.contributor_org_id = ? "
        "GROUP BY c.recipient_legislator_id ORDER BY total DESC LIMIT ?",
        (org_id, leg_limit),
    ):
        nid = f"leg-{leg['leg_id']}"
        if g.add_node(nid, leg["name"], "legislator",
                      party=leg["party"], state=leg["state"],
                      bioguide_id=leg["bioguide_id"]):
            g.add_edge(f"org-{org_id}", nid, "contribution",
                       amount=float(leg["total"]),
                       amount_label=_fmt_amount(leg["total"]))

    leg_ids_int = [int(nid.split("-", 1)[1]) for nid in g.nodes if nid.startswith("leg-")]
    if leg_ids_int:
        placeholders = ",".join("?" for _ in leg_ids_int)
        for cm in conn.execute(
            f"SELECT cm.legislator_id, cm.committee_id, cm.role, c.name AS cname, c.committee_id AS ccode, c.chamber "
            f"FROM committee_memberships cm JOIN committees c ON c.id = cm.committee_id "
            f"WHERE cm.legislator_id IN ({placeholders})",
            leg_ids_int,
        ):
            nid = f"com-{cm['committee_id']}"
            if g.add_node(nid, cm["cname"], "committee",
                          chamber=cm["chamber"], committee_code=cm["ccode"]):
                g.add_edge(f"leg-{cm['legislator_id']}", nid, "member_of", role=cm["role"])

    return g.build()


def get_legislator_graph(conn, bioguide_id: str, year_min=None, year_max=None,
                         node_limit: int = 50) -> dict:
    g = GraphBuilder(node_limit)
    leg = conn.execute(
        "SELECT id, bioguide_id, name, party, state FROM legislators WHERE bioguide_id = ?",
        (bioguide_id,)
    ).fetchone()
    if not leg:
        return {"nodes": [], "edges": [], "truncated": False}

    g.add_node(f"leg-{leg['id']}", leg["name"], "legislator",
               party=leg["party"], state=leg["state"], bioguide_id=leg["bioguide_id"])

    params = [leg["id"]]
    filters = ""
    if year_min:
        filters += " AND strftime('%Y', c.contribution_date) >= ?"
        params.append(str(year_min))
    if year_max:
        filters += " AND strftime('%Y', c.contribution_date) <= ?"
        params.append(str(year_max))
    params.append(node_limit)

    for org in conn.execute(
        f"SELECT c.contributor_org_id AS org_id, o.name, o.type, "
        f"SUM(c.amount) AS total, COUNT(c.id) AS cnt "
        f"FROM contributions c JOIN organizations o ON o.id = c.contributor_org_id "
        f"WHERE c.recipient_legislator_id = ? {filters} "
        f"GROUP BY c.contributor_org_id ORDER BY total DESC LIMIT ?",
        params,
    ):
        nid = f"org-{org['org_id']}"
        if g.add_node(nid, org["name"], "organization", subtype=org["type"]):
            g.add_edge(nid, f"leg-{leg['id']}", "contribution",
                       amount=float(org["total"]),
                       amount_label=_fmt_amount(org["total"]),
                       contribution_count=org["cnt"])

    for cm in conn.execute(
        "SELECT cm.committee_id, cm.role, c.name AS cname, c.committee_id AS ccode, c.chamber "
        "FROM committee_memberships cm JOIN committees c ON c.id = cm.committee_id "
        "WHERE cm.legislator_id = ?",
        (leg["id"],),
    ):
        nid = f"com-{cm['committee_id']}"
        if g.add_node(nid, cm["cname"], "committee",
                      chamber=cm["chamber"], committee_code=cm["ccode"]):
            g.add_edge(f"leg-{leg['id']}", nid, "member_of", role=cm["role"])

    return g.build()


def get_issue_graph(conn, q: str, year_min=None, year_max=None,
                    node_limit: int = 50) -> dict:
    g = GraphBuilder(node_limit)
    q_upper = q.strip().upper()

    params = [q, f"%{q.lower()}%", q_upper]
    filters = ""
    if year_min:
        filters += " AND lr.filing_year >= ?"
        params.append(year_min)
    if year_max:
        filters += " AND lr.filing_year <= ?"
        params.append(year_max)
    params.append(node_limit * 2)

    rows = conn.execute(
        f"SELECT lr.client_id, c.name AS cname, c.type AS ctype, "
        f"lr.registrant_id, r.name AS rname, "
        f"COUNT(lr.id) AS cnt, COALESCE(SUM(lr.amount),0) AS total, "
        f"lr.general_issue_codes "
        f"FROM lobbying_registrations lr "
        f"JOIN organizations c ON c.id = lr.client_id "
        f"JOIN organizations r ON r.id = lr.registrant_id "
        f"WHERE ( "
        f"  EXISTS (SELECT 1 FROM issues_fts WHERE issues_fts MATCH ? AND rowid = lr.id) "
        f"  OR LOWER(lr.specific_issues) LIKE LOWER(?) "
        f"  OR EXISTS (SELECT 1 FROM json_each(lr.general_issue_codes) je WHERE UPPER(je.value) = ?) "
        f") {filters} "
        f"GROUP BY lr.client_id, c.name, c.type, lr.registrant_id, r.name "
        f"ORDER BY total DESC LIMIT ?",
        params,
    ).fetchall()

    for row in rows:
        cn = f"org-{row['client_id']}"
        rn = f"org-{row['registrant_id']}"
        issue_list = []
        try:
            issue_list = _json.loads(row["general_issue_codes"] or "[]")
        except Exception:
            pass
        g.add_node(cn, row["cname"], "organization", subtype=row["ctype"])
        g.add_node(rn, row["rname"], "organization", subtype="firm")
        g.add_edge(cn, rn, "hired_firm", filing_count=row["cnt"],
                   amount=float(row["total"]), amount_label=_fmt_amount(row["total"]),
                   issue_codes=issue_list)

    org_ids = [int(nid.split("-", 1)[1]) for nid in g.nodes if nid.startswith("org-")]
    if org_ids:
        ph = ",".join("?" for _ in org_ids)
        for row in conn.execute(
            f"SELECT c.contributor_org_id AS oid, l.id AS lid, l.name, l.party, l.state, l.bioguide_id, "
            f"SUM(c.amount) AS total FROM contributions c "
            f"JOIN legislators l ON l.id = c.recipient_legislator_id "
            f"WHERE c.contributor_org_id IN ({ph}) "
            f"GROUP BY c.contributor_org_id, l.id ORDER BY total DESC LIMIT ?",
            org_ids + [node_limit * 3],
        ):
            lnid = f"leg-{row['lid']}"
            g.add_node(lnid, row["name"], "legislator",
                       party=row["party"], state=row["state"], bioguide_id=row["bioguide_id"])
            g.add_edge(f"org-{row['oid']}", lnid, "contribution",
                       amount=float(row["total"]), amount_label=_fmt_amount(row["total"]))

        leg_ids = [int(nid.split("-", 1)[1]) for nid in g.nodes if nid.startswith("leg-")]
        if leg_ids:
            ph2 = ",".join("?" for _ in leg_ids)
            for cm in conn.execute(
                f"SELECT cm.legislator_id, cm.committee_id, cm.role, "
                f"c.name AS cname, c.committee_id AS ccode, c.chamber "
                f"FROM committee_memberships cm JOIN committees c ON c.id = cm.committee_id "
                f"WHERE cm.legislator_id IN ({ph2})",
                leg_ids,
            ):
                nid = f"com-{cm['committee_id']}"
                g.add_node(nid, cm["cname"], "committee",
                           chamber=cm["chamber"], committee_code=cm["ccode"])
                g.add_edge(f"leg-{cm['legislator_id']}", nid, "member_of", role=cm["role"])

    return g.build()

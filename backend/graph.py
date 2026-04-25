from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from models import (
    Committee,
    CommitteeMembership,
    Contribution,
    Legislator,
    LobbyingLobbyist,
    LobbyingRegistration,
    Lobbyist,
    Organization,
    Vote,
)


class GraphBuilder:
    def __init__(self, max_nodes: int):
        self.max_nodes = max_nodes
        self.nodes = {}
        self.edges = set()
        self.edge_list = []

    def add_node(self, node_id: str, label: str, node_type: str, **attrs):
        if node_id in self.nodes:
            self.nodes[node_id].update(attrs)
            return True
        if len(self.nodes) >= self.max_nodes:
            return False
        self.nodes[node_id] = {"id": node_id, "label": label, "type": node_type, **attrs}
        return True

    def add_edge(self, source: str, target: str, edge_type: str, **attrs):
        edge_key = (source, target, edge_type)
        if edge_key in self.edges:
            return
        self.edges.add(edge_key)
        self.edge_list.append({"source": source, "target": target, "type": edge_type, **attrs})

    def build(self):
        return {"nodes": list(self.nodes.values()), "edges": self.edge_list, "truncated": len(self.nodes) >= self.max_nodes}


def format_amount_label(amount) -> str:
    if not amount:
        return ""
    try:
        value = float(amount)
    except (TypeError, ValueError):
        return ""
    if value >= 1_000_000:
        return f"${value / 1_000_000:.1f}M"
    if value >= 1_000:
        return f"${value / 1_000:.0f}k"
    return f"${value:.0f}"


def safe_amount(amount) -> float:
    try:
        return float(amount)
    except (TypeError, ValueError):
        return 0.0


NODE_COLORS = {
    "organization": "#1a1a1a",
    "firm": "#1a1a1a",
    "legislator": "#2563eb",
    "committee": "#92400e",
    "lobbyist": "#6b7280",
}


def _normalize_issue_codes(value):
    if not value:
        return []
    seen = set()
    normalized = []
    for item in value:
        code = str(item or "").strip()
        if not code or code in seen:
            continue
        seen.add(code)
        normalized.append(code)
    return normalized


def get_organization_graph(
    db: Session,
    org_id: int,
    year_min=None,
    year_max=None,
    min_contribution=None,
    issue_code=None,
    max_nodes: int = 50,
):
    g = GraphBuilder(max_nodes=max_nodes)

    org = db.get(Organization, org_id)
    if not org:
        return g.build()

    g.add_node(f"org-{org.id}", org.name, "organization", subtype=org.type, color=NODE_COLORS["organization"])

    committee_reserve = max(4, max_nodes // 6)
    available_for_org_and_leg = max(2, max_nodes - 1 - committee_reserve)
    firm_limit = max(1, available_for_org_and_leg // 2)
    legislator_limit = max(1, available_for_org_and_leg - firm_limit)

    firms_query = text(
        """
        SELECT
          r.id AS registrant_id,
          r.name AS registrant_name,
          r.name_normalized AS name_normalized,
          COUNT(lr.id) AS filing_count,
          COALESCE(SUM(lr.amount), 0) AS total_amount,
          COALESCE(
            ARRAY_AGG(DISTINCT lr.filing_uuid) FILTER (WHERE lr.filing_uuid IS NOT NULL),
            ARRAY[]::text[]
          ) AS filing_uuids,
          COALESCE(
            ARRAY_AGG(DISTINCT issue_code) FILTER (WHERE issue_code IS NOT NULL),
            ARRAY[]::text[]
          ) AS issue_codes
        FROM lobbying_registrations lr
        JOIN organizations r ON lr.registrant_id = r.id
        LEFT JOIN LATERAL UNNEST(COALESCE(lr.general_issue_codes, lr.issue_codes, ARRAY[]::text[])) issue_code ON TRUE
        WHERE lr.client_id = :org_id
          AND lr.filing_year >= COALESCE(:year_min, lr.filing_year)
          AND lr.filing_year <= COALESCE(:year_max, lr.filing_year)
          AND (
            COALESCE(:issue_code, '') = ''
            OR COALESCE(:issue_code, '') = ANY(COALESCE(lr.general_issue_codes, lr.issue_codes, ARRAY[]::text[]))
          )
        GROUP BY r.id, r.name, r.name_normalized
        ORDER BY COALESCE(SUM(lr.amount), 0) DESC, COUNT(lr.id) DESC
        LIMIT :limit
        """
    )
    firm_rows = db.execute(
        firms_query,
        {
            "org_id": org_id,
            "year_min": year_min,
            "year_max": year_max,
            "issue_code": issue_code,
            "limit": firm_limit,
        },
    ).all()

    for row in firm_rows:
        firm_id = f"org-{row.registrant_id}"
        if row.registrant_id == org_id:
            continue
        if g.add_node(
            firm_id,
            row.registrant_name,
            "organization",
            subtype="firm",
            color=NODE_COLORS["firm"],
        ):
            total_amount = safe_amount(row.total_amount)
            g.add_edge(
                f"org-{org.id}",
                firm_id,
                "hired_firm",
                filing_count=int(row.filing_count or 0),
                amount=total_amount,
                amount_label=format_amount_label(total_amount),
                filing_uuids=_normalize_issue_codes(row.filing_uuids),
                issue_codes=_normalize_issue_codes(row.issue_codes),
            )

    contrib_query = (
        select(
            Legislator.id.label("legislator_id"),
            Legislator.name.label("name"),
            Legislator.party.label("party"),
            Legislator.state.label("state"),
            Legislator.bioguide_id.label("bioguide_id"),
            func.coalesce(func.sum(Contribution.amount), 0).label("total_contributed"),
            func.count(Contribution.id).label("contribution_count"),
            func.array_agg(func.distinct(Contribution.fec_committee_id))
            .filter(Contribution.fec_committee_id.is_not(None))
            .label("fec_committee_ids"),
        )
        .join(Legislator, Legislator.id == Contribution.recipient_legislator_id)
        .where(Contribution.contributor_org_id == org_id)
    )
    if year_min:
        contrib_query = contrib_query.where(func.extract("year", Contribution.contribution_date) >= year_min)
    if year_max:
        contrib_query = contrib_query.where(func.extract("year", Contribution.contribution_date) <= year_max)
    contrib_query = contrib_query.group_by(
        Legislator.id, Legislator.name, Legislator.party, Legislator.state, Legislator.bioguide_id
    )
    if min_contribution:
        contrib_query = contrib_query.having(func.sum(Contribution.amount) >= min_contribution)
    contrib_query = contrib_query.order_by(func.sum(Contribution.amount).desc()).limit(legislator_limit)
    contrib_rows = db.execute(contrib_query).all()

    legislator_ids = []
    for row in contrib_rows:
        legislator_ids.append(row.legislator_id)
        leg_node = f"leg-{row.legislator_id}"
        if g.add_node(
            leg_node,
            row.name,
            "legislator",
            party=row.party,
            state=row.state,
            bioguide_id=row.bioguide_id,
            color=NODE_COLORS["legislator"],
        ):
            total_contributed = safe_amount(row.total_contributed)
            g.add_edge(
                f"org-{org.id}",
                leg_node,
                "contribution",
                amount=total_contributed,
                amount_label=format_amount_label(total_contributed),
                contribution_count=int(row.contribution_count or 0),
                fec_committee_ids=_normalize_issue_codes(row.fec_committee_ids),
            )

    if legislator_ids:
        cm_rows = db.execute(
            select(CommitteeMembership, Committee)
            .join(Committee, Committee.id == CommitteeMembership.committee_id)
            .where(CommitteeMembership.legislator_id.in_(legislator_ids))
        ).all()
        for cm, committee in cm_rows:
            com_node = f"com-{committee.id}"
            if g.add_node(
                com_node,
                committee.name,
                "committee",
                chamber=committee.chamber,
                committee_code=committee.committee_id,
                color=NODE_COLORS["committee"],
            ):
                g.add_edge(f"leg-{cm.legislator_id}", com_node, "member_of", role=cm.role)

    return g.build()


def get_legislator_graph(
    db: Session,
    bioguide_id: str,
    year_min=None,
    year_max=None,
    min_contribution=None,
    max_nodes: int = 50,
):
    g = GraphBuilder(max_nodes=max_nodes)
    legislator = db.execute(select(Legislator).where(Legislator.bioguide_id == bioguide_id)).scalar_one_or_none()
    if not legislator:
        return g.build()

    g.add_node(
        f"leg-{legislator.id}",
        legislator.name,
        "legislator",
        party=legislator.party,
        state=legislator.state,
        bioguide_id=legislator.bioguide_id,
        color=NODE_COLORS["legislator"],
    )

    contrib_query = (
        select(
            Organization,
            func.coalesce(func.sum(Contribution.amount), 0).label("total"),
            func.count(Contribution.id).label("contribution_count"),
            func.array_agg(func.distinct(Contribution.fec_committee_id))
            .filter(Contribution.fec_committee_id.is_not(None))
            .label("fec_committee_ids"),
        )
        .join(Contribution, Contribution.contributor_org_id == Organization.id)
        .where(Contribution.recipient_legislator_id == legislator.id)
    )
    if year_min:
        contrib_query = contrib_query.where(func.extract("year", Contribution.contribution_date) >= year_min)
    if year_max:
        contrib_query = contrib_query.where(func.extract("year", Contribution.contribution_date) <= year_max)
    contrib_query = contrib_query.group_by(Organization.id).order_by(func.sum(Contribution.amount).desc()).limit(max_nodes)
    if min_contribution:
        contrib_query = contrib_query.having(func.sum(Contribution.amount) >= min_contribution)

    contrib_rows = db.execute(contrib_query).all()

    for org, total, contribution_count, fec_committee_ids in contrib_rows:
        org_node = f"org-{org.id}"
        if g.add_node(org_node, org.name, "organization", subtype=org.type, color=NODE_COLORS["organization"]):
            total_amount = safe_amount(total)
            g.add_edge(
                org_node,
                f"leg-{legislator.id}",
                "contribution",
                amount=total_amount,
                amount_label=format_amount_label(total_amount),
                contribution_count=int(contribution_count or 0),
                fec_committee_ids=_normalize_issue_codes(fec_committee_ids),
            )

    cm_rows = db.execute(
        select(CommitteeMembership, Committee)
        .join(Committee, Committee.id == CommitteeMembership.committee_id)
        .where(CommitteeMembership.legislator_id == legislator.id)
    ).all()
    for cm, committee in cm_rows:
        com_node = f"com-{committee.id}"
        if g.add_node(
            com_node,
            committee.name,
            "committee",
            chamber=committee.chamber,
            committee_code=committee.committee_id,
            color=NODE_COLORS["committee"],
        ):
            g.add_edge(f"leg-{legislator.id}", com_node, "member_of", role=cm.role)

    return g.build()


def get_issue_graph(
    db: Session,
    q: str,
    year_min=None,
    year_max=None,
    min_contribution=None,
    max_nodes: int = 50,
):
    g = GraphBuilder(max_nodes=max_nodes)
    search_term = (q or "").strip()
    if not search_term:
        return g.build()

    issue_query = text(
        """
        SELECT
          c.id AS client_id,
          c.name AS client_name,
          c.type AS client_type,
          r.id AS registrant_id,
          r.name AS registrant_name,
          COUNT(lr.id) AS filing_count,
          COALESCE(SUM(lr.amount), 0) AS total_amount,
          COALESCE(
            ARRAY_AGG(DISTINCT lr.filing_uuid) FILTER (WHERE lr.filing_uuid IS NOT NULL),
            ARRAY[]::text[]
          ) AS filing_uuids,
          COALESCE(
            ARRAY_AGG(DISTINCT issue_code) FILTER (WHERE issue_code IS NOT NULL),
            ARRAY[]::text[]
          ) AS issue_codes
        FROM lobbying_registrations lr
        JOIN organizations c ON c.id = lr.client_id
        JOIN organizations r ON r.id = lr.registrant_id
        LEFT JOIN LATERAL UNNEST(COALESCE(lr.general_issue_codes, lr.issue_codes, ARRAY[]::text[])) issue_code ON TRUE
        WHERE (
            lr.specific_issues_tsv @@ plainto_tsquery('english', :q)
            OR lr.specific_issues ILIKE :ilike_q
            OR :q_upper = ANY(COALESCE(lr.general_issue_codes, lr.issue_codes, ARRAY[]::text[]))
          )
          AND lr.filing_year >= COALESCE(:year_min, lr.filing_year)
          AND lr.filing_year <= COALESCE(:year_max, lr.filing_year)
        GROUP BY c.id, c.name, c.type, r.id, r.name
        ORDER BY COALESCE(SUM(lr.amount), 0) DESC, COUNT(lr.id) DESC
        LIMIT :limit
        """
    )

    rows = db.execute(
        issue_query,
        {
            "q": search_term,
            "ilike_q": f"%{search_term}%",
            "q_upper": search_term.upper(),
            "year_min": year_min,
            "year_max": year_max,
            "limit": max_nodes * 2,
        },
    ).all()

    for row in rows:
        client_node = f"org-{row.client_id}"
        firm_node = f"org-{row.registrant_id}"
        if g.add_node(
            client_node,
            row.client_name,
            "organization",
            subtype=row.client_type,
            color=NODE_COLORS["organization"],
        ):
            pass
        if g.add_node(firm_node, row.registrant_name, "organization", subtype="firm", color=NODE_COLORS["firm"]):
            pass
        total_amount = safe_amount(row.total_amount)
        g.add_edge(
            client_node,
            firm_node,
            "hired_firm",
            filing_count=int(row.filing_count or 0),
            amount=total_amount,
            amount_label=format_amount_label(total_amount),
            filing_uuids=_normalize_issue_codes(row.filing_uuids),
            issue_codes=_normalize_issue_codes(row.issue_codes),
        )

    org_ids = [int(node_id.split("-", 1)[1]) for node_id in g.nodes.keys() if node_id.startswith("org-")]
    if org_ids:
        contrib_query = (
            select(
                Contribution.contributor_org_id.label("org_id"),
                Legislator.id.label("leg_id"),
                Legislator.name.label("name"),
                Legislator.party.label("party"),
                Legislator.state.label("state"),
                Legislator.bioguide_id.label("bioguide_id"),
                func.coalesce(func.sum(Contribution.amount), 0).label("total_contributed"),
                func.array_agg(func.distinct(Contribution.fec_committee_id))
                .filter(Contribution.fec_committee_id.is_not(None))
                .label("fec_committee_ids"),
            )
            .join(Legislator, Legislator.id == Contribution.recipient_legislator_id)
            .where(Contribution.contributor_org_id.in_(org_ids))
        )
        if year_min:
            contrib_query = contrib_query.where(func.extract("year", Contribution.contribution_date) >= year_min)
        if year_max:
            contrib_query = contrib_query.where(func.extract("year", Contribution.contribution_date) <= year_max)
        contrib_query = contrib_query.group_by(
            Contribution.contributor_org_id,
            Legislator.id,
            Legislator.name,
            Legislator.party,
            Legislator.state,
            Legislator.bioguide_id,
        )
        if min_contribution:
            contrib_query = contrib_query.having(func.sum(Contribution.amount) >= min_contribution)
        contrib_query = contrib_query.order_by(func.sum(Contribution.amount).desc()).limit(max_nodes * 3)

        leg_ids = set()
        for row in db.execute(contrib_query).all():
            leg_ids.add(row.leg_id)
            leg_node = f"leg-{row.leg_id}"
            g.add_node(
                leg_node,
                row.name,
                "legislator",
                party=row.party,
                state=row.state,
                bioguide_id=row.bioguide_id,
                color=NODE_COLORS["legislator"],
            )
            total_contributed = safe_amount(row.total_contributed)
            g.add_edge(
                f"org-{row.org_id}",
                leg_node,
                "contribution",
                amount=total_contributed,
                amount_label=format_amount_label(total_contributed),
                fec_committee_ids=_normalize_issue_codes(row.fec_committee_ids),
            )

        if leg_ids:
            for cm, committee in db.execute(
                select(CommitteeMembership, Committee)
                .join(Committee, Committee.id == CommitteeMembership.committee_id)
                .where(CommitteeMembership.legislator_id.in_(list(leg_ids)))
            ).all():
                com_node = f"com-{committee.id}"
                g.add_node(
                    com_node,
                    committee.name,
                    "committee",
                    chamber=committee.chamber,
                    committee_code=committee.committee_id,
                    color=NODE_COLORS["committee"],
                )
                g.add_edge(f"leg-{cm.legislator_id}", com_node, "member_of", role=cm.role)

    return g.build()


def get_organization_summary(db: Session, org_id: int):
    org = db.get(Organization, org_id)
    if not org:
        return {
            "id": int(org_id),
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

    regs = db.execute(
        select(LobbyingRegistration).where(
            (LobbyingRegistration.client_id == org_id) | (LobbyingRegistration.registrant_id == org_id)
        )
    ).scalars().all()
    filing_count = len(regs)
    total_lobbying_spend = float(sum(float(r.amount or 0) for r in regs))
    active_years = sorted({r.filing_year for r in regs if r.filing_year is not None})

    code_counts = {}
    for r in regs:
        for code in (r.general_issue_codes or r.issue_codes or []):
            code_counts[code] = code_counts.get(code, 0) + 1
    top_issue_codes = [k for k, _ in sorted(code_counts.items(), key=lambda x: x[1], reverse=True)[:3]]

    reg_ids = [r.id for r in regs]
    top_lobbyists = []
    if reg_ids:
        rows = db.execute(
            select(Lobbyist.name, func.count(LobbyingLobbyist.registration_id).label("filings"))
            .join(LobbyingLobbyist, LobbyingLobbyist.lobbyist_id == Lobbyist.id)
            .where(LobbyingLobbyist.registration_id.in_(reg_ids))
            .group_by(Lobbyist.name)
            .order_by(func.count(LobbyingLobbyist.registration_id).desc())
            .limit(5)
        ).all()
        top_lobbyists = [{"name": r.name, "filings": int(r.filings)} for r in rows]

    top_recipients = db.execute(
        select(
            Legislator.name,
            Legislator.bioguide_id,
            func.sum(Contribution.amount).label("total_received"),
        )
        .join(Legislator, Legislator.id == Contribution.recipient_legislator_id)
        .where(Contribution.contributor_org_id == org_id)
        .group_by(Legislator.name, Legislator.bioguide_id)
        .order_by(func.sum(Contribution.amount).desc())
        .limit(5)
    ).all()
    top_recipient_legislators = [
        {
            "name": row.name,
            "bioguide_id": row.bioguide_id,
            "total_received": float(row.total_received or 0),
        }
        for row in top_recipients
    ]
    total_contributions = float(sum(r["total_received"] for r in top_recipient_legislators))

    return {
        "id": org.id,
        "name": org.name,
        "type": "organization",
        "total_lobbying_spend": total_lobbying_spend,
        "active_years": active_years,
        "top_issue_codes": top_issue_codes,
        "top_lobbyists": top_lobbyists,
        "top_recipient_legislators": top_recipient_legislators,
        "total_contributions": total_contributions,
        "filing_count": filing_count,
    }


def get_legislator_summary(db: Session, entity_id: str):
    leg = None
    if str(entity_id).isdigit():
        leg = db.get(Legislator, int(entity_id))
    if leg is None:
        leg = db.execute(select(Legislator).where(Legislator.bioguide_id == str(entity_id))).scalar_one_or_none()
    if not leg:
        return {
            "id": int(entity_id) if str(entity_id).isdigit() else None,
            "bioguide_id": entity_id if not str(entity_id).isdigit() else None,
            "name": None,
            "party": None,
            "state": None,
            "chamber": None,
            "committees": [],
            "top_contributing_orgs": [],
            "total_contributions_received": 0,
            "orgs_lobbying_committee_jurisdiction": 0,
        }

    committee_rows = db.execute(
        select(Committee.name, CommitteeMembership.role)
        .join(CommitteeMembership, CommitteeMembership.committee_id == Committee.id)
        .where(CommitteeMembership.legislator_id == leg.id)
    ).all()
    committees = [{"name": r.name, "role": r.role} for r in committee_rows]

    contrib_rows = db.execute(
        select(Organization.name, func.sum(Contribution.amount).label("total_contributed"))
        .join(Contribution, Contribution.contributor_org_id == Organization.id)
        .where(Contribution.recipient_legislator_id == leg.id)
        .group_by(Organization.name)
        .order_by(func.sum(Contribution.amount).desc())
        .limit(5)
    ).all()
    top_contributing_orgs = [
        {"name": row.name, "total_contributed": float(row.total_contributed or 0)} for row in contrib_rows
    ]
    total_contributions_received = float(
        db.execute(
            select(func.sum(Contribution.amount)).where(Contribution.recipient_legislator_id == leg.id)
        ).scalar()
        or 0
    )
    vote_rows = db.execute(
        select(Vote.bill_id, Vote.congress, Vote.vote_date)
        .where(Vote.legislator_id == leg.id)
        .where(Vote.bill_id.is_not(None))
        .order_by(Vote.vote_date.desc().nullslast(), Vote.id.desc())
        .limit(10)
    ).all()
    recent_votes = [
        {"bill_id": row.bill_id, "congress": row.congress, "vote_date": str(row.vote_date) if row.vote_date else None}
        for row in vote_rows
    ]

    committee_member_ids = db.execute(
        select(CommitteeMembership.committee_id).where(CommitteeMembership.legislator_id == leg.id)
    ).scalars().all()
    orgs_lobbying_committee_jurisdiction = 0
    if committee_member_ids:
        member_ids = db.execute(
            select(CommitteeMembership.legislator_id).where(CommitteeMembership.committee_id.in_(committee_member_ids))
        ).scalars().all()
        orgs_lobbying_committee_jurisdiction = int(
            db.execute(
                select(func.count(func.distinct(Contribution.contributor_org_id))).where(
                    Contribution.recipient_legislator_id.in_(list(set(member_ids)))
                )
            ).scalar()
            or 0
        )

    return {
        "id": leg.id,
        "bioguide_id": leg.bioguide_id,
        "name": leg.name,
        "party": leg.party,
        "state": leg.state,
        "chamber": leg.chamber,
        "committees": committees,
        "top_contributing_orgs": top_contributing_orgs,
        "total_contributions_received": total_contributions_received,
        "orgs_lobbying_committee_jurisdiction": orgs_lobbying_committee_jurisdiction,
        "recent_votes": recent_votes,
    }


def get_committee_summary(db: Session, committee_id: str):
    committee = None
    if str(committee_id).isdigit():
        committee = db.get(Committee, int(committee_id))
    if committee is None:
        committee = db.execute(select(Committee).where(Committee.committee_id == str(committee_id))).scalar_one_or_none()
    if not committee:
        return {
            "id": int(committee_id) if str(committee_id).isdigit() else None,
            "name": None,
            "chamber": None,
            "member_count": 0,
            "members": [],
            "top_issue_codes": [],
            "active_lobbying_orgs": 0,
        }

    member_rows = db.execute(
        select(Legislator.name, Legislator.party, CommitteeMembership.role)
        .join(CommitteeMembership, CommitteeMembership.legislator_id == Legislator.id)
        .where(CommitteeMembership.committee_id == committee.id)
    ).all()
    members = [{"name": row.name, "party": row.party, "role": row.role} for row in member_rows]

    member_ids = db.execute(
        select(CommitteeMembership.legislator_id).where(CommitteeMembership.committee_id == committee.id)
    ).scalars().all()

    top_issue_codes = []
    active_lobbying_orgs = 0
    if member_ids:
        rows = db.execute(
            select(LobbyingRegistration.general_issue_codes)
            .join(Contribution, Contribution.contributor_org_id == LobbyingRegistration.client_id)
            .where(Contribution.recipient_legislator_id.in_(member_ids))
            .limit(500)
        ).all()
        counts = {}
        for row in rows:
            for code in row.general_issue_codes or []:
                counts[code] = counts.get(code, 0) + 1
        top_issue_codes = [k for k, _ in sorted(counts.items(), key=lambda x: x[1], reverse=True)[:3]]
        active_lobbying_orgs = int(
            db.execute(
                select(func.count(func.distinct(LobbyingRegistration.client_id))).where(
                    LobbyingRegistration.client_id.is_not(None)
                )
            ).scalar()
            or 0
        )

    return {
        "id": committee.id,
        "name": committee.name,
        "chamber": committee.chamber,
        "member_count": len(members),
        "members": members,
        "top_issue_codes": top_issue_codes,
        "active_lobbying_orgs": active_lobbying_orgs,
    }


def get_entity_summary(db: Session, entity_type: str, entity_id: str):
    if entity_type == "organization":
        return get_organization_summary(db, int(entity_id))
    if entity_type == "legislator":
        return get_legislator_summary(db, entity_id)
    if entity_type == "committee":
        return get_committee_summary(db, entity_id)
    return {"error": "unsupported_entity_type", "entity_type": entity_type, "entity_id": entity_id}

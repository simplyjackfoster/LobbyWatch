from sqlalchemy import func, select
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


def _apply_filters_for_registrations(query, year_min=None, year_max=None, issue_code=None):
    if year_min:
        query = query.where(LobbyingRegistration.filing_year >= year_min)
    if year_max:
        query = query.where(LobbyingRegistration.filing_year <= year_max)
    if issue_code:
        query = query.where(LobbyingRegistration.issue_codes.any(issue_code))
    return query


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
    g.add_node(f"org-{org.id}", org.name, "organization", subtype=org.type)

    reg_query = select(LobbyingRegistration).where(
        (LobbyingRegistration.client_id == org_id) | (LobbyingRegistration.registrant_id == org_id)
    )
    reg_query = _apply_filters_for_registrations(reg_query, year_min, year_max, issue_code)
    regs = db.execute(reg_query.limit(50)).scalars().all()

    issue_tags = set()
    reg_ids = [r.id for r in regs]

    for r in regs:
        reg_node = f"reg-{r.id}"
        if not g.add_node(reg_node, f"Filing {r.filing_uuid[:8]}", "registration", amount=float(r.amount or 0), filing_year=r.filing_year):
            break
        g.add_edge(
            f"org-{org.id}",
            reg_node,
            "filed_or_targeted",
            has_foreign_entity=bool(r.has_foreign_entity),
            foreign_countries=r.foreign_entity_countries or [],
        )
        for issue in r.issue_codes or []:
            issue_tags.add(issue)

        if r.registrant_id and r.registrant_id != org.id:
            registrant = db.get(Organization, r.registrant_id)
            if registrant and g.add_node(f"org-{registrant.id}", registrant.name, "organization", subtype=registrant.type):
                g.add_edge(
                    f"org-{registrant.id}",
                    reg_node,
                    "registrant",
                    has_foreign_entity=bool(r.has_foreign_entity),
                    foreign_countries=r.foreign_entity_countries or [],
                )
        if r.client_id and r.client_id != org.id:
            client = db.get(Organization, r.client_id)
            if client and g.add_node(f"org-{client.id}", client.name, "organization", subtype=client.type):
                g.add_edge(
                    f"org-{client.id}",
                    reg_node,
                    "client",
                    has_foreign_entity=bool(r.has_foreign_entity),
                    foreign_countries=r.foreign_entity_countries or [],
                )

    if reg_ids:
        lobbyist_rows = db.execute(
            select(Lobbyist, LobbyingLobbyist.registration_id)
            .join(LobbyingLobbyist, LobbyingLobbyist.lobbyist_id == Lobbyist.id)
            .where(LobbyingLobbyist.registration_id.in_(reg_ids))
        ).all()
        for lobbyist, reg_id in lobbyist_rows:
            lob_node = f"lob-{lobbyist.id}"
            if g.add_node(
                lob_node,
                lobbyist.name,
                "lobbyist",
                has_covered_position=bool(lobbyist.has_covered_position),
                covered_positions=lobbyist.covered_positions or [],
                has_conviction=bool(lobbyist.has_conviction),
                conviction_disclosure=lobbyist.conviction_disclosure,
            ):
                g.add_edge(f"reg-{reg_id}", lob_node, "represented_by")

    contrib_query = (
        select(Contribution, Legislator)
        .join(Legislator, Legislator.id == Contribution.recipient_legislator_id)
        .where(Contribution.contributor_org_id == org_id)
    )
    if year_min:
        contrib_query = contrib_query.where(func.extract("year", Contribution.contribution_date) >= year_min)
    if year_max:
        contrib_query = contrib_query.where(func.extract("year", Contribution.contribution_date) <= year_max)
    if min_contribution:
        contrib_query = contrib_query.where(Contribution.amount >= min_contribution)

    contrib_rows = db.execute(contrib_query.limit(100)).all()
    legislator_ids = set()
    for contribution, legislator in contrib_rows:
        leg_node = f"leg-{legislator.id}"
        legislator_ids.add(legislator.id)
        if g.add_node(
            leg_node,
            legislator.name,
            "legislator",
            party=legislator.party,
            state=legislator.state,
            bioguide_id=legislator.bioguide_id,
        ):
            g.add_edge(
                f"org-{org.id}",
                leg_node,
                "contribution",
                amount=safe_amount(contribution.amount),
                amount_label=format_amount_label(contribution.amount),
                cycle=contribution.cycle,
            )

    if legislator_ids:
        cm_rows = db.execute(
            select(CommitteeMembership, Committee)
            .join(Committee, Committee.id == CommitteeMembership.committee_id)
            .where(CommitteeMembership.legislator_id.in_(legislator_ids))
        ).all()
        committee_ids = set()
        for cm, committee in cm_rows:
            committee_ids.add(committee.id)
            com_node = f"com-{committee.id}"
            if g.add_node(com_node, committee.name, "committee", chamber=committee.chamber):
                g.add_edge(f"leg-{cm.legislator_id}", com_node, "member_of", role=cm.role)

        vote_query = select(Vote).where(Vote.legislator_id.in_(legislator_ids))
        if year_min:
            vote_query = vote_query.where(func.extract("year", Vote.vote_date) >= year_min)
        if year_max:
            vote_query = vote_query.where(func.extract("year", Vote.vote_date) <= year_max)
        if issue_tags:
            vote_query = vote_query.where(Vote.issue_tags.overlap(list(issue_tags)))

        vote_rows = db.execute(vote_query.limit(100)).scalars().all()
        for vote in vote_rows:
            vote_node = f"vote-{vote.id}"
            if g.add_node(vote_node, vote.bill_title or vote.bill_id or "Vote", "vote", vote_position=vote.vote_position):
                g.add_edge(f"leg-{vote.legislator_id}", vote_node, "voted", position=vote.vote_position)

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
    )

    contrib_rows = db.execute(
        select(Organization, func.sum(Contribution.amount).label("total"), func.max(Contribution.cycle).label("latest_cycle"))
        .join(Contribution, Contribution.contributor_org_id == Organization.id)
        .where(Contribution.recipient_legislator_id == legislator.id)
        .group_by(Organization.id)
        .order_by(func.sum(Contribution.amount).desc())
        .limit(40)
    ).all()

    for org, total, latest_cycle in contrib_rows:
        if min_contribution and float(total or 0) < float(min_contribution):
            continue
        org_node = f"org-{org.id}"
        if g.add_node(org_node, org.name, "organization", subtype=org.type):
            g.add_edge(
                org_node,
                f"leg-{legislator.id}",
                "contribution",
                amount=safe_amount(total),
                amount_label=format_amount_label(total),
                cycle=latest_cycle,
            )

    cm_rows = db.execute(
        select(CommitteeMembership, Committee)
        .join(Committee, Committee.id == CommitteeMembership.committee_id)
        .where(CommitteeMembership.legislator_id == legislator.id)
    ).all()
    committee_ids = []
    for cm, committee in cm_rows:
        committee_ids.append(committee.id)
        com_node = f"com-{committee.id}"
        if g.add_node(com_node, committee.name, "committee", chamber=committee.chamber):
            g.add_edge(f"leg-{legislator.id}", com_node, "member_of", role=cm.role)

    vote_query = select(Vote).where(Vote.legislator_id == legislator.id).order_by(Vote.vote_date.desc()).limit(50)
    if year_min:
        vote_query = vote_query.where(func.extract("year", Vote.vote_date) >= year_min)
    if year_max:
        vote_query = vote_query.where(func.extract("year", Vote.vote_date) <= year_max)

    vote_rows = db.execute(vote_query).scalars().all()
    issue_tags = set()
    for vote in vote_rows:
        vote_node = f"vote-{vote.id}"
        if g.add_node(vote_node, vote.bill_title or vote.bill_id or "Vote", "vote", vote_position=vote.vote_position):
            g.add_edge(f"leg-{legislator.id}", vote_node, "voted", position=vote.vote_position)
        for tag in vote.issue_tags or []:
            issue_tags.add(tag)

    if committee_ids and issue_tags:
        rel_orgs = db.execute(
            select(Organization)
            .join(LobbyingRegistration, LobbyingRegistration.client_id == Organization.id)
            .where(LobbyingRegistration.issue_codes.overlap(list(issue_tags)))
            .limit(25)
        ).scalars().all()
        for org in rel_orgs:
            org_node = f"org-{org.id}"
            if g.add_node(org_node, org.name, "organization", subtype=org.type):
                g.add_edge(org_node, f"leg-{legislator.id}", "lobbied_related_issues")

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

    reg_query = (
        select(LobbyingRegistration)
        .where(LobbyingRegistration.specific_issues.ilike(f"%{q}%"))
        .limit(60)
    )
    if year_min:
        reg_query = reg_query.where(LobbyingRegistration.filing_year >= year_min)
    if year_max:
        reg_query = reg_query.where(LobbyingRegistration.filing_year <= year_max)

    regs = db.execute(reg_query).scalars().all()

    org_ids = set()
    issue_codes = set()
    issue_node_id = f"issue-{q[:40].lower().replace(' ', '-') }"
    g.add_node(issue_node_id, q, "issue")

    for r in regs:
        for oid in [r.client_id, r.registrant_id]:
            if not oid:
                continue
            org_ids.add(oid)
            org = db.get(Organization, oid)
            if org and g.add_node(f"org-{org.id}", org.name, "organization", subtype=org.type):
                g.add_edge(
                    f"org-{org.id}",
                    issue_node_id,
                    "lobbied_on",
                    filing_year=r.filing_year,
                    has_foreign_entity=bool(r.has_foreign_entity),
                    foreign_countries=r.foreign_entity_countries or [],
                )
        for code in r.issue_codes or []:
            issue_codes.add(code)

    if org_ids:
        contrib_rows = db.execute(
            select(Contribution, Legislator)
            .join(Legislator, Legislator.id == Contribution.recipient_legislator_id)
            .where(Contribution.contributor_org_id.in_(list(org_ids)))
            .limit(100)
        ).all()
        leg_ids = set()
        for c, leg in contrib_rows:
            if min_contribution and float(c.amount or 0) < float(min_contribution):
                continue
            leg_ids.add(leg.id)
            if g.add_node(
                f"leg-{leg.id}",
                leg.name,
                "legislator",
                party=leg.party,
                state=leg.state,
                bioguide_id=leg.bioguide_id,
            ):
                g.add_edge(
                    f"org-{c.contributor_org_id}",
                    f"leg-{leg.id}",
                    "contribution",
                    amount=safe_amount(c.amount),
                    amount_label=format_amount_label(c.amount),
                    cycle=c.cycle,
                )

        if leg_ids:
            cm_rows = db.execute(
                select(CommitteeMembership, Committee)
                .join(Committee, Committee.id == CommitteeMembership.committee_id)
                .where(CommitteeMembership.legislator_id.in_(list(leg_ids)))
                .limit(100)
            ).all()
            for cm, committee in cm_rows:
                if g.add_node(f"com-{committee.id}", committee.name, "committee", chamber=committee.chamber):
                    g.add_edge(f"leg-{cm.legislator_id}", f"com-{committee.id}", "member_of", role=cm.role)

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

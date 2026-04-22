import os

from dotenv import load_dotenv
from sqlalchemy import Boolean, Column, Date, ForeignKey, Integer, Numeric, Text, create_engine
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, TSVECTOR
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.pool import NullPool

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg://lobbying:lobbying@localhost:5432/lobbying")
if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg://", 1)

_pool_size = int(os.getenv("SQLALCHEMY_POOL_SIZE", "5"))
_max_overflow = int(os.getenv("SQLALCHEMY_MAX_OVERFLOW", "10"))
_pool_timeout = int(os.getenv("SQLALCHEMY_POOL_TIMEOUT_SECONDS", "30"))
_pool_recycle = int(os.getenv("SQLALCHEMY_POOL_RECYCLE_SECONDS", "1800"))
_disable_pooling = os.getenv("SQLALCHEMY_DISABLE_POOLING", "0") == "1"

engine_kwargs = {"pool_pre_ping": True}
if _disable_pooling:
    engine_kwargs["poolclass"] = NullPool
else:
    engine_kwargs["pool_size"] = _pool_size
    engine_kwargs["max_overflow"] = _max_overflow
    engine_kwargs["pool_timeout"] = _pool_timeout
    engine_kwargs["pool_recycle"] = _pool_recycle

engine = create_engine(DATABASE_URL, **engine_kwargs)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()


class Organization(Base):
    __tablename__ = "organizations"
    id = Column(Integer, primary_key=True)
    name = Column(Text, nullable=False)
    name_normalized = Column(Text, nullable=False)
    type = Column(Text)
    industry_code = Column(Text)


class Legislator(Base):
    __tablename__ = "legislators"
    id = Column(Integer, primary_key=True)
    bioguide_id = Column(Text, nullable=False, unique=True)
    name = Column(Text, nullable=False)
    party = Column(Text)
    state = Column(Text)
    chamber = Column(Text)
    is_active = Column(Boolean, default=True)


class Committee(Base):
    __tablename__ = "committees"
    id = Column(Integer, primary_key=True)
    committee_id = Column(Text, nullable=False, unique=True)
    name = Column(Text, nullable=False)
    chamber = Column(Text)
    subcommittee_of = Column(Text)


class Lobbyist(Base):
    __tablename__ = "lobbyists"
    id = Column(Integer, primary_key=True)
    name = Column(Text, nullable=False)
    name_normalized = Column(Text, nullable=False)
    lda_id = Column(Text, unique=True)
    covered_positions = Column(ARRAY(Text))
    has_covered_position = Column(Boolean, default=False)
    conviction_disclosure = Column(Text)
    has_conviction = Column(Boolean, default=False)


class LobbyingRegistration(Base):
    __tablename__ = "lobbying_registrations"
    id = Column(Integer, primary_key=True)
    registrant_id = Column(Integer, ForeignKey("organizations.id"))
    client_id = Column(Integer, ForeignKey("organizations.id"))
    filing_uuid = Column(Text, unique=True)
    filing_year = Column(Integer)
    filing_period = Column(Text)
    amount = Column(Numeric)
    issue_codes = Column(ARRAY(Text))
    general_issue_codes = Column(ARRAY(Text))
    specific_issues = Column(Text)
    specific_issues_tsv = Column(TSVECTOR)
    has_foreign_entity = Column(Boolean, default=False)
    foreign_entity_names = Column(ARRAY(Text))
    foreign_entity_countries = Column(ARRAY(Text))


class LobbyingLobbyist(Base):
    __tablename__ = "lobbying_lobbyists"
    registration_id = Column(Integer, ForeignKey("lobbying_registrations.id"), primary_key=True)
    lobbyist_id = Column(Integer, ForeignKey("lobbyists.id"), primary_key=True)


class Contribution(Base):
    __tablename__ = "contributions"
    id = Column(Integer, primary_key=True)
    contributor_org_id = Column(Integer, ForeignKey("organizations.id"))
    recipient_legislator_id = Column(Integer, ForeignKey("legislators.id"))
    amount = Column(Numeric, nullable=False)
    contribution_date = Column(Date)
    fec_committee_id = Column(Text)
    cycle = Column(Integer)


class CommitteeMembership(Base):
    __tablename__ = "committee_memberships"
    legislator_id = Column(Integer, ForeignKey("legislators.id"), primary_key=True)
    committee_id = Column(Integer, ForeignKey("committees.id"), primary_key=True)
    role = Column(Text)


class Vote(Base):
    __tablename__ = "votes"
    id = Column(Integer, primary_key=True)
    legislator_id = Column(Integer, ForeignKey("legislators.id"))
    bill_id = Column(Text)
    bill_title = Column(Text)
    vote_position = Column(Text)
    vote_date = Column(Date)
    congress = Column(Integer)
    issue_tags = Column(ARRAY(Text))


class LobbyistContribution(Base):
    __tablename__ = "lobbyist_contributions"
    id = Column(Integer, primary_key=True)
    filing_uuid = Column(Text, unique=True, nullable=False)
    lobbyist_id = Column(Integer, ForeignKey("lobbyists.id"))
    registrant_id = Column(Integer, ForeignKey("organizations.id"))
    filing_year = Column(Integer)
    filing_period = Column(Text)
    contribution_items = Column(JSONB)
    pacs = Column(ARRAY(Text))
    dt_posted = Column(Date)

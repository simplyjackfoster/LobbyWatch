from aws_env import bootstrap_ssm_env

bootstrap_ssm_env()

from mangum import Mangum
from main import app
from models import engine
from sqlalchemy import text

with engine.connect() as _conn:
    _conn.execute(text("""
        CREATE TABLE IF NOT EXISTS co_sponsorships (
            id SERIAL PRIMARY KEY,
            legislator_id INTEGER REFERENCES legislators(id),
            bill_id TEXT NOT NULL,
            bill_title TEXT,
            congress INTEGER,
            introduced_date DATE,
            UNIQUE(legislator_id, bill_id)
        )
    """))
    _conn.commit()

handler = Mangum(app, lifespan="off")

import pytest
from sqlalchemy import create_engine, text
import os

@pytest.fixture(scope="session")
def engine():
    """
    Creates a test Postgres engine using same env vars as main script.
    Uses a dedicated test schema so real data is untouched.
    """

    POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
    POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
    POSTGRES_DB = os.environ.get("POSTGRES_DB")
    POSTGRES_USER = os.environ.get("POSTGRES_USER")
    POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")

    url = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    eng = create_engine(url)

    yield eng

    # optional cleanup after all tests
    with eng.begin() as conn:
        conn.execute(text("""
            DO $$
            DECLARE r RECORD;
            BEGIN
                FOR r IN (
                    SELECT schema_name
                    FROM information_schema.schemata
                    WHERE schema_name LIKE 'test_%'
                )
                LOOP
                    EXECUTE 'DROP SCHEMA IF EXISTS ' || quote_ident(r.schema_name) || ' CASCADE';
                END LOOP;
            END $$;
        """))
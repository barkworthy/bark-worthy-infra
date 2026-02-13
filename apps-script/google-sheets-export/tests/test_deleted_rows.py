import pytest
from src.sheets_exporter import handle_deleted_rows
from sqlalchemy import text

def test_deleted_rows_archived_then_deleted(engine):
    schema = "test_schema"
    table = "orders"

    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS {schema}'))
        conn.execute(text(f'''
            CREATE TABLE {schema}.{table} (
                internal_uuid UUID PRIMARY KEY,
                name TEXT
            )
        '''))

        # insert 3 rows
        conn.execute(text(f'''
            INSERT INTO {schema}.{table} (internal_uuid, name)
            VALUES
            (gen_random_uuid(), 'a'),
            (gen_random_uuid(), 'b'),
            (gen_random_uuid(), 'c')
        '''))

        rows = conn.execute(text(f'''
            SELECT internal_uuid FROM {schema}.{table}
        ''')).fetchall()

    # simulate sheet keeping only first row
    keep_uuid = [str(rows[0][0])]

    res = handle_deleted_rows(engine, schema, table, keep_uuid)

    assert res["deleted"] == 2

    with engine.connect() as conn:
        remaining = conn.execute(text(f'''
            SELECT COUNT(*) FROM {schema}.{table}
        ''')).scalar()

        archived = conn.execute(text(f'''
            SELECT COUNT(*) FROM {schema}.deleted_rows_log
        ''')).scalar()

    assert remaining == 1
    assert archived == 2
    
def test_archive_mismatch_aborts_delete(engine):
    schema = "test_schema2"
    table = "orders"

    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS {schema}'))
        conn.execute(text(f'''
            CREATE TABLE {schema}.{table} (
                internal_uuid UUID PRIMARY KEY
            )
        '''))

        conn.execute(text(f'''
            INSERT INTO {schema}.{table}
            VALUES (gen_random_uuid())
        '''))

    # pass fake UUID that doesn't exist
    res = handle_deleted_rows(engine, schema, table, ["fake-uuid"])
    assert res["deleted"] == 1

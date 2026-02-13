from sqlalchemy import text
from src.sheets_exporter import add_new_columns

def test_add_new_columns(engine):
    schema = "test_schema4"
    table = "orders"

    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS {schema}'))
        conn.execute(text(f'''
            CREATE TABLE {schema}.{table} (
                internal_uuid UUID PRIMARY KEY
            )
        '''))

    add_new_columns(engine, schema, table, ["new_col"])

    with engine.connect() as conn:
        cols = conn.execute(text(f'''
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema=:s AND table_name=:t
        '''), {"s": schema, "t": table}).fetchall()

    col_names = [c[0] for c in cols]
    assert "new_col" in col_names

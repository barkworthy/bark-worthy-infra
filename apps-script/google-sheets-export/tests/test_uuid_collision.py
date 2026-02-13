from sqlalchemy import text

def test_uuid_collision_detection(engine):
    schema = "test_schema3"
    table = "orders"

    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS {schema}'))
        conn.execute(text(f'''
            CREATE TABLE {schema}.{table} (
                internal_uuid UUID PRIMARY KEY
            )
        '''))

        # insert known uuid
        conn.execute(text(f'''
            INSERT INTO {schema}.{table}
            VALUES ('11111111-1111-1111-1111-111111111111')
        '''))

    ins = [{"internal_uuid": "11111111-1111-1111-1111-111111111111"}]

    # simulate your collision check block
    new_uuids = [r["internal_uuid"] for r in ins]

    placeholders = ":u0"
    params = {"u0": new_uuids[0]}

    sql = text(f'''
        SELECT internal_uuid
        FROM {schema}.{table}
        WHERE internal_uuid IN ({placeholders})
    ''')

    with engine.connect() as conn:
        existing = conn.execute(sql, params).fetchall()

    assert len(existing) == 1

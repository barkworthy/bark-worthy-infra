print("importing")
import os
from dotenv import load_dotenv
load_dotenv()
from google.oauth2.service_account import Credentials
from sqlalchemy import create_engine, text
import gspread
import traceback
from sqlalchemy.engine import Engine
from google.oauth2 import service_account
from typing import Tuple, List
import uuid
import yaml
print("done")

# --- config (from env) ---
print("setting env vars")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB")
POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
MAPPINGS_PATH = os.environ.get("MAPPINGS_YML", "mappings.yml")
print("done")

# --- mappings (from mappings.yml) ---
with open(MAPPINGS_PATH, "r", encoding="utf-8") as f:
    MAPPINGS = yaml.safe_load(f)

if not isinstance(MAPPINGS, list) or not MAPPINGS:
    raise RuntimeError("mappings.yml must contain a non-empty list at the top level.")

# barkdb credentials
def get_engine() -> Engine:
    """
    1) Build DB URL from environment (uses same env vars already defined above).
    2) Return a SQLAlchemy Engine with pool_pre_ping to avoid stale connections.
    """
    url = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    return create_engine(url, pool_pre_ping=True)

# google API credentials
def get_gspread_client(scopes: List[str] = None):
    """
    1) Read GOOGLE_APPLICATION_CREDENTIALS (SERVICE_ACCOUNT_JSON already read earlier).
    2) Create google oauth credentials with requested scopes and return an authorized gspread client.
    """
    sa_path = SERVICE_ACCOUNT_JSON
    if not sa_path:
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS (SERVICE_ACCOUNT_JSON) is not set")

    if scopes is None:
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]

    creds = service_account.Credentials.from_service_account_file(sa_path, scopes=scopes)
    return gspread.authorize(creds)

# read the data
def read_sheet(sheet_id, sheet_name):
    gc = get_gspread_client()
    ws = gc.open_by_key(sheet_id).worksheet(sheet_name)
    rows = ws.get_all_records()   # dicts, header row auto-handled
    return rows, ws

# Figure out which rows to insert and which to update, depending on whether they already have an internal_uuid
def split_rows(rows, uuid_col):
    inserts = []
    updates = []
    for r in rows:
        uid = r.get(uuid_col, "").strip()
        if uid:
            updates.append(r)
        else:
            inserts.append(r)
    return inserts, updates

# Assign new rows internal_uuids
def assign_uuids(rows, uuid_col="internal_uuid"):
    for r in rows:
        r[uuid_col] = str(uuid.uuid4())
    return rows

def normalize_columns(cols):
    """Return a list of normalized lowercase column names for DB."""
    cols = [c.lower() for c in cols]
    if "processed_at" not in cols:
        cols.append("processed_at")
    if "internal_uuid" not in cols:
        cols.append("internal_uuid")
    return cols


def build_col_defs(cols):
    """Build SQL column definitions based on normalized names."""
    col_defs = []
    for c in cols:
        if c == "internal_uuid":
            col_defs.append(f'"{c}" UUID PRIMARY KEY')
        elif c == "processed_at":
            col_defs.append(f'"{c}" TIMESTAMPTZ')
        else:
            col_defs.append(f'"{c}" TEXT')
    return col_defs

# Create the staging table, all text. will ETL later
def create_staging_table(engine, schema, table, cols):
    cols = normalize_columns(cols)
    col_defs = build_col_defs(cols)

    ddl = f"""
        DROP TABLE IF EXISTS {schema}."{table}";
        CREATE TABLE {schema}."{table}" (
            {", ".join(col_defs)}
        );
    """
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        conn.execute(text(ddl))

def drop_table(engine, schema, table):
    ddl = f"""
        DROP TABLE IF EXISTS {schema}."{table}";
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))

# Load data into staging table
def load_staging(engine, schema, table, rows, batch_size=300):
    if not rows:
        print("no rows to load into staging")
        return

    cols = [c for c in rows[0].keys() if c != "processed_at"]
    quoted_cols = ", ".join([f"\"{c}\"" for c in cols])
    placeholders = ", ".join([f":{c}" for c in cols])
    insert_sql = text(
        f"INSERT INTO {schema}.\"{table}\" ({quoted_cols}, processed_at) VALUES ({placeholders}, NOW())"
    )

    # Remove processed_at from each dict so DB can fill it
    for r in rows:
        if "processed_at" in r:
            r.pop("processed_at")

    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE {schema}.\"{table}\""))
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i+batch_size]
            conn.execute(insert_sql, batch)

    print(f"loaded {len(rows)} rows into staging")

def create_target_table_if_not_exists(engine, schema, table, cols):
    cols = normalize_columns(cols)
    col_defs = build_col_defs(cols)

    ddl_schema = text(f'CREATE SCHEMA IF NOT EXISTS {schema};')
    ddl_table = text(f"""
        CREATE TABLE IF NOT EXISTS {schema}."{table}" (
            {", ".join(col_defs)}
        );
    """)

    with engine.begin() as conn:
        conn.execute(ddl_schema)
        conn.execute(ddl_table)

def upsert_staging_into_target(engine, schema, staging, target, cols):
    # defensive checks
    if not isinstance(cols, (list, tuple)):
        raise TypeError("cols must be a list/tuple of column names")
    if "internal_uuid" not in cols:
        raise ValueError("cols must include 'internal_uuid'")

    col_list = ", ".join([f'"{c}"' for c in cols])
    pk = "internal_uuid"
    update_assignments = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in cols if c != pk])

    sql = f"""
        INSERT INTO {schema}."{target}" ({col_list})
        SELECT {col_list}
        FROM {schema}."{staging}"
        ON CONFLICT ("{pk}") DO UPDATE SET
            {update_assignments}
        RETURNING "{pk}";
    """

    with engine.begin() as conn:
        result = conn.execute(text(sql))
        return [row[0] for row in result.fetchall()]

def update_sheet_with_results(engine, worksheet, processed_uuids, schema, target_table, uuid_col, processed_col, original_rows=None):
       # defensive checks
    if processed_uuids is None or not isinstance(processed_uuids, (list, tuple)):
        raise ValueError("processed_uuids must be a list/tuple of uuid strings")

    # read header row from sheet and ensure columns exist
    headers = worksheet.row_values(1)
    # normalize header values (strip BOM/spaces)
    headers = [h.strip().lstrip("\ufeff") for h in headers]

    # add missing header columns to the sheet (append to end)
    header_modified = False
    if uuid_col not in headers:
        headers.append(uuid_col)
        header_modified = True
    if processed_col not in headers:
        headers.append(processed_col)
        header_modified = True
    if header_modified:
        # write full header back (safe: small operation)
        worksheet.update("1:1", [headers])

    # refresh headers & compute column indexes (1-based)
    headers = worksheet.row_values(1)
    headers = [h.strip().lstrip("\ufeff") for h in headers]
    try:
        uuid_idx = headers.index(uuid_col) + 1
        processed_idx = headers.index(processed_col) + 1
    except ValueError as e:
        raise RuntimeError("Failed to locate uuid/processed columns after ensuring headers.") from e

     # original_rows corresponds to sheet rows starting at row 2
    if original_rows is None:
        # get_all_records() caller already had rows; fallback: re-read so function can operate alone
        original_rows = worksheet.get_all_records()

    total_rows = len(original_rows)
    if total_rows == 0:
        print("No data rows found to update.")
        return

        # Build list of sheet row numbers (1-based) and determine which rows lacked uuid originally
    rows_missing_uuid_idx = []   # list of 1-based sheet row numbers that need uuids
    existing_uuids = []          # uuids already present in sheet (for updates)
    for i, r in enumerate(original_rows, start=2):  # sheet rows start at 2
        uid = (r.get(uuid_col, "") or "").strip()
        if uid:
            existing_uuids.append(uid)
        else:
            rows_missing_uuid_idx.append(i)

    # Determine how many were existing
    n_existing = len(existing_uuids)

    # processed_uuids expected ordering: [existing_uuids..., new_inserts...]
    if len(processed_uuids) < n_existing:
        # defensive: should not happen
        raise RuntimeError("processed_uuids length less than number of existing uuids in sheet")

    # split processed_uuids into existing portion and inserts portion
    inserts_uuids = processed_uuids[n_existing:]  # these correspond to rows_missing_uuid_idx in sheet order

    if len(inserts_uuids) != len(rows_missing_uuid_idx):
        # still possible mismatch (e.g., different staging ordering). Try to continue by using set-diff fallback:
        # Build mapping by set difference: new_uuids = processed_uuids \ existing_uuids
        new_uuids_set = set(processed_uuids) - set(existing_uuids)
        if len(new_uuids_set) != len(rows_missing_uuid_idx):
            # unrecoverable mismatch; surface error (do not change sheet)
            raise RuntimeError("Mismatch between computed insert rows and processed_uuids; aborting sheet update.")
        # otherwise produce inserts_uuids in arbitrary order (convert to list)
        inserts_uuids = list(new_uuids_set)

    # Build final per-row uuid list aligned with sheet rows (index i -> value or empty)
    final_uuids = []
    insert_iter = iter(inserts_uuids)
    for r in original_rows:
        uid = (r.get(uuid_col, "") or "").strip()
        if uid:
            final_uuids.append(uid)
        else:
            final_uuids.append(next(insert_iter))

    # Query the DB for processed_at timestamps for all processed_uuids (avoid per-row queries)
    # Use parametrized IN query
    # Build SQL using text() and pass list as tuple; SQLAlchemy will expand the tuple via :uids
    uuids_tuple = tuple(processed_uuids)
    placeholders = ", ".join([f":u{i}" for i in range(len(uuids_tuple))])
    params = {f"u{i}": uuids_tuple[i] for i in range(len(uuids_tuple))}
    sql = text(f"""
        SELECT {uuid_col}::text AS internal_uuid, {processed_col}
        FROM {schema}."{target_table}"
        WHERE internal_uuid IN ({placeholders})
    """)
    uuid_to_processed = {}
    with engine.connect() as conn:
        result = conn.execute(sql, params)
        for row in result.mappings():
            k = str(row["internal_uuid"])
            v = row["processed_at"]
            # normalize to ISO string for sheet (if datetime-like)
            try:
                processed_val = v.isoformat()
            except Exception:
                processed_val = str(v)
            uuid_to_processed[k] = processed_val

    # Compose values for the two columns to write back (rows 2..n+1)
    internal_uuid_column_values = [[val] for val in final_uuids]  # list of single-element lists (gspread expects 2D)
    processed_col_values = [[ uuid_to_processed.get(val, "") ] for val in final_uuids]

    # Build range strings for batch updates
    start_row = 2
    end_row = total_rows + 1
    uuid_range = gspread.utils.rowcol_to_a1(start_row, uuid_idx) + ":" + gspread.utils.rowcol_to_a1(end_row, uuid_idx)
    processed_range = gspread.utils.rowcol_to_a1(start_row, processed_idx) + ":" + gspread.utils.rowcol_to_a1(end_row, processed_idx)

    worksheet.update(
        values=internal_uuid_column_values,
        range_name=uuid_range,
        value_input_option="RAW"
    )

    worksheet.update(
        values=processed_col_values,
        range_name=processed_range,
        value_input_option="RAW"
    )

    print(f"Sheet updated: wrote {len(final_uuids)} internal_uuid and processed_at values.")

def main():
    eng = get_engine()

    for m in MAPPINGS:
        name = m.get("name") or f"mapping_{m.get('sheet_name')}"
        sheet_id = m.get("sheet_id")
        sheet_name = m.get("sheet_name")
        schema = m.get("schema")
        target_table = m.get("target_table")
        staging_table = m.get("staging_table") or f"{target_table}_stg"
        uuid_col = m.get("uuid_col", "internal_uuid")
        processed_col = m.get("processed_col", "processed_at")

        if not sheet_name or not target_table:
            print(f"[{name}] skipping invalid mapping (missing sheet_name or target_table): {m}")
            continue

        print(f"[{name}] Processing: sheet '{sheet_name}' -> {schema}.{target_table}")

        try:
            rows, worksheet = read_sheet(sheet_id, sheet_name)
        except Exception as e:
            print(f"[{name}] Failed to read sheet '{sheet_name}': {e}")
            traceback.print_exc()
            continue

         # print summary
        print("total rows:", len(rows))
        print("sample row:", rows[0] if rows else None)

        # read header row (do NOT overwrite `rows`)
        header_row = worksheet.row_values(1)
        cols = [ (h.strip().lstrip("\ufeff") or f"_col_{i}") for i,h in enumerate(header_row, start=1) ]
        # ensure mapping uuid/processed columns exist in final schema
        if uuid_col not in cols:
            cols.append(uuid_col)
        if processed_col not in cols:
            cols.append(processed_col)

        drop_table(eng, schema, target_table)
        # create empty staging table from headers even if no data rows
        create_staging_table(eng, schema, staging_table, cols)

        # split and assign UUIDs (still use the rows dicts)
        ins, upd = split_rows(rows, uuid_col)
        ins = assign_uuids(ins, uuid_col)
        print("updates:", len(upd))
        print("inserts:", len(ins))
        all_rows = upd + ins
        print("Staging payload", len(all_rows))

        # load staging (will no-op if all_rows is empty)
        load_staging(eng, schema, staging_table, all_rows)

        # create target table if missing
        create_target_table_if_not_exists(eng, schema, target_table, cols)

        # upsert, then update sheet ONLY if upsert succeeds
        try:
            processed_uuids = upsert_staging_into_target(
                eng,
                schema,
                staging_table,
                target_table,
                cols
            )
            print("Upsert successful, proceeding to update sheet.")

            # update sheet using the rows we already read (avoid another read)
            update_sheet_with_results(eng, worksheet, processed_uuids, schema, target_table, uuid_col, processed_col, original_rows=rows)
            print("Sheet updated. Proceeding to drop staging table")
            # delete staging table
            drop_table(eng, schema, staging_table)

        except Exception as e:
            print("Upsert failed. Sheet will NOT be updated.")
            print("Error:", e)
            traceback.print_exc()
            return

main()

# docker compose build sheets-exporter
# docker compose run --rm sheets-exporter
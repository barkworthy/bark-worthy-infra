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
import warnings
import re
from typing import Iterable, List, Union
from psycopg2.extras import Json
print("done")

# --- config (from env) ---
print("setting env vars")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB")
POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
MODULE_DIR = os.path.dirname(__file__)
MAPPINGS_PATH = os.path.join(MODULE_DIR, "mappings.yml")
print("done")

# --- when loading the file, guard for missing file ---
try:
    with open(MAPPINGS_PATH, "r", encoding="utf-8") as f:
        MAPPINGS = yaml.safe_load(f)
except FileNotFoundError:
    warnings.warn("mappings.yml not found — continuing with empty mappings (tests/CI).")
    MAPPINGS = []

if not isinstance(MAPPINGS, list) or len(MAPPINGS) == 0:
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
    import time
    import random

    for attempt in range(8):
        try:
            ws = gc.open_by_key(sheet_id).worksheet(sheet_name)
            rows = ws.get_all_records()
            return rows, ws

        except Exception as e:
            print(f"[{sheet_name}] FULL ERROR:", repr(e))

            if "429" in str(e):
                wait = (2 ** attempt) + random.uniform(0, 1)
                print(f"[{sheet_name}] Hit Google rate limit. Sleeping {wait:.1f}s then retrying...")
                time.sleep(wait)
                continue
            raise

# Figure out which rows to insert and which to update, depending on whether they already have an internal_uuid
def split_rows(rows, uuid_col):
    inserts = []
    updates = []
    for r in rows:
        uid = (r.get(uuid_col) or "").strip()
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

def normalize_columns(cols: Union[str, Iterable[str]]) -> Union[str, List[str]]:
    def _norm(c: str) -> str:
        s = str(c).strip().lower()
        s = re.sub(r'[^a-z0-9]+', '_', s)
        s = re.sub(r'_+', '_', s).strip('_')
        return s

    # single string
    if isinstance(cols, str):
        return _norm(cols)

    # iterable
    if isinstance(cols, Iterable):
        normalized = []
        seen = {}

        for c in cols:
            if c is None:
                continue

            new = _norm(c)
            normalized.append(new)

            # detect collisions
            if new in seen:
                raise RuntimeError(
                    f"Column name collision after normalization: "
                    f"'{seen[new]}' and '{c}' → '{new}'"
                )
            seen[new] = c

        return normalized

    raise TypeError("normalize_columns expects a string or iterable")


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

def get_table_columns(engine, schema, table):
    # Get all columns from the target table. Will be used later to determine if Gsheet has new/dropped columns.
    sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema
        AND table_name = :table
        ORDER BY ordinal_position
    """
    with engine.connect() as conn:
        res = conn.execute(text(sql), {"schema": schema, "table": table})
        return [r[0] for r in res.fetchall()]

def add_new_columns(engine, schema, table, new_cols):
    # Add new column to target (for if Gsheet has new columns)
    if not new_cols:
        return

    with engine.begin() as conn:
        for c in new_cols:
            print(f"[{table}] Adding new column: {c}")
            conn.execute(text(
                f'ALTER TABLE {schema}."{table}" ADD COLUMN "{c}" TEXT'
            ))

def inject_missing_columns(rows, missing_cols, table_name):
    # For when Gsheet no longer has some columns, that still exist in the target table
    if not missing_cols:
        return rows

    for c in missing_cols:
        print(f"[{table_name}] WARNING: column '{c}' exists in DB but not in sheet. Filling NULLs.")

    for r in rows:
        for c in missing_cols:
            r[c] = None

    return rows

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
def load_staging(engine, schema, target_table, staging_table, rows, batch_size=300):
    if not rows:
        print("no rows to load into staging")
        return

    cols = [c for c in rows[0].keys() if c != "processed_at"]
    quoted_cols = ", ".join([f"\"{c}\"" for c in cols])
    placeholders = ", ".join([f":{c}" for c in cols])
    insert_sql = text(
        f"INSERT INTO {schema}.\"{staging_table}\" ({quoted_cols}, processed_at) VALUES ({placeholders}, NOW())"
    )

    # Remove processed_at from each dict so DB can fill it
    for r in rows:
        if "processed_at" in r:
            r.pop("processed_at")

    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE {schema}.\"{staging_table}\""))
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i+batch_size]
            conn.execute(insert_sql, batch)

    print(f"[{target_table}] Loaded {len(rows)} rows into staging")

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
    if "processed_at" not in cols:
        raise ValueError("cols must include 'processed_at'")

    col_list = ", ".join([f'"{c}"' for c in cols])
    pk = "internal_uuid"
    compare_cols = [c for c in cols if c not in (pk, "processed_at")]

    update_assignments = ", ".join(
        [f'"{c}" = EXCLUDED."{c}"' for c in cols if c != pk]
    )

    change_conditions = " OR ".join(
        [f'target."{c}" IS DISTINCT FROM EXCLUDED."{c}"' for c in compare_cols]
    )

    sql = f"""
        WITH upserted AS (
            INSERT INTO {schema}."{target}" AS target ({col_list})
            SELECT {col_list}
            FROM {schema}."{staging}"
            ON CONFLICT ("{pk}") DO UPDATE SET
                {update_assignments}
            WHERE {change_conditions}
            RETURNING
                "{pk}",
                (xmax = 0) AS inserted
        )
        SELECT
            COUNT(*) FILTER (WHERE inserted) AS inserted,
            COUNT(*) FILTER (WHERE NOT inserted) AS updated
        FROM upserted;
    """

    with engine.begin() as conn:
        # print("COMPARE COLS:", compare_cols)
        # print("FIRST STAGING ROW:")
        # print(conn.execute(text(f'SELECT * FROM {schema}."{staging}" LIMIT 1')).fetchone())
        # print("FIRST TARGET ROW:")
        # print(conn.execute(text(f'SELECT * FROM {schema}."{target}" LIMIT 1')).fetchone())

        result = conn.execute(text(sql)).fetchone()

        inserted = result[0] or 0
        updated = result[1] or 0

        total = conn.execute(
            text(f'SELECT COUNT(*) FROM {schema}."{staging}"')
        ).scalar()

        unchanged = total - (inserted + updated)

        # fetch all uuids from staging for sheet update
        uuid_rows = conn.execute(
            text(f'SELECT "{pk}" FROM {schema}."{staging}"')
        ).fetchall()

        processed_uuids = [r[0] for r in uuid_rows]

        return {
            "inserted": inserted,
            "updated": updated,
            "unchanged": unchanged,
            "processed_uuids": processed_uuids,
        }

def update_sheet_with_results(engine, worksheet, processed_uuids, schema, target_table, uuid_col, processed_col, insert_count, update_count, original_rows=None):

       # defensive checks
    if processed_uuids is None or not isinstance(processed_uuids, (list, tuple)):
        raise ValueError("processed_uuids must be a list/tuple of uuid strings")

    # read header row from sheet and ensure columns exist
    headers = worksheet.row_values(1)
    # normalize header values (strip BOM/spaces)
    headers = [h.strip().lstrip("\ufeff") for h in headers]

    # add missing header columns to the sheet (append to end)
    header_modified = False
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

    print(f"[{target_table}] Sheet updated")
    print(f"[{target_table}] Internal_uuid written: {insert_count}")
    print(f"[{target_table}] Processed_at updated: {update_count}")

def create_deleted_log_table_if_not_exists(engine, schema):
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.deleted_rows_log (
            internal_uuid UUID,
            table_name TEXT,
            deleted_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            deleted_row_json JSONB
        );
    """

    create_index_sql = f"""
        CREATE INDEX IF NOT EXISTS idx_deleted_uuid_table
        ON {schema}.deleted_rows_log (internal_uuid, table_name);
    """

    with engine.begin() as conn:
        conn.execute(text(create_table_sql))
        conn.execute(text(create_index_sql))

def handle_deleted_rows(engine, schema, target_table, sheet_uuids):
    # ---- SAFETY: if sheet has zero uuids, do NOT delete anything ----
    if not sheet_uuids:
        raise RuntimeError(
            f"[{target_table}] Sheet returned zero UUIDs. "
            f"Aborting deletion logic — possible sheet wipe or read failure."
        )

    """
    If a uuid exists in target but not in sheet:
    - copy full row to deleted_rows_log
    - delete from target
    """

    create_deleted_log_table_if_not_exists(engine, schema)

    with engine.begin() as conn:

        # get target uuids
        res = conn.execute(text(f'''
            SELECT internal_uuid
            FROM {schema}."{target_table}"
        '''))
        target_uuids = {str(r[0]) for r in res.fetchall() if r[0]}

        sheet_uuid_set = {str(u) for u in sheet_uuids if u}

        if not sheet_uuid_set:
            raise RuntimeError(
                f"[{target_table}] No valid UUIDs found in sheet. "
                f"Aborting deletion logic."
            )

        to_delete = target_uuids - sheet_uuid_set
        if len(to_delete) > 20:
            print(f"[{target_table}] WARNING: large deletion detected ({len(to_delete)} rows)")

        if not to_delete:
            print(f"[{target_table}] No deleted rows detected.")
            return {"deleted": 0}

        print(f"[{target_table}] Found rows deleted from sheet: {len(to_delete)}")

        # fetch full rows to archive
        placeholders = ", ".join([f":u{i}" for i in range(len(to_delete))])
        params = {f"u{i}": u for i, u in enumerate(to_delete)}

        fetch_sql = text(f'''
            SELECT *
            FROM {schema}."{target_table}"
            WHERE internal_uuid IN ({placeholders})
        ''')

        rows = conn.execute(fetch_sql, params).mappings().all()

        rows = conn.execute(fetch_sql, params).mappings().all()

        if len(rows) != len(to_delete):
            raise RuntimeError(
                f"[{target_table}] Archive mismatch. "
                f"Expected {len(to_delete)} rows but fetched {len(rows)}. Aborting delete."
            )

        # ---------- make JSON safe ----------
        import datetime

        def make_json_safe(v):
            if isinstance(v, uuid.UUID):
                return str(v)
            if isinstance(v, datetime.datetime):
                return v.isoformat()
            return v

        archive_payload = []

        for r in rows:
            safe_row = {k: make_json_safe(v) for k, v in dict(r).items()}
            archive_payload.append({
                "uuid": str(r["internal_uuid"]),
                "table": target_table,
                "row_json": Json(safe_row)
            })

        # ---------- archive FIRST ----------
        insert_sql = text(f'''
            INSERT INTO {schema}.deleted_rows_log
            (internal_uuid, table_name, deleted_timestamp, deleted_row_json)
            VALUES (:uuid, :table, CURRENT_TIMESTAMP, :row_json)
        ''')

        conn.execute(insert_sql, archive_payload)

        print(f"[{target_table}] Archived rows:", len(archive_payload))

        # ---------- THEN delete ----------
        delete_sql = text(f'''
            DELETE FROM {schema}."{target_table}"
            WHERE internal_uuid IN ({placeholders})
        ''')

        conn.execute(delete_sql, params)

        # insert into deleted log
        insert_sql = text(f'''
            INSERT INTO {schema}.deleted_rows_log
            (internal_uuid, table_name, deleted_timestamp, deleted_row_json)
            VALUES (:uuid, :table, CURRENT_TIMESTAMP, :row_json)
        ''')

        print(f"[{target_table}] Deleted rows moved to deleted_rows_log.")

        deleted_count = len(to_delete)
        return {"deleted": deleted_count}

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
            # --- normalize sheet column names ---
            if rows:
                original_cols = list(rows[0].keys())
                normalized_cols = normalize_columns(original_cols)

                col_map = dict(zip(original_cols, normalized_cols))

                # warn if anything changed
                changed = [(o, n) for o, n in col_map.items() if o != n]
                if changed:
                    print(f"[{name}] WARNING: non-snake-case columns detected:")
                    for o, n in changed:
                        print(f"    {o} -> {n}")

                # apply normalization to each row
                new_rows = []
                for r in rows:
                    new_r = {}
                    for k, v in r.items():
                        new_k = col_map.get(k, k)
                        new_r[new_k] = v
                    new_rows.append(new_r)

                rows = new_rows
        except Exception as e:
            print(f"[{name}] Failed to read sheet '{sheet_name}': {e}")
            traceback.print_exc()
            continue

         # print summary
        print(f"[{name}] total rows:", len(rows))
        #print("sample row:", rows[0] if rows else None)

        # read header row (do NOT overwrite `rows`)
        header_row = worksheet.row_values(1)

        clean_headers = [h.strip().lstrip("\ufeff") for h in header_row]

        # ---- HARD GUARD: required system columns must exist ----
        required_cols = {uuid_col, processed_col}
        header_set = set(clean_headers)

        missing = required_cols - header_set
        if missing:
            raise RuntimeError(
                f"[{target_table}] Sheet missing required system columns: {missing}. "
                "Aborting run before any DB writes."
            )

        blank_cols = [i+1 for i,h in enumerate(clean_headers) if not h]

        if blank_cols:
            raise RuntimeError(
                f"[{target_table}] Blank column headers detected at positions: {blank_cols}. "
                f"All columns must have names."
            )

        raw_cols = [(h.strip().lstrip("\ufeff") or f"_col_{i}") for i, h in enumerate(header_row, start=1)]
        cols = normalize_columns(raw_cols)

        # ensure uuid + processed columns exist in schema list
        if uuid_col not in cols:
            cols.append(uuid_col)
        if processed_col not in cols:
            cols.append(processed_col)

        # get existing DB columns (if table exists)
        existing_cols = get_table_columns(eng, schema, target_table)

        if not existing_cols:
            # table doesn't exist yet → normal creation path
            final_cols = cols
        else:
            sheet_cols = set(cols)
            db_cols = set(existing_cols)

            new_cols = sheet_cols - db_cols
            missing_cols = db_cols - sheet_cols

            # add new columns to DB
            add_new_columns(eng, schema, target_table, new_cols)

            # inject NULLs for removed columns
            rows = inject_missing_columns(rows, missing_cols, target_table)

            # final column set = union
            final_cols = list(db_cols.union(sheet_cols))

        # ensure mapping uuid/processed columns exist in final schema
        if uuid_col not in final_cols:
            final_cols.append(uuid_col)
        if processed_col not in final_cols:
            final_cols.append(processed_col)

        # only for testing
        # drop_table(eng, schema, target_table)

        # create empty staging table from headers even if no data rows
        create_staging_table(eng, schema, staging_table, final_cols)

        # split and assign UUIDs (still use the rows dicts)
        ins, upd = split_rows(rows, uuid_col)
        ins = assign_uuids(ins, uuid_col)

        original_sheet_uuids = [r.get(uuid_col) for r in upd if r.get(uuid_col)]

        all_rows = upd + ins
        print(f"[{target_table}] Staging payload", len(all_rows))

        # load staging (will no-op if all_rows is empty)
        load_staging(eng, schema, target_table, staging_table, all_rows)

        # create target table if missing
        create_target_table_if_not_exists(eng, schema, target_table, final_cols)


        new_uuids = [r[uuid_col] for r in ins if r.get(uuid_col)]

        if new_uuids:
            placeholders = ", ".join([f":u{i}" for i in range(len(new_uuids))])
            params = {f"u{i}": u for i, u in enumerate(new_uuids)}

            sql = text(f'''
                SELECT internal_uuid
                FROM {schema}."{target_table}"
                WHERE internal_uuid IN ({placeholders})
            ''')

            with eng.connect() as conn:
                existing = conn.execute(sql, params).fetchall()

            if existing:
                raise RuntimeError(
                    f"[{target_table}] UUID collision detected during generation. "
                    f"Aborting run so UUIDs can be regenerated."
                )


        # upsert, then update sheet ONLY if upsert succeeds
        try:
            res = upsert_staging_into_target(
                eng,
                schema,
                staging_table,
                target_table,
                final_cols
            )
            processed_uuids = res["processed_uuids"]

            print(f"[{target_table}] Inserted: {res['inserted']}")
            print(f"[{target_table}] Updated: {res['updated']}")
            print(f"[{target_table}] Unchanged: {res['unchanged']}")



        except Exception as e:
            print("Target upsert failed. Sheet will NOT be updated.")
            print("Error:", e)
            traceback.print_exc()
            return

        try:
            # update sheet using the rows we already read (avoid another read)
            update_sheet_with_results(
                eng,
                worksheet,
                processed_uuids,
                schema,
                target_table,
                uuid_col,
                processed_col,
                insert_count=res["inserted"],
                update_count=res["updated"],
                original_rows=rows
            )
            print(f"[{target_table}] Sheet updated. Proceeding to drop staging table")

        except Exception as e:
            print("Sheet update failed")
            print("Error:", e)
            traceback.print_exc()
            return

        try:
            # delete staging table
            drop_table(eng, schema, staging_table)

            # detect deletions (rows removed from sheet)
            sheet_uuids = processed_uuids

            deletion_res = handle_deleted_rows(
                eng,
                schema,
                target_table,
                sheet_uuids
            )

            deleted_count = (deletion_res or {}).get("deleted", 0)

            print(f"[{target_table}] Internal_uuid deleted: {deleted_count}")

        except Exception as e:
            print("Deletion handling failed AFTER sheet sync")
            print("Error:", e)
            traceback.print_exc()
            return


if __name__ == "__main__":
    main()
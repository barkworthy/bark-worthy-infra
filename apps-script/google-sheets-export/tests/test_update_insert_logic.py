import sys
import os

sys.path.append(
    os.path.abspath("apps-script/google-sheets-export")
)

from src.sheets_exporter import split_rows

def test_split_rows_insert_vs_update():
    rows = [
        {"internal_uuid": ""},
        {"internal_uuid": "abc"},
        {"internal_uuid": None},
        {"internal_uuid": "def"},
    ]

    ins, upd = split_rows(rows, "internal_uuid")

    assert len(ins) == 2   # empty + None
    assert len(upd) == 2   # abc + def

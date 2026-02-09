import sys
import os

sys.path.append(
    os.path.abspath("apps-script/google-sheets-export")
)

from src.sheets_exporter import assign_uuids

# Make sure the script actually generates uuids
def test_assign_uuids_adds_uuid():
    rows = [{"name": "A"}, {"name": "B"}]
    out = assign_uuids(rows, "internal_uuid")

    assert len(out) == 2
    assert "internal_uuid" in out[0]
    assert out[0]["internal_uuid"] != out[1]["internal_uuid"]

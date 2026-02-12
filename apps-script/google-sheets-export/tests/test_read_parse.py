import sys
import os

sys.path.append(
    os.path.abspath("apps-script/google-sheets-export")
)

# This test verifies that read_sheet() correctly:
# 1. Connects to a Google Sheet (we will fake this)
# 2. Reads rows using get_all_records()
# 3. Returns rows + worksheet object

# We fake Google Sheets so tests run:
# - without internet
# - without credentials

def test_read_sheet_returns_rows(monkeypatch):

    # -------------------------------
    # Step 1 — Fake data returned by Google Sheets
    # -------------------------------
    # This simulates what gspread would normally return.
    # get_all_records() returns list of dicts.
    fake_rows = [
        {"id": "1", "email": "alice@test.com"},
        {"id": "2", "email": "bob@test.com"},
    ]

    # -------------------------------
    # Step 2 — Create fake worksheet
    # -------------------------------
    # Our real code calls:
    # ws.get_all_records()
    #
    # So we create a fake object that has that method.
    class FakeWorksheet:
        def get_all_records(self):
            return fake_rows

    # -------------------------------
    # Step 3 — Fake spreadsheet object
    # -------------------------------
    # Real code does:
    # gc.open_by_key(...).worksheet(...)
    #
    # So we fake both layers.
    class FakeSpreadsheet:
        def worksheet(self, name):
            return FakeWorksheet()

    # -------------------------------
    # Step 4 — Fake gspread client
    # -------------------------------
    # Real code calls:
    # get_gspread_client().open_by_key(...)
    #
    # So we fake client returned by get_gspread_client().
    class FakeClient:
        def open_by_key(self, key):
            return FakeSpreadsheet()

    # -------------------------------
    # Step 5 — Import your real module
    # -------------------------------
    # We import here (inside test) so monkeypatch can modify it.
    from src import sheets_exporter

    # -------------------------------
    # Step 6 — Monkeypatch Google client
    # -------------------------------
    # Replace real get_gspread_client() with fake one.
    # So no real Google API calls happen.
    monkeypatch.setattr(
        sheets_exporter,
        "get_gspread_client",
        lambda: FakeClient()
    )

    # -------------------------------
    # Step 7 — Call real function
    # -------------------------------
    rows, worksheet = sheets_exporter.read_sheet(
        sheet_id="dummy",
        sheet_name="Sheet1"
    )

    # -------------------------------
    # Step 8 — Assertions
    # -------------------------------
    # Verify function returns expected data shape.
    assert rows == fake_rows
    assert isinstance(rows, list)
    assert len(rows) == 2

    # verify worksheet object also returned
    assert worksheet is not None

    # verify content correctness
    assert rows[0]["email"] == "alice@test.com"
    assert rows[1]["email"] == "bob@test.com"
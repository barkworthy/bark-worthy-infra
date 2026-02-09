import pytest
import sys
import os

sys.path.append(
    os.path.abspath("apps-script/google-sheets-export")
)

from src.sheets_exporter import inject_missing_columns, add_new_columns


def test_schema_reconcile_new_and_missing_columns():
    """
    Simulate:
    - DB has: a, b, c
    - Sheet has: a, b, d

    Expected:
    - new column: d
    - missing column: c
    - rows get c=None injected
    """

    # pretend DB already has these
    db_cols = {"a", "b", "c"}

    # pretend sheet now has these
    sheet_cols = {"a", "b", "d"}

    new_cols = sheet_cols - db_cols
    missing_cols = db_cols - sheet_cols

    assert new_cols == {"d"}
    assert missing_cols == {"c"}

    rows = [
        {"a": 1, "b": 2, "d": 3},
        {"a": 4, "b": 5, "d": 6},
    ]

    out = inject_missing_columns(rows, missing_cols, "test_table")

    # missing column should now exist with None
    assert out[0]["c"] is None
    assert out[1]["c"] is None

    # new column should remain untouched
    assert out[0]["d"] == 3
    assert out[1]["d"] == 6


def test_schema_reconcile_no_changes():
    """
    If DB and sheet columns match,
    nothing should be added or injected.
    """

    db_cols = {"a", "b"}
    sheet_cols = {"a", "b"}

    new_cols = sheet_cols - db_cols
    missing_cols = db_cols - sheet_cols

    assert new_cols == set()
    assert missing_cols == set()

    rows = [{"a": 1, "b": 2}]
    out = inject_missing_columns(rows, missing_cols, "test_table")

    assert out == rows


class FakeConn:
    def __init__(self):
        self.executed = []

    def execute(self, sql):
        # capture executed SQL text
        self.executed.append(str(sql))

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class FakeEngine:
    def __init__(self):
        self.conn = FakeConn()

    def begin(self):
        return self.conn


def test_add_new_columns_executes_alter():
    """
    If new columns are detected, ALTER TABLE should run.
    """

    engine = FakeEngine()

    new_cols = {"new_col1", "new_col2"}

    add_new_columns(engine, "test_schema", "test_table", new_cols)

    executed_sql = " ".join(engine.conn.executed)

    assert "ALTER TABLE test_schema.\"test_table\"" in executed_sql
    assert "new_col1" in executed_sql
    assert "new_col2" in executed_sql


def test_add_new_columns_noop_when_empty():
    """
    If no new columns, nothing should run.
    """

    engine = FakeEngine()

    add_new_columns(engine, "test_schema", "test_table", set())

    assert engine.conn.executed == []
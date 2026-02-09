import pytest
import sys
import os

sys.path.append(
    os.path.abspath("apps-script/google-sheets-export")
)

from src.sheets_exporter import normalize_columns

@pytest.mark.parametrize("input_text, expected", [
    ("Product Code", "product_code"),
    ("  Supplier-Name! ", "supplier_name"),
    ("Test.Report/1", "test_report_1"),
    ("__Leading AND trailing__ ", "leading_and_trailing"),
    ("Multiple   Spaces---and__chars", "multiple_spaces_and_chars"),
    ("UPPERCASE", "uppercase"),
    ("already_normalized", "already_normalized"),
    ("123 numbers 45", "123_numbers_45"),
    ("   ", ""),  # all whitespace => empty
    ("!@#$%^", ""),  # only punctuation => empty
])

def test_normalize_basic_cases(input_text, expected):
    assert normalize_columns(input_text) == expected

def test_normalize_idempotent():
    # applying normalization twice should give same result
    examples = [
        "Product Code",
        "Test.Report/1",
        " supplier-name ",
    ]
    for ex in examples:
        first = normalize_columns(ex)
        second = normalize_columns(first)
        assert first == second

def test_normalize_preserves_ascii_alphanum_only():
    # resulting name contains only a-z, 0-9 and underscores
    samples = ["Product Code", "Test.Report/1", " supplier-name "]
    for s in samples:
        out = normalize_columns(s)
        assert out == out.lower()
        assert all(ch.isalnum() or ch == "_" for ch in out)

def test_list_handles_none():
    out = normalize_columns(["Name", None])
    assert "name" in out

# add test that adds new column to schema whenever a new column is detected on gsheets
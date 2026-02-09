import pytest
import sys
import os

sys.path.append(
    os.path.abspath("apps-script/google-sheets-export")
)

from src.sheets_exporter import normalize_columns

# make sure gsheet does not have more than 1 column with the same name, after normalization
def test_normalize_collision_raises():
    with pytest.raises(RuntimeError):
        normalize_columns(["Payment Method", "payment-method"])

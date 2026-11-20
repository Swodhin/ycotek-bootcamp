
import re
from pathlib import Path


def test_silver_pos_data_quality_rules():
    sql_path = (
        Path(__file__).resolve().parents[1]
        / "include"
        / "sql"
        / "03_Silver_Transformation.sql"
    )

    sql = sql_path.read_text(encoding="utf-8")

    # Normalize whitespace to make checks independent of formatting.
    sql = re.sub(r"\s+", " ", sql).upper()

    # 1. Invalid POS records must be quarantined.
    assert "INSERT INTO SILVER_3.QUARANTINE_RECORDS" in sql
    assert "'MISSING ITEM ID'" in sql
    assert "'MISSING QUANTITY'" in sql
    assert "'NEGATIVE QUANTITY FOR SALE'" in sql
    assert "'ITEM ID NOT FOUND IN ITEM MASTER'" in sql

    # 2. Duplicate transaction lines must be identified.
    assert "'DUPLICATE TRANSACTION LINE'" in sql
    assert "QUALIFY ROW_NUMBER() OVER" in sql
    assert ") > 1" in sql

    # 3. Negative SALE quantities must be excluded from valid sales.
    assert "UPPER(TRIM(P.TRANSACTION_TYPE)) = 'SALE' AND P.QUANTITY < 0" in sql

    # Negative RETURN quantities must not be excluded by that condition.
    assert "P.QUANTITY IS NOT NULL AND NOT ( UPPER(TRIM(P.TRANSACTION_TYPE)) = 'SALE' AND P.QUANTITY < 0 )" in sql

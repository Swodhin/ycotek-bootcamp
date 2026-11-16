-- Assignment 2.2: One-day inventory reload demonstration
-- Purpose: Safely demonstrate a date-scoped reload without changing the
-- production Silver inventory table.
--
-- Selected snapshot date: 2026-10-04
-- Source file: rms/soh_full/ITEM_LOC_SOH_FULL_20261004.csv
--
-- IMPORTANT:
-- This script uses a TEMPORARY TABLE (INVENTORY_RELOAD_DEMO).
-- It does NOT modify RETAIL_DW.SILVER.INVENTORY_SNAPSHOT.

USE WAREHOUSE RETAIL_WH;
USE DATABASE RETAIL_DW;
USE SCHEMA SILVER;

-- 1. Create an isolated copy of the selected date.
CREATE OR REPLACE TEMPORARY TABLE INVENTORY_RELOAD_DEMO AS
SELECT *
FROM RETAIL_DW.SILVER.INVENTORY_SNAPSHOT
WHERE SNAPSHOT_DATE = '2026-10-04';

-- Expected initial row count: 17,440
SELECT COUNT(*) AS INITIAL_ROWS
FROM INVENTORY_RELOAD_DEMO;

-- 2. Simulate a bad load by deleting one row from the temporary copy.
DELETE FROM INVENTORY_RELOAD_DEMO
WHERE ITEM_ID = '10000002'
  AND LOCATION_ID = '1101'
  AND LOCATION_TYPE = 'S';

-- Expected simulated-error row count: 17,439
SELECT COUNT(*) AS ROWS_AFTER_SIMULATED_ERROR
FROM INVENTORY_RELOAD_DEMO;

-- 3. Reload only the selected date from Bronze.
DELETE FROM INVENTORY_RELOAD_DEMO
WHERE SNAPSHOT_DATE = '2026-10-04';

INSERT INTO INVENTORY_RELOAD_DEMO (
    ITEM_ID,
    LOCATION_ID,
    LOCATION_TYPE,
    SNAPSHOT_DATE,
    STOCK_ON_HAND,
    SOURCE_FILE,
    SOURCE_ROW_NUMBER,
    INGESTED_AT
)
SELECT
    C1,
    C2,
    C3,
    TRY_TO_DATE(C4, 'YYYYMMDD'),
    TRY_TO_NUMBER(C5),
    SOURCE_FILE,
    SOURCE_ROW_NUMBER,
    INGESTED_AT
FROM RETAIL_DW.BRONZE.RAW_SOURCE_ROWS
WHERE SOURCE_FILE = 'rms/soh_full/ITEM_LOC_SOH_FULL_20261004.csv'
  AND TRY_TO_DATE(C4, 'YYYYMMDD') = '2026-10-04'
  AND TRY_TO_NUMBER(C5) IS NOT NULL;

-- 4. Validate the reload.
-- Expected TOTAL_ROWS = 17,440 and RESTORED_ROW = 1.
SELECT
    COUNT(*) AS TOTAL_ROWS,
    COUNT_IF(
        ITEM_ID = '10000002'
        AND LOCATION_ID = '1101'
        AND LOCATION_TYPE = 'S'
    ) AS RESTORED_ROW
FROM INVENTORY_RELOAD_DEMO;

-- The successful demonstration produced:
-- INITIAL_ROWS                  = 17,440
-- ROWS_AFTER_SIMULATED_ERROR    = 17,439
-- Number of rows inserted       = 17,440
-- TOTAL_ROWS                    = 17,440
-- RESTORED_ROW                  = 1

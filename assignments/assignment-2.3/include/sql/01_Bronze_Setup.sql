
-- ============================================================
-- ASSIGNMENT 2.3: BRONZE LAYER SETUP
-- Purpose: Store raw source data and ingestion metadata.
-- ============================================================

USE DATABASE RETAIL_DW;

CREATE SCHEMA IF NOT EXISTS BRONZE_3;

-- ============================================================
-- 1. ITEM MASTER RAW
-- ============================================================

CREATE TABLE IF NOT EXISTS BRONZE_3.ITEM_MASTER_RAW (
    ITEM_ID            VARCHAR,
    ITEM_PARENT        VARCHAR,
    ITEM_LEVEL         NUMBER,
    DEPT_ID            VARCHAR,
    CLASS_ID           VARCHAR,
    SUBCLASS_ID        VARCHAR,
    SOURCE_FILE        VARCHAR,
    SOURCE_ROW_NUMBER  NUMBER,
    INGESTED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- 2. STORE MASTER RAW
-- ============================================================

CREATE TABLE IF NOT EXISTS BRONZE_3.STORE_MASTER_RAW (
    STORE_ID            NUMBER,
    STORE_NAME          VARCHAR,
    STORE_NAME10        VARCHAR,
    STORE_TYPE          VARCHAR,
    DISTRICT_ID         VARCHAR,
    CHANNEL_ID          VARCHAR,
    CURRENCY_CODE       VARCHAR,
    STORE_OPEN_DATE     DATE,
    STORE_CLOSE_DATE    DATE,
    COUNTRY             VARCHAR,
    STATE               VARCHAR,
    CITY                VARCHAR,
    POSTAL_CODE         VARCHAR,
    TOTAL_SQUARE_FT     NUMBER,
    COMP_FLAG           VARCHAR,
    SOURCE_FILE         VARCHAR,
    SOURCE_ROW_NUMBER   NUMBER,
    INGESTED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- 3. STORE REGION MAPPING RAW
-- ============================================================

CREATE TABLE IF NOT EXISTS BRONZE_3.STORE_REGION_MAPPING_RAW (
    STORE_ID            NUMBER,
    STORE_CODE          VARCHAR,
    REGION_NAME         VARCHAR,
    DISTRICT_NAME       VARCHAR,
    SOURCE_FILE         VARCHAR,
    SOURCE_ROW_NUMBER   NUMBER,
    INGESTED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- 4. POS SALES RAW
-- ============================================================

CREATE TABLE IF NOT EXISTS BRONZE_3.POS_SALES_RAW (
    LOCATION_ID           NUMBER,
    WORKSTATION_ID        NUMBER,
    TRANSACTION_SEQUENCE  NUMBER,
    LINE_SEQUENCE         NUMBER,
    BUSINESS_DATE         DATE,
    BEGIN_DATETIME        TIMESTAMP_NTZ,
    TRANSACTION_TYPE      VARCHAR,
    ITEM_ID               VARCHAR,
    ITEM_SCAN_ID          VARCHAR,
    QUANTITY              NUMBER(18,3),
    UNIT_PRICE            NUMBER(18,2),
    EXTENDED_AMOUNT       NUMBER(18,2),
    DISCOUNT_AMOUNT       NUMBER(18,2),
    TAX_AMOUNT            NUMBER(18,2),
    NET_AMOUNT            NUMBER(18,2),
    CURRENCY              VARCHAR,
    CASHIER_ID            VARCHAR,
    SOURCE_FILE           VARCHAR,
    SOURCE_ROW_NUMBER     NUMBER,
    INGESTED_AT           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- 5. INVENTORY SNAPSHOT RAW
-- ============================================================

CREATE TABLE IF NOT EXISTS BRONZE_3.INVENTORY_SNAPSHOT_RAW (
    ITEM_ID              VARCHAR,
    LOCATION_ID          NUMBER,
    LOCATION_TYPE        VARCHAR,
    SNAPSHOT_DATE        DATE,
    STOCK_ON_HAND        NUMBER(18,3),
    SOURCE_FILE          VARCHAR,
    SOURCE_ROW_NUMBER    NUMBER,
    INGESTED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- 6. CSV FILE FORMAT
-- ============================================================

CREATE FILE FORMAT IF NOT EXISTS BRONZE_3.CSV_FORMAT
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null')
    EMPTY_FIELD_AS_NULL = TRUE
    TRIM_SPACE = TRUE;

-- ============================================================
-- 7. INTERNAL SNOWFLAKE STAGE
-- ============================================================

CREATE STAGE IF NOT EXISTS BRONZE_3.RETAIL_STAGE
    FILE_FORMAT = BRONZE_3.CSV_FORMAT;
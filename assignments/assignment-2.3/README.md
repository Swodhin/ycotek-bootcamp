# Assignment 2.3 – Retail Data Engineering Pipeline

## 1. Project Overview

This project implements an end-to-end retail data engineering pipeline using Apache Airflow and Snowflake.

The pipeline processes five consecutive days of retail data, including item master, POS sales, inventory snapshots, store master, and store-region mapping files.

It follows a Medallion Architecture consisting of Bronze, Silver, and Gold layers.

## 2. Architecture

### Bronze Layer
- Loads raw CSV files into Snowflake.
- Preserves source file names and row numbers.
- Uses Snowflake stages and COPY INTO for ingestion.
- Supports reprocessing of source files.

### Silver Layer
- Cleans and standardizes raw data.
- Validates records.
- Identifies missing item IDs, negative sales quantities, missing item master references, and duplicate transaction lines.
- Stores invalid records in a quarantine table.
- Retains valid records for Gold processing.

### Gold Layer
- Creates dimensional and fact tables.
- Implements SCD Type 2 for DIM_ITEM and DIM_LOCN.
- Implements SCD Type 1 for STORE_NAME10 in DIM_LOCN.
- Loads sales and inventory facts.
- Generates daily sales and inventory reports.

## 3. Technologies Used

- Apache Airflow 3.3.2
- Docker Desktop
- Snowflake
- Python
- SQL
- GitHub
- Pytest

## 4. Project Structure

```text
assignment-2.3/
├── dags/
│   └── assignment_2_3_pipeline.py
├── data/
│   ├── inventory_snapshot/
│   ├── item_master/
│   ├── pos_sales/
│   └── store_master/
├── docs/
├── evidence/
├── include/
│   ├── config/
│   │   └── assignment_2_3.json
│   └── sql/
│       ├── 01_Bronze_Setup.sql
│       ├── 02_Silver_Setup.sql
│       ├── 03_Silver_Transformation.sql
│       ├── 04_Gold_Setup.sql
│       └── 05_Gold_Transformation.sql
└── tests/
    ├── test_dag_parse.py
    ├── test_dag_no_cycles.py
    └── test_task_retries.py
```

## 5. How to Run the Pipeline

1. Start Docker Desktop.
2. Start the Airflow services using Docker Compose.
3. Open the Airflow UI at http://localhost:8080.
4. Locate the `assignment_2_3_pipeline` DAG.
5. Trigger the DAG.
6. Monitor task execution in the Airflow Grid or Graph view.

## 6. Data Quality and Validation

The pipeline checks for:
- Missing item IDs.
- Negative quantities for sales.
- Items missing from the Item Master.
- Duplicate transaction lines.
- Invalid inventory records.

Invalid records are stored in the quarantine table with their source file, row number, and reason.

## 7. Historical Data Management

- DIM_ITEM: SCD Type 2 logic implemented.
- DIM_LOCN: SCD Type 2 history verified using Store 1101's district change.
- DIM_LOCN: SCD Type 1 logic implemented for STORE_NAME10.
- FCT_INVENTORY: Historical inventory snapshots are retained with effective dates and current-record indicators.

**DIM_ITEM SCD2 validation note:** SCD2 change-detection logic is implemented for DIM_ITEM. However, the five supplied item-master snapshots contain no changes in the tracked attributes, so no historical item versions were generated. The logic is ready to create new versions when attribute changes occur.

## 8. Reconciliation

Gold sales are reconciled against Silver sales for all five business dates.

The reconciliation differences were zero for every date:

- 2026-10-01
- 2026-10-02
- 2026-10-03
- 2026-10-04
- 2026-10-05

## 9. Idempotency

The pipeline was executed more than once to verify that rerunning the same data does not create duplicate records or change the final row counts.

## 10. Automated Testing

The following automated tests were executed successfully using Pytest:

- DAG loads without import errors.
- DAG contains no dependency cycles.
- All tasks have retry configurations.

All three tests passed.
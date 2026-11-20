# Assignment 2.3 – Retail Data Engineering Pipeline

## 1. Project Overview

This project implements an end-to-end retail data engineering pipeline using Apache Airflow and Snowflake.

It processes five consecutive days of retail data, including item master, POS sales, inventory snapshots, store master, and store-region mapping files.

The pipeline follows the Medallion Architecture: Bronze, Silver, and Gold.

## 2. Architecture

### Bronze Layer
- Loads raw CSV files into Snowflake.
- Preserves source file names and row numbers.
- Uses Snowflake stages and `COPY INTO` for ingestion.
- Supports source-file reprocessing.

### Silver Layer
- Cleans and standardizes raw data.
- Validates records.
- Identifies missing item IDs, negative sales quantities, missing item master references, and duplicate transaction lines.
- Stores invalid sales records in quarantine.
- Retains valid records for Gold processing.

### Gold Layer
- Creates dimension and fact tables.
- Implements SCD Type 2 for `DIM_ITEM` and `DIM_LOCN`.
- Implements SCD Type 1 for `STORE_NAME10` in `DIM_LOCN`.
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
    ├── test_dag_dependencies.py
    ├── test_dag_no_cycles.py
    ├── test_task_retries.py
    ├── test_bronze_rerun.py
    └── test_silver_quality.py
```

## 5. How to Run the Pipeline

1. Start Docker Desktop.
2. Start the Airflow services using Docker Compose.
3. Open the Airflow UI at [http://localhost:8080](http://localhost:8080).
4. Locate the `assignment_2_3_pipeline` DAG.
5. Trigger the DAG.
6. Monitor execution in the Airflow Grid or Graph view.

## 6. Backfilling Historical Dates

The pipeline supports processing historical data for October 1–5, 2026.

To process a specific date:

1. Open the Airflow UI and locate `assignment_2_3_pipeline`.
2. Trigger the DAG with the following configuration, replacing the date as needed:

```json
{
  "run_date": "20261001"
}
```

Supported dates:

- `20261001`
- `20261002`
- `20261003`
- `20261004`
- `20261005`

The pipeline processes the source files selected for the specified date.

## 7. Data Quality and Validation

The pipeline checks for:

- Missing item IDs.
- Negative sales quantities.
- Items missing from the Item Master.
- Duplicate transaction lines.
- Invalid inventory quantities.

Invalid sales records are stored in quarantine with their source file, row number, and reason. Negative inventory quantities were identified in the source data and retained in inventory history.

## 8. Historical Data Management

- `DIM_ITEM`: SCD Type 2 change-detection logic implemented.
- `DIM_LOCN`: SCD Type 2 history verified using Store 1101's district change.
- `DIM_LOCN`: SCD Type 1 implemented for `STORE_NAME10`.
- `FCT_INVENTORY`: Historical inventory snapshots are retained with effective dates and current-record indicators.

**DIM_ITEM SCD Type 2 validation:** The five supplied item-master snapshots contain no changes in tracked attributes. Therefore, no historical item versions were generated, although the change-detection logic is implemented.

## 9. Reconciliation

Gold sales were reconciled against Silver sales for all five business dates.

The reconciliation difference was zero for every date:

- 2026-10-01
- 2026-10-02
- 2026-10-03
- 2026-10-04
- 2026-10-05

The reconciliation task raises an error if the totals do not match within the configured tolerance.

## 10. Idempotency

The pipeline was executed more than once to verify that rerunning the same data does not create duplicate records or change the final row counts.

## 11. Automated Testing

The project includes six automated tests implemented using Pytest.

### Test Coverage

1. **DAG Parsing:** Verifies that the DAG loads without import errors.
2. **DAG Dependencies:** Verifies the expected task dependencies.
3. **DAG Cycles:** Ensures that the DAG contains no dependency cycles.
4. **Task Retries:** Verifies that tasks have retry configurations.
5. **Bronze Rerun:** Checks that existing records are removed before forced re-ingestion.
6. **Silver Data Quality:** Checks that SQL contains the expected validation, quarantine, and duplicate-handling rules.

### Running the Tests

Execute the following command from the Airflow directory:

```bash
docker compose exec airflow-worker python -m pytest /opt/airflow/assignment-2.3/tests -v
```

### Test Results

All six automated tests passed successfully.

**Result: 6 passed, 0 failed.**

Screenshots of the test results are available in the `evidence/` folder.

## 12. Known Limitations

- The supplied item-master snapshots contain no tracked attribute changes, so DIM_ITEM historical versions could not be demonstrated with the provided data.
- Negative inventory quantities exist in the supplied source data. They are retained in inventory history and represent an upstream data-quality issue.
- Automated tests validate DAG structure and selected SQL logic; they do not execute the complete pipeline against a live Snowflake environment.
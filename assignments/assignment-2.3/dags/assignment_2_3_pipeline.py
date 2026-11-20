
import json
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task, get_current_context
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


# ============================================================
# 1. CONFIGURATION
# ============================================================

ASSIGNMENT_DIRECTORY = (
    Path(__file__).resolve().parents[2] / "assignment-2.3"
)

CONFIG_FILE = (
    ASSIGNMENT_DIRECTORY
    / "include"
    / "config"
    / "assignment_2_3.json"
)


def read_config():
    """Read pipeline settings from the external JSON configuration."""
    with CONFIG_FILE.open("r", encoding="utf-8") as file:
        return json.load(file)


def get_processing_date():
    """
    Get the processing date from manual trigger configuration.
    If no run_date is supplied, fall back to Airflow's data interval.
    """

    context = get_current_context()
    dag_run = context.get("dag_run")

    run_conf = dag_run.conf or {} if dag_run else {}

    run_date = run_conf.get("run_date")

    if not run_date:
        run_date = context["data_interval_start"].strftime("%Y%m%d")

    # Validate YYYYMMDD format.
    try:
        parsed_date = datetime.strptime(run_date, "%Y%m%d")
    except (TypeError, ValueError):
        raise ValueError(
            "Invalid run_date. Use YYYYMMDD format, "
            "for example 20261001."
        )

    if parsed_date.strftime("%Y%m%d") != run_date:
        raise ValueError("run_date must use YYYYMMDD format.")

    # This DAG is currently configured for October 1–5.
    if run_date not in {
        "20261001",
        "20261002",
        "20261003",
        "20261004",
        "20261005",
    }:
        raise ValueError(
            f"Date {run_date} is outside the assignment processing period."
        )

    return run_date


# ============================================================
# 2. DEFINE DAG
# ============================================================

@dag(
    dag_id="assignment_2_3_pipeline",
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["assignment_2_3", "pipeline"],
)
def assignment_2_3_pipeline():

    config = read_config()

    # ========================================================
    # 3. GET SOURCE FILE SPECIFICATIONS FOR CURRENT RUN
    # ========================================================

    @task
    def get_file_specs():

        run_date = get_processing_date()

        all_files = read_config()["files"]

        selected_files = [
            spec
            for spec in all_files
            if (
                spec["file"] == "store_region_mapping.csv"
                or spec["file"].endswith(f"{run_date}.csv")
                or (
                    run_date == "20261001"
                    and spec["file"] == "store_master_20260924.csv"
                )
            )
        ]

        # Check that the required daily source files exist
        # in the configuration for this processing date.
        required_folders = {
            "item_master",
            "pos_sales",
            "inventory_snapshot",
        }

        selected_folders = {
            spec["folder"]
            for spec in selected_files
        }

        missing_folders = required_folders - selected_folders

        if missing_folders:
            raise ValueError(
                f"Missing source file specifications for {run_date}: "
                f"{sorted(missing_folders)}"
            )

        if not selected_files:
            raise ValueError(
                f"No source files found for {run_date}"
            )

        print(f"Processing date: {run_date}")
        print(f"Selected {len(selected_files)} source files:")

        for spec in selected_files:
            print(f'- {spec["file"]}')

        return selected_files

    @task
    def get_sensor_paths(file_specs):

        return [
            f'{spec["folder"]}/{spec["file"]}'
            for spec in file_specs
        ]

    file_specs = get_file_specs()
    sensor_paths = get_sensor_paths(file_specs)

    # ========================================================
    # 4. WAIT FOR SOURCE FILES
    # ========================================================

    wait_for_files = FileSensor.partial(
        task_id="wait_for_source_files",
        fs_conn_id=config["filesystem_connection_id"],
        poke_interval=config["sensors"]["poke_interval"],
        timeout=config["sensors"]["timeout"],
        mode="reschedule",
    ).expand(filepath=sensor_paths)

    # ========================================================
    # 5. LOAD SOURCE FILES INTO BRONZE
    # ========================================================

    @task
    def ingest_file(file_spec):

        task_config = read_config()

        data_directory = Path(task_config["data_directory"])

        local_file = (
            data_directory
            / file_spec["folder"]
            / file_spec["file"]
        )

        if not local_file.is_file():
            raise FileNotFoundError(
                f"Source file not found: {local_file}"
            )

        hook = SnowflakeHook(
            snowflake_conn_id=task_config["snowflake_connection_id"]
        )

        connection = hook.get_conn()
        cursor = connection.cursor()

        try:

            stage = task_config["stage"]
            stage_folder = file_spec["stage_folder"]
            file_name = file_spec["file"]

            # Upload the local file to Snowflake stage.
            put_sql = f"""
                PUT file://{local_file}
                @{stage}/{stage_folder}/
                AUTO_COMPRESS=FALSE
                OVERWRITE=TRUE
            """

            cursor.execute(put_sql)

            columns = file_spec["columns"]

            target_columns = ", ".join(
                columns + [
                    "SOURCE_FILE",
                    "SOURCE_ROW_NUMBER",
                    "INGESTED_AT",
                ]
            )

            source_columns = ", ".join(
                f"${index}"
                for index in range(1, len(columns) + 1)
            )

            select_columns = f"""
                {source_columns},
                METADATA$FILENAME,
                METADATA$FILE_ROW_NUMBER,
                CURRENT_TIMESTAMP()
            """

            safe_file_name = file_name.replace("'", "''")

            # Remove existing Bronze rows for this exact file.
            delete_sql = f"""
                DELETE FROM
                    {task_config["database"]}.
                    {task_config["bronze_schema"]}.
                    {file_spec["table"]}
                WHERE RIGHT(
                    SOURCE_FILE,
                    LENGTH('{safe_file_name}')
                ) = '{safe_file_name}'
            """

            cursor.execute(delete_sql)

            # Reload the file into Bronze.
            copy_sql = f"""
                COPY INTO
                    {task_config["database"]}.
                    {task_config["bronze_schema"]}.
                    {file_spec["table"]}
                ({target_columns})
                FROM (
                    SELECT {select_columns}
                    FROM @{stage}/{stage_folder}/
                )
                FILES = ('{safe_file_name}')
                FILE_FORMAT = (
                    FORMAT_NAME = '{task_config["file_format"]}'
                )
                FORCE = TRUE
                ON_ERROR = 'ABORT_STATEMENT'
            """

            cursor.execute(copy_sql)

            connection.commit()

            print(
                f"Bronze ingestion completed: {file_name}"
            )

        except Exception:
            connection.rollback()
            raise

        finally:
            cursor.close()
            connection.close()

    bronze_loads = ingest_file.expand(file_spec=file_specs)

    # ========================================================
    # 6. EXECUTE SQL FILES
    # ========================================================

    
    @task
    def execute_sql_file(sql_file_name):

        task_config = read_config()

        sql_directory = Path(task_config["sql_directory"])
        sql_file = sql_directory / sql_file_name

        if not sql_file.is_file():
            raise FileNotFoundError(
                f"SQL file not found: {sql_file}"
            )

        sql = sql_file.read_text(encoding="utf-8")

        run_date = get_processing_date()
        sql = sql.replace("{{RUN_DATE}}", run_date)

        hook = SnowflakeHook(
            snowflake_conn_id=task_config["snowflake_connection_id"]
        )

        connection = hook.get_conn()
        cursor = connection.cursor()

        try:
            # Commit each successful SQL statement independently.
            connection.autocommit(True)

            statements = hook.split_sql_string(sql)

            for statement in statements:
                if statement.strip():
                    cursor.execute(statement)

            print(f"Executed SQL file: {sql_file_name}")
            print(f"Processing date: {run_date}")

        except Exception:
            connection.rollback()
            raise

        finally:
            cursor.close()
            connection.close()

    # BRONZE AND SILVER SETUP / TRANSFORMATION

    bronze_setup = execute_sql_file.override(
        task_id="bronze_setup"
    )("01_Bronze_Setup.sql")

    silver_setup = execute_sql_file.override(
        task_id="silver_setup"
    )("02_Silver_Setup.sql")

    silver_transformation = execute_sql_file.override(
        task_id="silver_transformation"
    )("03_Silver_Transformation.sql")

    # ========================================================
    # 7. QUARANTINE SUMMARY FOR CURRENT RUN
    # ========================================================

    @task
    def quarantine_summary():

        task_config = read_config()

        run_date = get_processing_date()

        hook = SnowflakeHook(
            snowflake_conn_id=task_config["snowflake_connection_id"]
        )

        quarantine_table = (
            f'{task_config["database"]}.'
            f'{task_config["silver_schema"]}.'
            "QUARANTINE_RECORDS"
        )

        query = f"""
            SELECT
                SOURCE_FILE,
                REASON,
                COUNT(*) AS QUARANTINED_COUNT
            FROM {quarantine_table}
            WHERE
                REGEXP_SUBSTR(SOURCE_FILE, '[0-9]{{8}}') = '{run_date}'
                OR SOURCE_FILE ILIKE '%store_region_mapping.csv'
                OR (
                    '{run_date}' = '20261001'
                    AND SOURCE_FILE ILIKE '%store_master_20260924.csv'
                )
            GROUP BY
                SOURCE_FILE,
                REASON
            ORDER BY
                SOURCE_FILE,
                REASON
        """

        results = hook.get_records(query)

        print("===== QUARANTINE SUMMARY =====")
        print(f"Processing date: {run_date}")

        total_records = 0

        for source_file, reason, count in results:

            total_records += count

            print(
                f"File: {source_file} | "
                f"Reason: {reason} | "
                f"Count: {count}"
            )

        print(
            f"Total quarantined records: {total_records}"
        )

    quarantine_report = quarantine_summary()

    # ========================================================
    # 8. GOLD SETUP AND TRANSFORMATION
    # ========================================================

    gold_setup = execute_sql_file.override(
        task_id="gold_setup"
    )("04_Gold_Setup.sql")

    gold_transformation = execute_sql_file.override(
        task_id="gold_transformation"
    )("05_Gold_Transformation.sql")

    # ========================================================
    # 9. SALES RECONCILIATION FOR CURRENT RUN DATE
    # ========================================================

    @task
    def reconcile_sales():

        task_config = read_config()

        run_date = get_processing_date()

        hook = SnowflakeHook(
            snowflake_conn_id=task_config["snowflake_connection_id"]
        )

        silver_table = (
            f'{task_config["database"]}.'
            f'{task_config["silver_schema"]}.'
            "POS_SALES_CLEAN"
        )

        gold_table = (
            f'{task_config["database"]}.'
            f'{task_config["gold_schema"]}.'
            "FCT_SALES"
        )

        date_dimension = (
            f'{task_config["database"]}.'
            f'{task_config["gold_schema"]}.'
            "DIM_DATE"
        )

        query = f"""
            WITH SILVER_TOTAL AS (
                SELECT
                    BUSINESS_DATE,
                    SUM(NET_AMOUNT) AS NET_SALES
                FROM {silver_table}
                WHERE BUSINESS_DATE = TO_DATE('{run_date}', 'YYYYMMDD')
                GROUP BY BUSINESS_DATE
            ),

            GOLD_TOTAL AS (
                SELECT
                    D.FULL_DATE AS BUSINESS_DATE,
                    SUM(F.NET_AMOUNT) AS NET_SALES
                FROM {gold_table} F
                JOIN {date_dimension} D
                    ON F.DATE_KEY = D.DATE_KEY
                WHERE D.FULL_DATE = TO_DATE('{run_date}', 'YYYYMMDD')
                GROUP BY D.FULL_DATE
            )

            SELECT
                COALESCE(
                    S.BUSINESS_DATE,
                    G.BUSINESS_DATE
                ) AS BUSINESS_DATE,

                COALESCE(S.NET_SALES, 0) AS SILVER_NET_SALES,

                COALESCE(G.NET_SALES, 0) AS GOLD_NET_SALES,

                COALESCE(S.NET_SALES, 0)
                    - COALESCE(G.NET_SALES, 0) AS DIFFERENCE

            FROM SILVER_TOTAL S

            FULL OUTER JOIN GOLD_TOTAL G
                ON S.BUSINESS_DATE = G.BUSINESS_DATE

            ORDER BY BUSINESS_DATE
        """

        results = hook.get_records(query)

        if not results:
            raise ValueError(
                f"Reconciliation returned no results for {run_date}."
            )

        all_matched = True

        print("===== SALES RECONCILIATION =====")
        print(f"Processing date: {run_date}")

        for business_date, silver_total, gold_total, difference in results:

            print(
                f"Date: {business_date} | "
                f"Silver: {silver_total} | "
                f"Gold: {gold_total} | "
                f"Difference: {difference}"
            )

            if (
                difference is None
                or abs(float(difference)) > 0.01
            ):
                all_matched = False

        if not all_matched:
            raise ValueError(
                f"Sales reconciliation failed for {run_date}. "
                "Silver and Gold totals do not match."
            )

        print(
            f"Sales reconciliation passed for {run_date}."
        )

    reconciliation = reconcile_sales()

    # ========================================================
    # 10. TASK DEPENDENCIES
    # ========================================================

    bronze_setup >> silver_setup

    silver_setup >> wait_for_files

    wait_for_files >> bronze_loads

    bronze_loads >> silver_transformation

    silver_transformation >> quarantine_report

    quarantine_report >> gold_setup

    gold_setup >> gold_transformation

    gold_transformation >> reconciliation


assignment_2_3_pipeline()

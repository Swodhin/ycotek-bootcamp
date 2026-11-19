
import json
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


# Locate the assignment folder relative to this DAG file.
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


@dag(
    dag_id="assignment_2_3_pipeline",
    start_date=datetime(2026, 10, 1),
    schedule=None,
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

    # 1. GET SOURCE FILE SPECIFICATIONS

    @task
    def get_file_specs():
        return read_config()["files"]

    @task
    def get_sensor_paths(file_specs):
        return [
            f'{spec["folder"]}/{spec["file"]}'
            for spec in file_specs
        ]

    file_specs = get_file_specs()
    sensor_paths = get_sensor_paths(file_specs)

    # 2. WAIT FOR ALL CONFIGURED SOURCE FILES

    wait_for_files = FileSensor.partial(
        task_id="wait_for_source_files",
        fs_conn_id=config["filesystem_connection_id"],
        poke_interval=config["sensors"]["poke_interval"],
        timeout=config["sensors"]["timeout"],
        mode="reschedule",
    ).expand(filepath=sensor_paths)

    # 3. LOAD SOURCE FILES INTO BRONZE

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

            # Upload the local file to its stage folder.
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

            # Remove existing Bronze rows for this exact source filename.
            # RIGHT() avoids SQL LIKE wildcard issues with underscores.
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

            # Reload the source file, even if Snowflake loaded it before.
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

    # 4. EXECUTE SQL FILES

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

        hook = SnowflakeHook(
            snowflake_conn_id=task_config["snowflake_connection_id"]
        )

        hook.run(
            sql=sql,
            split_statements=True,
        )

        print(f"Executed SQL file: {sql_file_name}")

    bronze_setup = execute_sql_file.override(
        task_id="bronze_setup"
    )("01_Bronze_Setup.sql")

    silver_setup = execute_sql_file.override(
        task_id="silver_setup"
    )("02_Silver_Setup.sql")

    silver_transformation = execute_sql_file.override(
        task_id="silver_transformation"
    )("03_Silver_Transformation.sql")

    # 5. QUARANTINE SUMMARY

    @task
    def quarantine_summary():

        task_config = read_config()

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
            GROUP BY SOURCE_FILE, REASON
            ORDER BY SOURCE_FILE, REASON
        """

        results = hook.get_records(query)

        print("===== QUARANTINE SUMMARY =====")

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

    # 6. GOLD SETUP AND TRANSFORMATION

    gold_setup = execute_sql_file.override(
        task_id="gold_setup"
    )("04_Gold_Setup.sql")

    gold_transformation = execute_sql_file.override(
        task_id="gold_transformation"
    )("05_Gold_Transformation.sql")

    # 7. SALES RECONCILIATION

    @task
    def reconcile_sales():

        task_config = read_config()

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

        query = f"""
            WITH SILVER_TOTAL AS (
                SELECT BUSINESS_DATE, SUM(NET_AMOUNT) AS NET_SALES
                FROM {silver_table}
                GROUP BY BUSINESS_DATE
            ),
            GOLD_TOTAL AS (
                SELECT D.FULL_DATE AS BUSINESS_DATE,
                       SUM(F.NET_AMOUNT) AS NET_SALES
                FROM {gold_table} F
                JOIN {task_config["database"]}.{task_config["gold_schema"]}.DIM_DATE D
                    ON F.DATE_KEY = D.DATE_KEY
                GROUP BY D.FULL_DATE
            )
            SELECT
                COALESCE(S.BUSINESS_DATE, G.BUSINESS_DATE) AS BUSINESS_DATE,
                COALESCE(S.NET_SALES, 0) AS SILVER_NET_SALES,
                COALESCE(G.NET_SALES, 0) AS GOLD_NET_SALES,
                COALESCE(S.NET_SALES, 0) - COALESCE(G.NET_SALES, 0) AS DIFFERENCE
            FROM SILVER_TOTAL S
            FULL OUTER JOIN GOLD_TOTAL G
                ON S.BUSINESS_DATE = G.BUSINESS_DATE
            ORDER BY BUSINESS_DATE
        """

        results = hook.get_records(query)

        results = hook.get_records(query)

        if not results:
            raise ValueError(
                "Reconciliation returned no results."
            )

        all_matched = True

        print("===== SALES RECONCILIATION =====")

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
                "Sales reconciliation failed. "
                "Silver and Gold totals do not match."
            )

        print("Sales reconciliation passed.")

    reconciliation = reconcile_sales()

    # 8. DEFINE TASK DEPENDENCIES

    bronze_setup >> silver_setup
    silver_setup >> wait_for_files

    wait_for_files >> bronze_loads
    bronze_loads >> silver_transformation

    silver_transformation >> quarantine_report
    quarantine_report >> gold_setup
    gold_setup >> gold_transformation
    gold_transformation >> reconciliation


assignment_2_3_pipeline()


from airflow.models import DagBag


def test_bronze_ingestion_deletes_before_forced_reload(
    monkeypatch, tmp_path
):
    dag_bag = DagBag(dag_folder="dags/assignment-2.3")
    dag = dag_bag.get_dag("assignment_2_3_pipeline")

    assert dag is not None

    ingest_task = dag.get_task("ingest_file")
    ingest_file = ingest_task.python_callable

    # Create a temporary source file.
    data_directory = tmp_path / "data"
    source_folder = data_directory / "pos_sales"
    source_folder.mkdir(parents=True)

    file_name = "pos_sales_20261001.csv"
    (source_folder / file_name).write_text("test,data\n", encoding="utf-8")

    config = {
        "data_directory": str(data_directory),
        "snowflake_connection_id": "snowflake_conn",
        "database": "RETAIL_DW",
        "bronze_schema": "BRONZE_3",
        "stage": "RETAIL_DW.BRONZE_3.RETAIL_STAGE",
        "file_format": "RETAIL_DW.BRONZE_3.CSV_FORMAT",
    }

    executed_sql = []

    class FakeCursor:
        def execute(self, sql):
            executed_sql.append(sql)

        def close(self):
            pass

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            self.committed = True

        def rollback(self):
            self.rolled_back = True

        def close(self):
            pass

    fake_connection = FakeConnection()
    fake_connection.committed = False
    fake_connection.rolled_back = False

    class FakeSnowflakeHook:
        def __init__(self, snowflake_conn_id):
            assert snowflake_conn_id == "snowflake_conn"

        def get_conn(self):
            return fake_connection

    # Replace configuration and Snowflake connection with mocks.
    monkeypatch.setitem(
        ingest_file.__globals__, "read_config", lambda: config
    )
    monkeypatch.setitem(
        ingest_file.__globals__, "SnowflakeHook", FakeSnowflakeHook
    )

    file_spec = {
        "folder": "pos_sales",
        "file": file_name,
        "stage_folder": "POS_SALES",
        "table": "POS_SALES_RAW",
        "columns": [
            "LOCATION_ID",
            "WORKSTATION_ID",
            "TRANSACTION_SEQUENCE",
            "LINE_SEQUENCE",
            "BUSINESS_DATE",
            "BEGIN_DATETIME",
            "TRANSACTION_TYPE",
            "ITEM_ID",
            "ITEM_SCAN_ID",
            "QUANTITY",
            "UNIT_PRICE",
            "EXTENDED_AMOUNT",
            "DISCOUNT_AMOUNT",
            "TAX_AMOUNT",
            "NET_AMOUNT",
            "CURRENCY",
            "CASHIER_ID",
        ],
    }

    # Execute the ingestion callable using mocked Snowflake objects.
    ingest_file(file_spec)

    # Identify the DELETE and COPY statements.
    delete_index = next(
        i for i, sql in enumerate(executed_sql)
        if "DELETE FROM" in sql
    )

    copy_index = next(
        i for i, sql in enumerate(executed_sql)
        if "COPY INTO" in sql
    )

    delete_sql = executed_sql[delete_index]
    copy_sql = executed_sql[copy_index]

    # Verify exact-file deletion happens before reload.
    assert delete_index < copy_index
    assert file_name in delete_sql

    # Verify the reload is forced for the selected file.
    assert "FORCE = TRUE" in copy_sql.upper()
    assert file_name in copy_sql

    # Verify successful transaction completion.
    assert fake_connection.committed is True
    assert not fake_connection.rolled_back
from datetime import datetime
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag(
    dag_id="test_postgres",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
)
def test_postgres_connection():

    @task
    def check_connection():
        hook = PostgresHook(postgres_conn_id="retail_postgres")
        result = hook.get_first("SELECT current_database(), current_user;")
        print(f"Connection successful: {result}")

    check_connection()

test_postgres_connection()
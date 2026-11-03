from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="test_dag",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
)
def test_workflow():

    @task
    def hello():
        print("Airflow is working successfully!")

    hello()


test_workflow()

from airflow.models import DagBag


def test_all_tasks_have_retries():
    dag_bag = DagBag(
        dag_folder="dags/assignment-2.3",
    )

    dag = dag_bag.get_dag("assignment_2_3_pipeline")

    assert dag is not None

    for task in dag.tasks:
        assert task.retries >= 1, f"{task.task_id} has no retries configured"

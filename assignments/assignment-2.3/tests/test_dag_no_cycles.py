
from airflow.models import DagBag


def test_dag_has_no_cycles():
    dag_bag = DagBag(
        dag_folder="dags/assignment-2.3",
    )

    dag = dag_bag.get_dag("assignment_2_3_pipeline")

    assert dag is not None
    assert dag.topological_sort()

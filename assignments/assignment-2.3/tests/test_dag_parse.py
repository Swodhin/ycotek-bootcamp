
from airflow.models import DagBag


def test_dag_loads_without_import_errors():
    dag_bag = DagBag(
        dag_folder="dags/assignment-2.3",
    )

    assert dag_bag.import_errors == {}
    assert "assignment_2_3_pipeline" in dag_bag.dags

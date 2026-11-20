
from airflow.models import DagBag


def test_pipeline_task_dependencies():
    dag_bag = DagBag(
        dag_folder="dags/assignment-2.3",
    )

    dag = dag_bag.get_dag("assignment_2_3_pipeline")

    assert dag is not None

    expected_dependencies = {
        "bronze_setup": ["silver_setup"],
        "silver_setup": ["wait_for_source_files"],
        "wait_for_source_files": ["ingest_file"],
        "ingest_file": ["silver_transformation"],
        "silver_transformation": ["quarantine_summary"],
        "quarantine_summary": ["gold_setup"],
        "gold_setup": ["gold_transformation"],
        "gold_transformation": ["reconcile_sales"],
    }

    for upstream_task, expected_downstream in expected_dependencies.items():
        task = dag.get_task(upstream_task)

        actual_downstream = {
            downstream.task_id
            for downstream in task.downstream_list
        }

        assert set(expected_downstream).issubset(actual_downstream), (
            f"{upstream_task} has incorrect downstream dependencies"
        )

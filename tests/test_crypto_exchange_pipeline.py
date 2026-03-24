from airflow.models import DagBag


def test_dag_loaded():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    assert "crypto_exchange_pipeline" in dag_bag.dags
    assert not dag_bag.import_errors


def test_dag_task_count():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    dag = dag_bag.get_dag("crypto_exchange_pipeline")
    assert dag is not None
    assert len(dag.tasks) == 8

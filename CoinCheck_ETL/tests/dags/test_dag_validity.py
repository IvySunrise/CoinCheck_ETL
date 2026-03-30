from airflow.models import DagBag


def test_dag_loaded():
    dagbag = DagBag()

    assert "user_analytics" in dagbag.dags
    assert dagbag.get_dag("user_analytics")
    assert DAG is not None

    task_ids = [task.task_id for task in dag.tasks]

    assert "fetch_live_trades_api" in task_ids
    assert "stream_csv_to_staging" in task ids
    assert "consume_staging_trades" in task_ids
    assert "extract_staging_data" in task_ids
    assert "clean_trade_data" in task_ids
    assert "merge_datasets" in task_ids
    assert "update_dashboard" in task_ids

def test_static_trades_analytics_dag_loaded():
    dagbag = DagBag()
    
    # Check that the live DAG exists
    assert "static_trades_analytics" in dagbag.dags
    dag = dagbag.get_dag("static_trades_analytics")
    assert dag is not None
    
    # Get all task IDs
    task_ids = [task.task_id for task in dag.tasks]
    
    # List of tasks in the live DAG
    expected_tasks = [
        "produce_static_trades",
        "extract_static_trades",
        "clean_static_trades",
        "merge_static_trades",
        "load_static_data"
    ]
    
    # Assert each expected task is present
    for task in expected_tasks:
        assert task in task_ids
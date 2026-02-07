import json
from unittest.mock import MagicMock

from src.consumer_dag import branch_check, dag


def test_branch_check_no_events():
    """Test branch_check when no events are received."""
    assert branch_check.function([]) == "end_skipped"
    assert branch_check.function(None) == "end_skipped"


def test_branch_check_standard_events():
    """Test branch_check with standard events (no high-value purchase)."""
    events = [
        {"event_type": "LOGIN", "user_id": 1234},
        {"event_type": "PURCHASE", "amount": 50.0},
        {"event_type": "VIEW", "user_id": 5678},
    ]
    assert branch_check.function(events) == "process_standard"


def test_branch_check_high_value_purchase():
    """Test branch_check with a high-value purchase."""
    events = [
        {"event_type": "LOGIN", "user_id": 1234},
        {"event_type": "PURCHASE", "amount": 150.0},
        {"event_type": "VIEW", "user_id": 5678},
    ]
    assert branch_check.function(events) == "process_high_value"


def test_consume_task_func_logic(mocker):
    """Test the logic inside consume_task_func using mocks."""
    # Since consume_task_func is defined inside the DAG context and uses local imports,
    # we need to be careful with how we test it.
    # However, we can test the function if we extract it or mock the dependencies
    # inside it.

    # Let's find the task object
    consume_task = dag.get_task("consume_from_topic")

    # Define a mock message
    mock_msg = MagicMock()
    mock_msg.value.return_value = json.dumps(
        {"event_type": "LOGIN", "user_id": 1}
    ).encode("utf-8")

    # Mock KafkaConsumerHook
    hook_path = "airflow.providers.apache.kafka.hooks.consume.KafkaConsumerHook"
    mock_hook_class = mocker.patch(hook_path)
    mock_hook_instance = mock_hook_class.return_value
    mock_consumer = mock_hook_instance.get_consumer.return_value
    mock_consumer.consume.return_value = [mock_msg]

    # Execute the function (it's a decorated task, so .python_callable
    # gives the original func)
    result = consume_task.python_callable()

    assert len(result) == 1
    assert result[0]["event_type"] == "LOGIN"
    mock_consumer.close.assert_called_once()


def test_dag_structure():
    """Test the structure of the consumer DAG."""
    assert dag.dag_id == "kafka_consumer_dag"
    expected_tasks = {
        "consume_from_topic",
        "branch_logic",
        "process_high_value",
        "process_standard",
        "end_skipped",
        "end_task",
    }
    assert set(dag.task_ids) == expected_tasks


def test_dag_dependencies():
    """Test task dependencies in the consumer DAG."""
    branch_task = dag.get_task("branch_logic")
    assert "consume_from_topic" in branch_task.upstream_task_ids

    high_value_task = dag.get_task("process_high_value")
    assert "branch_logic" in high_value_task.upstream_task_ids

    standard_task = dag.get_task("process_standard")
    assert "branch_logic" in standard_task.upstream_task_ids

    end_task = dag.get_task("end_task")
    assert "process_high_value" in end_task.upstream_task_ids
    assert "process_standard" in end_task.upstream_task_ids
    assert "end_skipped" in end_task.upstream_task_ids

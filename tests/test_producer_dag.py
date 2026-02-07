import json

from src.producer_dag import dag, producer_function


def test_producer_function_defaults():
    """Test producer_function with default arguments."""
    events = producer_function()
    assert len(events) == 10
    for key, value in events:
        assert isinstance(key, str)
        event_dict = json.loads(value)
        assert "event_id" in event_dict
        assert event_dict["event_id"] == key
        assert event_dict["event_type"] in ["LOGIN", "PURCHASE", "VIEW", "LOGOUT"]
        assert 1000 <= event_dict["user_id"] <= 9999
        assert 10.0 <= event_dict["amount"] <= 500.0
        assert "timestamp" in event_dict


def test_producer_function_custom_count():
    """Test producer_function with custom event count."""
    count = 5
    events = producer_function(num_events=count)
    assert len(events) == count


def test_dag_structure():
    """Test the structure of the producer DAG."""
    assert dag.dag_id == "kafka_producer_dag"
    assert "produce_to_topic" in dag.task_ids

    task = dag.get_task("produce_to_topic")
    assert task.topic == "lab_events"
    assert task.producer_function == producer_function
    assert task.producer_function_kwargs == {"num_events": 20}


def test_dag_tags():
    """Test DAG tags."""
    assert set(dag.tags) == {"lab", "kafka"}

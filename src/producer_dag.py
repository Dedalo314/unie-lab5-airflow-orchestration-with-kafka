import json
import random
import uuid
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 2, 1),
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

TOPIC = "lab_events"


def producer_function(num_events: int = 10) -> Any:
    """
    Generates a list of events to send to Kafka.
    """
    events = []
    for _ in range(num_events):
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": random.choice(["LOGIN", "PURCHASE", "VIEW", "LOGOUT"]),  # nosec
            "user_id": random.randint(1000, 9999),  # nosec
            "amount": round(random.uniform(10.0, 500.0), 2),  # nosec
            "timestamp": datetime.now().isoformat(),
        }
        # key, value
        events.append((event["event_id"], json.dumps(event)))
    return events


with DAG(
    "kafka_producer_dag",
    default_args=default_args,
    description="A simple DAG to produce events to Kafka",
    schedule_interval=timedelta(minutes=10),
    catchup=False,
    tags=["lab", "kafka"],
) as dag:
    producer_task = ProduceToTopicOperator(
        task_id="produce_to_topic",
        topic=TOPIC,
        producer_function=producer_function,
        producer_function_args=[],
        producer_function_kwargs={"num_events": 20},
        kafka_config_id="connection_producer_1",
        synchronous=True,
    )

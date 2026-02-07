import json
import logging
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

TOPIC = "lab_events"


with DAG(
    "kafka_consumer_dag",
    default_args=default_args,
    description="A DAG to consume and process events from Kafka",
    schedule_interval=timedelta(minutes=10),
    catchup=False,
    tags=["lab", "kafka"],
) as dag:
    # 1. Consume from Kafka
    @task(task_id="consume_from_topic")
    def consume_task_func() -> list[dict[str, Any]]:
        from airflow.providers.apache.kafka.hooks.consume import KafkaConsumerHook

        consumer_hook = KafkaConsumerHook(
            topics=[TOPIC], kafka_config_id="connection_consumer_1"
        )
        consumer = consumer_hook.get_consumer()

        processed_messages = []
        try:
            # Poll for messages
            msgs = consumer.consume(num_messages=20, timeout=60)

            for message in msgs:
                try:
                    logging.info(f"Received message: {message}")
                    if message and message.value():
                        val = json.loads(message.value().decode("utf-8"))
                        logging.info(f"Decoded message: {val}")
                        processed_messages.append(val)
                except Exception as e:
                    logging.error(f"Error decoding message: {e}")
        finally:
            consumer.close()

        return processed_messages

    # 2. Branching Logic
    @task.branch(task_id="branch_logic")
    def branch_check(events: Any) -> str:
        logging.info(f"Received events type: {type(events)}")
        logging.info(f"{events=}")

        if not events:
            logging.info("No events received. Skipping processing.")
            return "end_skipped"

        has_purchase = False
        for event in events:
            if event and event.get("event_type") == "PURCHASE":
                if event.get("amount", 0) > 100:
                    logging.info(f"High value purchase found: {event.get('amount')}")
                    has_purchase = True
                    break

        if has_purchase:
            return "process_high_value"
        else:
            return "process_standard"

    # 3. Processing Tasks
    @task(task_id="process_high_value")
    def process_high_value(events: Any) -> int:
        logging.info("PROCESSING HIGH VALUE TRANSACTIONS found in batch.")
        # Logic to store to DB or alert could go here
        return len(events)

    @task(task_id="process_standard")
    def process_standard(events: Any) -> int:
        logging.info("Processing standard events batch.")
        return len(events)

    events = consume_task_func()

    end_skipped = EmptyOperator(task_id="end_skipped")
    end_task = EmptyOperator(task_id="end_task", trigger_rule="none_failed")

    (
        branch_check(events)
        >> [process_high_value(events), process_standard(events), end_skipped]
        >> end_task
    )

# unie-lab5-airflow-orchestration-with-kafka

**Academic, reproducible pipeline** for orchestrating data streams using Apache Airflow and Apache Kafka.

**Purpose**: Demonstrates the integration of Airflow with Kafka for event-driven data processing. Includes a **producer** for generating mock user activity and a **consumer** with branching logic to categorize events (e.g., identifies high-value purchases). Emphasizes infrastructure-as-code using [Compose](https://compose-spec.io/) for local development and testing.

## Project Structure
```bash
.
├── .github/              # GitHub Actions CI and Dependabot config
├── src/                  # Airflow DAGs and logic
│   ├── producer_dag.py   # DAG: Generates mock events to Kafka
│   └── consumer_dag.py   # DAG: Consumes and branches based on event data
├── tests/                # Unit tests for DAGs and logic
├── compose.yaml          # Multi-container setup (Airflow, Kafka, Postgres)
├── Containerfile         # Custom Airflow image with Kafka providers
├── pyproject.toml        # Python project dependencies (uv)
├── .pre-commit-config.yaml # Pre-commit hooks for code quality
└── README.md
```

## Copyright and License

- **© 2026 Álvaro Martín-Cortinas**
- **Code**: Apache License 2.0

Developed and tested on:

- **Python ≥ 3.12**
- **Podman / Podman Compose**
- **Dependencies:** see pyproject.toml file

## Quick start

```bash
# 1. Start the infrastructure
podman compose up -d --build

# 2. Access the services
# Airflow Webserver: http://localhost:8080 (admin/admin)
# Kafka-UI: http://localhost:8081

# 3. Running the pipeline
# - Navigate to the Airflow UI.
# - Unpause 'kafka_producer_dag' to generate event data.
# - Unpause 'kafka_consumer_dag' to process the created events.

# 4. Stop the services
podman compose down
```

**Disclaimer**: Provided *as is*, for academic use only.

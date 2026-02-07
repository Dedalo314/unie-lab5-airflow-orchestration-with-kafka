FROM apache/airflow:2.10.4

USER root
# Install git and other system deps if needed (e.g. for some providers)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    git \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy pyproject.toml to workdir
COPY pyproject.toml .

# Install dependencies using uv
# We use `pip install` with uv as the installer for Airflow compatibility or direct uv pip install.
# Since standard airflow image has pip, we can use uv to generate requirements or just install directly.
# Let's install uv first.
RUN pip install uv

# Install dependencies
RUN uv pip install --requirement pyproject.toml

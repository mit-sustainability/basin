FROM python:3.13-slim-bookworm

RUN apt-get update && \
    apt-get install -y libpq-dev gcc ca-certificates build-essential curl jq git unzip wget

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
ENV UV_SYSTEM_PYTHON=1

# Setup Oracle client
ENV ORACLE_HOME=/opt/oracle
RUN mkdir -p $ORACLE_HOME
WORKDIR $ORACLE_HOME
RUN apt-get install -y libaio1 && \
    wget https://download.oracle.com/otn_software/linux/instantclient/1922000/instantclient-basiclite-linux.x64-19.22.0.0.0dbru.zip && \
    unzip instantclient-basiclite-linux.x64-19.22.0.0.0dbru.zip && \
    rm -f instantclient-basiclite-linux.x64-19.22.0.0.0dbru.zip && \
    echo /opt/oracle/instantclient_19_22 > /etc/ld.so.conf.d/oracle-instantclient.conf && \
    ldconfig && \
    apt-get purge -y --auto-remove wget unzip

# Setup Dagster home
ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p $DAGSTER_HOME
WORKDIR /opt/dagster/app

COPY orchestrator/dagster.yaml $DAGSTER_HOME

# Copy dependency manifests first for better layer caching
COPY pyproject.toml uv.lock ./
COPY orchestrator/pyproject.toml orchestrator/pyproject.toml

# Copy source needed for editable install and dbt
COPY orchestrator orchestrator
COPY warehouse warehouse

# Install all Python dependencies from lockfile
RUN uv sync --frozen --no-dev
ENV PATH="/opt/dagster/app/.venv/bin:$PATH"

# Set dbt build env vars
ARG pg_host
ARG pg_user
ARG pg_pass
ENV PG_WAREHOUSE_HOST=${pg_host}
ENV PG_USER=${pg_user}
ENV PG_PASSWORD=${pg_pass}

# Install dbt packages and compile manifest
RUN uv run dbt deps --project-dir warehouse --profiles-dir warehouse && \
    uv run dbt \
        --no-use-colors \
        --log-format json \
        ls \
        --project-dir warehouse \
        --profiles-dir warehouse \
        --select '*' \
        --output json

ENV CMDSTAN=/opt/cmdstan
RUN uv run python -c "from cmdstanpy import install_cmdstan; install_cmdstan(dir='/opt')" \
    && ln -s "$(find /opt -maxdepth 1 -type d -name 'cmdstan-*' | head -n 1)" /opt/cmdstan
RUN uv run playwright install --with-deps chromium

EXPOSE 3000
WORKDIR /opt/dagster/app/orchestrator
ENV PYTHONPATH="/opt/dagster/app/"

CMD ["dagster-webserver", "-w", "workspace.yaml", "-h", "0.0.0.0", "-p", "3000"]

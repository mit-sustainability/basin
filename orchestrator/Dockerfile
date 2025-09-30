# FROM --platform=linux/arm64 python:3.11-slim
FROM --platform=linux/amd64 python:3.11-slim


RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y libpq-dev gcc ca-certificates build-essential curl jq git unzip wget


# Setup Oracle client
ENV ORACLE_HOME=/opt/oracle
RUN mkdir -p $ORACLE_HOME
WORKDIR $ORACLE_HOME
RUN apt-get update -y && \
    apt-get upgrade -y && \
    apt-get install -y libaio wget  && \
    wget https://download.oracle.com/otn_software/linux/instantclient/1922000/instantclient-basiclite-linux.x64-19.22.0.0.0dbru.zip  && \
    unzip instantclient-basiclite-linux.x64-19.22.0.0.0dbru.zip && \
    rm -f instantclient-basiclite-linux.x64-19.22.0.0.0dbru.zip && \
    echo /opt/oracle/instantclient_19_22 > /etc/ld.so.conf.d/oracle-instantclient.conf && \
    ldconfig && \
    apt-get purge -y --auto-remove wget unzip


# Setup Dagster Environment
ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p $DAGSTER_HOME
WORKDIR /opt/dagster/app

COPY orchestrator/dagster.yaml $DAGSTER_HOME
RUN pip install --upgrade pip


# Copy DBT project in
COPY warehouse warehouse
RUN cd warehouse && bash setup.sh

# Set dbt build env vars
ARG pg_host
ARG pg_user
ARG pg_pass
ENV PG_WAREHOUSE_HOST=${pg_host}
ENV PG_USER=${pg_user}
ENV PG_PASSWORD=${pg_pass}

# Compile DBT manifest file
RUN dbt \
    --no-use-colors \
    --log-format json \
    ls \
    --project-dir warehouse \
    --profiles-dir warehouse \
    --select '*' \
    --output json

# Install orchestrator dependencies
COPY orchestrator/requirements.txt requirements.txt
RUN pip install -r requirements.txt

# # Copy libs in and install w3b_dp package
# # COPY libs libs
# # RUN pip install ./libs

# Copy Dagster pipeline code in
COPY orchestrator orchestrator
EXPOSE 3000
WORKDIR /opt/dagster/app/orchestrator
RUN pip install -e .
ENV PYTHONPATH="/opt/dagster/app/"

CMD ["dagster-webserver", "-w", "workspace.yaml", "-h", "0.0.0.0", "-p", "3000"]

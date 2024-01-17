# FROM --platform=linux/arm64 python:3.11-slim
FROM --platform=linux/amd64 python:3.11-slim


RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y libpq-dev gcc ca-certificates build-essential curl jq git unzip


# # Install AWS CLI
# RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
#     unzip awscliv2.zip && \
#     ./aws/install

ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p $DAGSTER_HOME

COPY orchestrator/dagster.yaml $DAGSTER_HOME

RUN pip install --upgrade pip

# Install orchestrator dependencies
WORKDIR /opt/dagster/app
COPY orchestrator/requirements.txt requirements.txt
RUN pip install -r requirements.txt


# Copy DBT project in
COPY warehouse warehouse
RUN cd warehouse && bash setup.sh

# Set dbt build env vars
ARG pg_host
ARG pg_user
ENV PG_WAREHOUSE_HOST=${pg_host}
ENV PG_USER=${pg_user}


# Compile DBT manifest file
RUN --mount=type=secret,id=pgpass \
     PG_PASSWORD=$(cat /run/secrets/pgpass |grep 'PG_PASSWORD' | awk -F '=' '{print $2}') && \
    dbt \
    --no-use-colors \
    --log-format json  \
    ls \
    --project-dir warehouse \
    --profiles-dir warehouse \
    --select * \
    --output json

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

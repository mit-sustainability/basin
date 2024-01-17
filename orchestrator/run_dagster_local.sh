#!/bin/bash
mkdir -p "$(pwd)/.dagster"
export DAGSTER_HOME="$(pwd)/.dagster"
export DAGSTER_LOCAL_MODE="true"
DAGSTER_DBT_PARSE_PROJECT_ON_LOAD=1 dagster dev

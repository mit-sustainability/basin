#!/bin/bash

# Install python packages for DBT & SQL linter
pip install -r requirements.txt
# Install DBT packages
dbt deps

# Copy DBT profiles.yml to expected location
mkdir -p ~/.dbt && cp profiles.yml ~/.dbt/profiles.yml

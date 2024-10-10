# BASIN: The MITOS data-platform

A river basin consists of many streams...

## Project Structure

- `Makefile` -> Repo setup & management automation
- `.github` -> Github Actions workflows
- `warehouse` -> DBT project which builds our BigQuery data warehouse
- `orchestrator` -> Dagster Assets/Schedules/Dockerfile

## Environment Setup

1. Install Python 3.11.7+ and create a virtual environment:
   1. Install pyenv and pyenv-virtualenv to manage python versions and virtual environments `brew install pyenv pyenv-virtualenv` (See [pyenv](https://github.com/pyenv/pyenv) for details)
   2. Once you have pyenv installed, you can install the correct Python version with `pyenv install 3.11.7` (if pyenv does not know about 3.11.7, you need to upgrade it `brew upgrade pyenv` or `pyenv update`)
   3. Create the python virtual environment `pyenv virtualenv 3.11.7 mitos-dev`. This will create a virtual environment in the location: `~/.pyenv/versions/3.11.7/envs/mitos-dev`.
   4. Set your IDE's python interpreter path to `~/.pyenv/versions/3.11.7/envs/mitos-dev/bin/python`
   5. Alternatively, you may use other environment tools like `conda` and `poetry`
2. Inside the python virtual environment, run `pip install —-upgrade pip`
3. Inside the python virtual environment, run `make setup-dev` to install all project dependencies

## Setup Local Dagster and Postgres Instances

1. To see Dagster/dbt in action, one must setup a local Postgres instance. Follow the instructions to install postgresql and setup a user/password [here.](https://www.sqlshack.com/setting-up-a-postgresql-database-on-mac/)
2. Setup environment variables required for dagster, docker, Makefile and dbt. It is recommended to use `direnv` in the root folder of this repo, and set up a .envrc file. See `envrc_template` for example. With `direnv`, a `.envrc` file containing environment variables will be automatially loaded when one enters the `basin` directory.
3. Under the `orchestrator` directory execute `./run_dagster_local.sh`

## Contributing

1. Install [pre-commit](https://pre-commit.com/) into your Python environment with `pip install pre-commit` and run `pre-commit install` to install the pre-commit hooks. This will run `black`, `flake8`, `pytest`, and other project tools on your code before you commit it.

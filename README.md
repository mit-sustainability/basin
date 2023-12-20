# BASIN: The MITOS data-platform
A river basin consists of many streams...

## Project Structure
- `Makefile` -> Repo setup & management automation
- `.github` -> Github Actions workflows
- `warehouse` -> DBT project which builds our BigQuery data warehouse
- `orchestrator` -> Dagster Assets/Schedules/Dockerfile

## Environment Setup
1. Install Python 3.11.5 and create virtual environment:
   1. Install pyenv and pyenv-virtualenv to manage python versions and virtual environments `brew install pyenv pyenv-virtualenv` (See [pyenv](https://github.com/pyenv/pyenv) for details)
   2. Once you have pyenv installed, you can install the correct Python version with `pyenv install 3.11.5` (if pyenv does not know about 3.11.5, you need to upgrade it `brew upgrade pyenv` or `pyenv update`)
   3. Create the python virtual environment `pyenv virtualenv 3.11.5 mitos-dev`. This will create a virtual environment in the location: `~/.pyenv/versions/3.11.5/envs/mitos-dev`.
   4. Set your IDE's python interpreter path to `~/.pyenv/versions/3.11.5/envs/mitos-dev/bin/python`
   5. Alternatively, you may use other environment tools like `conda` and `poetry`
2. Inside the python virtual environment, run `pip install —-upgrade pip`
3. Inside the python virtual environment, run `make setup-dev` to install all project dependencies

## Contributing
1. Install [pre-commit](https://pre-commit.com/) into your Python environment with `pip install pre-commit` and run `pre-commit install` to install the pre-commit hooks. This will run `black`, `flake8`, `pytest`, and other project tools on your code before you commit it.

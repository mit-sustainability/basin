# BASIN: The MITOS data-platform
A river basin consists of many streams...

## Project Structure
- `Makefile` -> Repo setup & management automation
- `.github` -> Github Actions workflows
- `warehouse` -> DBT project which builds our PostgreSQL warehouse layers
- `orchestrator` -> Dagster Assets/Schedules/Dockerfile
- `orchestrator/pipes` -> Thin ECS Dagster Pipes entrypoints for assets that support remote execution; see [orchestrator/pipes/README.md](/Users/yucheng/Documents/Projects/basin/orchestrator/pipes/README.md)

## Environment Setup
1. On Apple Silicon, install Rosetta because the Oracle-backed local runtime needs an Intel (`x86_64`) Python process:
   1. `softwareupdate --install-rosetta --agree-to-license`
2. Install pyenv and pyenv-virtualenv:
   1. `brew install pyenv pyenv-virtualenv`
   2. `pyenv install 3.11.5`
   3. `pyenv virtualenv 3.11.5 oracle_client`
3. Open the repository root in VS Code.
   1. [`.vscode/settings.json`](/Users/yucheng/Documents/Projects/basin/.vscode/settings.json) starts the integrated terminal under Rosetta and points the Python extension at `~/.pyenv/versions/oracle_client/bin/python`.
   2. [`.python-version`](/Users/yucheng/Documents/Projects/basin/.python-version) keeps `pyenv` on `oracle_client` in shells that load `pyenv init`.
4. Inside that environment, run `python -m pip install --upgrade pip`.
5. Run `make setup-dev` from the repository root to install dbt, Dagster, the editable orchestrator package, and Playwright Chromium.

## Contributing
1. Install [pre-commit](https://pre-commit.com/) into your Python environment with `pip install pre-commit` and run `pre-commit install` to install the pre-commit hooks. This will run `black`, `flake8`, `pytest`, and other project tools on your code before you commit it.

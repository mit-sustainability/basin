# BASIN: The MITOS data-platform
A river basin consists of many streams...

## Project Structure
- `Makefile` -> Repo setup & management automation
- `.github` -> Github Actions workflows
- `warehouse` -> DBT project which builds our PostgreSQL warehouse layers
- `orchestrator` -> Dagster Assets/Schedules/Dockerfile
- `orchestrator/pipes` -> Thin ECS Dagster Pipes entrypoints for assets that support remote execution; see [orchestrator/pipes/README.md](./orchestrator/pipes/README.md)

## Repository Highlights
- `orchestrator/assets/website_content_health.py` crawls `https://sustainability.mit.edu` into raw page snapshot and hyperlink health tables for scheduled monitoring.
- `orchestrator/assets/confluence_wiki.py` snapshots the MITOS Confluence space into `raw.confluence_mitos_pages`, preserving storage XHTML plus normalized markdown for downstream RAG ingestion.

## Environment Setup

### Prerequisites
Install the following tools via Homebrew if not already present:
```bash
brew install mise uv direnv
```

### First-time setup
1. Clone the repo and `cd` into it.
2. Install Python (managed by mise):
   ```bash
   mise trust && mise install
   ```
   This installs Python 3.13 as declared in `.mise.toml`.
3. Install all Python dependencies into `.venv`:
   ```bash
   make setup-python
   ```
   This runs `uv sync --all-groups`, creating `.venv` and installing all packages (Dagster, dbt, dev tools) from `pyproject.toml` and the locked `uv.lock`.
4. Allow direnv to activate the environment automatically on `cd`:
   ```bash
   direnv allow
   ```
5. Install dbt packages and copy the profiles file:
   ```bash
   make setup-dbt
   ```
6. Install Playwright browser:
   ```bash
   make setup-playwright
   ```

Or run all of the above in one shot:
```bash
make setup-dev
```

### Apple Silicon (ARM64) — Oracle Instant Client

MIT Data Warehouse (`DWRHS`) requires Oracle Native Network Encryption, which only works in `oracledb` thick mode and therefore needs the Oracle Instant Client libraries. The Homebrew `instantclient-basiclite` formula is x86_64 only; on Apple Silicon you must install the ARM64 build manually.

Download the **Basic Light Package** for macOS ARM64 from Oracle's website, copy all libraries into `/opt/oracle/instantclient_23_26/`, and ensure `DYLD_LIBRARY_PATH` includes that path (already set in `.envrc`).

### Dependency management
All Python dependencies live in the root `pyproject.toml`. The `orchestrator` package is installed as an editable local package via `[tool.uv.sources]`. To add a dependency:
```bash
uv add <package>          # adds to pyproject.toml and updates uv.lock
uv add --group dev <pkg>  # adds to the dev dependency group
```
After pulling changes that modify `pyproject.toml` or `uv.lock`, run `uv sync` to update your environment.

## Contributing
1. Install [pre-commit](https://pre-commit.com/) into your Python environment with `pip install pre-commit` and run `pre-commit install` to install the pre-commit hooks. This will run `black`, `flake8`, `pytest`, and other project tools on your code before you commit it.

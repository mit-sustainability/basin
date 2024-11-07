# BASIN: The MITOS data-platform

A river basin consists of many streams...

## Project Structure

- `Makefile` -> Repo setup & management automation
- `.github` -> Github Actions workflows
- `warehouse` -> DBT project which builds our BigQuery data warehouse
- `orchestrator` -> Dagster Assets/Schedules/Dockerfile

## Setup

### Python Environment
You can follow the following steps to set up the proper python environment:
1. Install Python 3.11.7+ and create a virtual environment:
   1. Install pyenv and pyenv-virtualenv to manage python versions and virtual environments `brew install pyenv pyenv-virtualenv` (See [pyenv](https://github.com/pyenv/pyenv) for details)
   2. Once you have pyenv installed, you can install the correct Python version with `pyenv install 3.11.7` (if pyenv does not know about 3.11.7, you need to upgrade it `brew upgrade pyenv` or `pyenv update`)
   3. Create the python virtual environment `pyenv virtualenv 3.11.7 mitos-dev`. This will create a virtual environment in the location: `~/.pyenv/versions/3.11.7/envs/mitos-dev`.
   4. Set your IDE's python interpreter path to `~/.pyenv/versions/3.11.7/envs/mitos-dev/bin/python`
   5. Alternatively, you may use other environment tools like `conda` and `poetry`
   6. Your terminal may have the form `(mitos-dev) ...@... basin %` if this environment is set properly
2. Inside the python virtual environment, run `pip install —-upgrade pip`
3. Inside the python virtual environment, run `make setup-dev` to install all project dependencies

### Setup Local Postgres Instances
To see Dagster/dbt in action, one must first setup a local Postgres instance. You can follow the instructions to install postgresql and set up a user/password [here.](https://www.sqlshack.com/setting-up-a-postgresql-database-on-mac/)

### Environment Variables
You should also set up environment variables required for dagster, docker, Makefile and dbt. 
It is recommended to use [`direnv`](https://formulae.brew.sh/formula/direnv#default) in the root folder of this repo. With `direnv`, a `.envrc` file containing environment variables will be automatially loaded when one enters the `basin` directory. For mac homebrew users, you can run `brew install direnv` to install direnv
Then, create or update an .envrc file to set environment variables. You can see `envrc_template` for an example of the variables to include in it
To export these variables into your environment, you can run either:
1. `eval "$(direnv hook zsh)"`
2. `direnv allow`
3. `eval "$(direnv hook bash)" `
You may need to run this command each time your terminal restarts
To check if a particular variable has loaded, you can run `echo $VARIABLE_NAME` in your terminal and check if the output is what is set in your .envrc 


## Dagster

### Running
After ensuring that your environment variables have loaded and you've run make setup, you can run run_dagster_local.sh in the orchestrator folder using the command `./run_dagster_local.sh`
If this runs properly, you should be able to view the dagster UI at http://127.0.0.1:3000/
You can use the UI to start jobs that materialize tables and make them available locally for DBT to use
Materializing tables may take some time, but their progress is reported in both the UI and CLI
More documentation on the dagster UI can be found at https://docs.dagster.io/concepts/webserver/ui

### Testing
You can run tests via `make run-tests` in the basin folder or `./execute_unit_tests.sh` in the orchestrator folder

## DBT

### Running
To run a particular model on DBT, you must first ensure the raw tables have been materialized via dagster
Afterwards, once `make setup-dev` and `direnv allow` have been run, you can run models on DBT via `dbt run -s table_name` in the warehouse folder

### Testing
You can view what tests each table enforces by running `make serve-dbt-catalog`, navigating to http://localhost:8080, and checking the Data Tests column for each table
More documentation on the DBT UI can be found at https://docs.getdbt.com/docs/cloud/dbt-cloud-ide/ide-user-interface 

## Viewing output

### Local Dagster
You can view the output of local dagster materialized tables at #TODO

### Local DBT
You can view the output of DBT runs by creating a local postgres database connection on an application like dBeaver
<!-- First, ensure that you have Postgres installed. You can download it from https://www.postgresql.org/download/
Then, you can set it up by following the directions listed here https://www.sqlshack.com/setting-up-a-postgresql-database-on-mac/ 
1. You can access the PostgreSQL server by running `psql -U your_admin_username -d your_database_name -h your_host`, where your_database_name may be postgres and your_host may be localhost
2. To make a new user on this server, you can run `CREATE USER your_new_username WITH PASSWORD 'your_password';` on the PostgreSQL server, and grant them privileges by running `GRANT ALL PRIVILEGES ON DATABASE your_database_name TO your_new_username;` -->
You can create a connection on dBeaver to view the DBT table output by configuring the host as localhost, the database as postgres, the port as 5432, and the username and password as what you set your new user to be for the Database Native authentication

### Database Stream
You can also view the current state of the schemas as they exist on github
You may ask a system administrator for login information to make a database connection on an application like dBeaver
On dBeaver, you can navigate to the "Create a Connection" configuration window, fill in the host as #TODO, the database as postgres, the port as 5432, and use the admin-provided username and password for the Database Native authentication dropdown option

## Potential Issues / Debugging Tips #TODO
Ensure that `direnv allow` has been run and properly populates the variables
Make sure that `make setup-dev` has been run to install the required dependencies

## Contributing

1. Install [pre-commit](https://pre-commit.com/) into your Python environment with `pip install pre-commit` and run `pre-commit install` to install the pre-commit hooks. This will run `black`, `flake8`, `pytest`, and other project tools on your code before you commit it.

# <p align="center"><img src="https://www.usaspending.gov/img/logo@2x.png" alt="USAspending API"></p>

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/python/black) [![Pull Request Checks](https://github.com/fedspendingtransparency/usaspending-api/actions/workflows/pull-request-checks.yaml/badge.svg?branch=staging)](https://github.com/fedspendingtransparency/usaspending-api/actions/workflows/pull-request-checks.yaml) [![Test Coverage](https://codeclimate.com/github/fedspendingtransparency/usaspending-api/badges/coverage.svg)](https://codeclimate.com/github/fedspendingtransparency/usaspending-api/coverage) [![Code Climate](https://codeclimate.com/github/fedspendingtransparency/usaspending-api/badges/gpa.svg)](https://codeclimate.com/github/fedspendingtransparency/usaspending-api)

_This API is utilized by USAspending.gov to obtain all federal spending data which is open source and provided to the public as part of the DATA Act._

![USAspending Landing Page](readme.jpg?raw=true "Readme")

## Creating a Development Environment

Ensure the following dependencies are installed and working prior to continuing:

### Requirements
- [`docker`](https://docs.docker.com/install/) which will handle the other application dependencies.
- [`docker compose`](https://docs.docker.com/compose/)
- `bash` or another Unix Shell equivalent
    - Bash is available on Windows as [Windows Subsystem for Linux](https://docs.microsoft.com/en-us/windows/wsl/install-win10)
- [`git`](https://git-scm.com/downloads)
- [`make`](https://www.gnu.org/software/make/) for running build/test/run targets in the `Makefile`. (Run `$ make` for a list of targets.)

_**If not using Docker, you'll need to install app components on your machine:**_
> _Using Docker is recommended since it provides a clean environment. Setting up your own local environment requires some technical abilities and experience with modern software tools._

- Command line package manager
    - Windows' WSL bash uses `apt`
    - MacOS users can use [`Homebrew`](https://brew.sh/)
    - Linux users already know their package manager (`yum`, `apt`, `pacman`, etc.)
- [`PostgreSQL`](https://www.postgresql.org/download/) version 13.x (with a dedicated `data_store_api` database)
- [`Elasticsearch`](https://www.elastic.co/downloads/elasticsearch) version 7.1
- `Python` version 3.10 environment
  - Highly recommended to use a virtual environment. There are various tools and associated instructions depending on preferences
  - See [Required Python Libraries](#required-python-libraries) for an example using `pyenv`

### Cloning the Repository
Now, navigate to the base file directory where you will store the USAspending repositories

```shell
mkdir -p usaspending && cd usaspending
git clone https://github.com/fedspendingtransparency/usaspending-api.git
cd usaspending-api
```
### Environment Variables

Choose an option between `.env` and `.envrc` that best fits your preferred workflow. Pay close attention to the values in these environment variables as usage of `localhost` vs a container's name differ between local setups.

#### Create Your `.env` File (recommended)
Copy the template `.env` file with local runtime environment variables defined. Change as needed for your environment. _This file is git-ignored and will not be committed by git if changed._

```shell
cp .env.template .env
```

A `.env` file is a common way to manage environment variables in a declarative file. Certain tools, like `docker compose`, will read and honor these variables.

#### Create Your `.envrc` File
_[direnv](https://direnv.net/) is a shell extension that automatically runs shell commands in a `.envrc` file (commonly env var `export` commands) when entering or exiting a folder with that file_

Create a `.envrc` file in the repo root, which will be ignored by git. Change credentials and ports as-needed for your local dev environment.

```shell
export DATABASE_URL=postgres://usaspending:usaspender@localhost:5432/data_store_api
export ES_HOSTNAME=http://localhost:9200
export DATA_BROKER_DATABASE_URL=postgres://admin:root@localhost:5435/data_broker
```

If `direnv` does not pick this up after saving the file, type

```shell
direnv allow
```
_Alternatively, you could skip using `direnv` and just export these variables in your shell environment._

**Just make sure your env vars declared in the shell and in `.env` match for a consistent experience inside and outside of Docker**

### Build `usaspending-backend` Docker Image
_This image is used as the basis for running application components and running containerized setup services._

```shell
docker compose --profile usaspending build
```

_:bangbang: Re-run this command if any python package dependencies change (in `requirements/requirements-app.txt`), since they are baked into the docker image at build-time._

### Database Setup
A postgres database is required to run the app. You can run it in a `postgres` docker container (preferred), or run a PostgreSQL server on your local machine. In either case, it will be empty until data is loaded.

- :warning: If running your own PostgreSQL server be sure to:
    1. Have a DB named `data_store_api`
    2. A superuser role (user), e.g. `ALTER ROLE <<role/user you created>> WITH SUPERUSER;`
    3. Cross-check your `.env` or `.envrc` files if used to be sure it references your DBs user, password, host, and port where needed

##### Start the Postgres DB Container
_If not using your own local install..._

```shell
docker compose --profile usaspending up -d usaspending-db
```
... will create and run a Postgres database.

Use the following commands to create necessary users and set the `usaspending` user's search_path

```shell
docker exec -it usaspending-db sh -c " \
    psql \
        -h localhost \
        -p 5432 \
        -U usaspending \
        -d data_store_api \
        -c 'CREATE USER etl_user;' \
        -c 'CREATE USER readonly;' \
        -c 'ALTER USER usaspending SET search_path TO public,raw,int,temp,rpt;' \
"
```

##### Bring DB Schema Up-to-Date

- To run [Django migrations](https://docs.djangoproject.com/en/2.2/topics/migrations/).
    ```shell
    docker compose run --rm usaspending-manage python3 -u manage.py migrate
    ```
- To provision the materialized views which are required by certain API endpoints.
    ```shell
    docker compose run --rm usaspending-manage python3 -u manage.py matview_runner --dependencies
    ```

##### Seeding and Loading Database Data
_To just get essential reference data, you can run:_

-  To load essential reference data (agencies, program activity codes, CFDA program data, country codes, and others).
    ```shell
    docker compose run --rm usaspending-manage python3 -u manage.py load_reference_data
    ```

_Alternatively, to download a fully populuated production snapshot of the database (full or a subset) and restore it into PostgreSQL, use the `pg_restore` tool as described here: [USAspending Database Download](https://onevoicecrm.my.site.com/usaspending/s/database-download)_

- Recreate matviews with the command documented in the previous section if this is done

_**Executing individual data-loaders** to load in data is also possible, but requires more familiarity with those ad-hoc scripts and commands, and also requires an external data source (Data Broker DB, or external file, etc.) from which to load the data._

- For details on loading reference data, Data Accountability Broker Submissions, and current USAspending data into the API, see [loading_data.md](loading_data.md).
- For details on how our data loaders modify incoming data, see [data_reformatting.md](data_reformatting.md).

### Elasticsearch Setup
Some API endpoints reach into Elasticsearch for data.

```shell
docker compose --profile usaspending up -d usaspending-es
```
... will create and start a single-node Elasticsearch cluster as a docker container with data persisted to a docker volume.

- The cluster should be reachable via at http://localhost:9200 ("You Know, for Search").

- Optionally, to see log output, use `docker compose logs usaspending-es` (these logs are stored by docker even if you don't use this).

While not required, it is highly recommended to also create the Kibana docker container for querying the Elasticsearch cluster.

```shell
docker compose --profile usaspending up usaspending-kibana-es
```

#### Generate Elasticsearch Indexes
The following will generate the indexes:

```shell
CURR_DATE=$(date '+%Y-%m-%d-%H-%M-%S')
docker compose run --rm usaspending-manage python3 -u manage.py elasticsearch_indexer --create-new-index --index-name "$CURR_DATE-transactions" --load-type transaction
docker compose run --rm usaspending-manage python3 -u manage.py elasticsearch_indexer --create-new-index --index-name "$CURR_DATE-awards" --load-type award
docker compose run --rm usaspending-manage python3 -u manage.py elasticsearch_indexer --create-new-index --index-name "$CURR_DATE-recipients" --load-type recipient
docker compose run --rm usaspending-manage python3 -u manage.py elasticsearch_indexer --create-new-index --index-name "$CURR_DATE-locations" --load-type location
docker compose run --rm usaspending-manage python3 -u manage.py elasticsearch_indexer --create-new-index --index-name "$CURR_DATE-subaward" --load-type subaward
```

## Running the API

Run the following to bring up the Django app for the RESTful API:

```shell
docker compose --profile usaspending up usaspending-api
```

You can update environment variables in `settings.py` (buckets, elasticsearch, local paths) and they will be mounted and used when you run this.

The application will now be available at `http://localhost:8000`.

_**NOTE**: if the code was run outside of Docker then compiled Python files will potentially trip up the docker environment. A useful command to run for clearing out the files on your host is:_

```shell
find . | grep -E "(__pycache__|\.pyc|\.pyo$)" | xargs rm -rf
```

#### Using the API

In your local development environment, available API endpoints may be found at `http://localhost:8000/docs/endpoints`.

Deployed production API endpoints and docs are found by following links here: `https://api.usaspending.gov`.

## Running Tests

### Test Setup

1. Build the base `usaspending-backend` Docker image (the test container is based on this Docker image). In the parent **usaspending-api** directory run:

    ```shell
    docker build -t usaspending-backend .
    ```

2. Start the Spark containers for the Spark related tests
    ```shell
    docker compose --profile spark up -d
    ```

3. To run all USAspending tests in the docker services run
    ```shell
    docker compose run --rm -e DATA_BROKER_DATABASE_URL='' usaspending-test
    ```

_**NOTE**: If an env var named `DATA_BROKER_DATABASE_URL` is set, Broker Integration tests will attempt to be run as well. If doing so, Broker dependencies must be met (see below) or ALL tests will fail hard. Running the above command with `-e DATA_BROKER_DATABASE_URL=''` is a precaution to keep them excluded, unless you really want them (see below if so)._

To run tests locally and not in the docker services, you need:

1. **Postgres**: A running PostgreSQL database server _(See [Database Setup above](#database-setup))_
1. **Elasticsearch**: A running Elasticsearch cluster _(See [Elasticsearch Setup above](#elasticsearch-setup))_
1. **Environment Variables**: Tell python where to connect to the various data stores _(See [Environmnet Variables](#environment-variables))_
1. **Required Python Libraries**: Python package dependencies downloaded and discoverable _(See below)_

_**NOTE**: Running test locally might require you to run `export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES`. As discussed [here](https://github.com/rails/rails/issues/38560), there is an issue than can cause some of the Spark tests to fail without this environment variable set._

Once these are satisfied, run:

```shell
make tests
```

or, alternatively to skip using `make`

```shell
pytest
```

#### Required Python Libraries
Setup python, virtual environment, and pip dependencies, then check version info

```shell
make local-dev-setup
source $(make activate)
```

Your prompt should then look as below to show you are _in_ the virtual environment named `usaspending-api` (_to exit that virtual environment, simply type `deactivate` at the prompt_).

```shell
(usaspending-api) $
```

### Including Broker Integration Tests
Some automated integration tests run against a [Broker](https://github.com/fedspendingtransparency/data-act-broker-backend) database. If certain dependencies to run such integration tests are not satisfied, those tests will bail out and be marked as _Skipped_.
(You can see messages about those skipped tests by adding the `-rs` flag to pytest, like: `pytest -rs`)

To satisfy these dependencies and include execution of these tests, do the following:

1. Ensure the `Broker` source code is checked out alongside this repo at `../data-act-broker-backend`
1. Ensure you have [`Docker`](https://docs.docker.com/install/) installed and running on your machine
1. Ensure you have built the `Broker` backend Docker image by running:

    ```shell
    docker build -t dataact-broker-backend ../data-act-broker-backend
    ```
1. Ensure you have the `DATA_BROKER_DATABASE_URL` environment variable set, and it points to what will be a live PostgreSQL server (no database required) at the time tests are run.
    1. _WARNING: If this is set at all, then ALL above dependencies must be met or ALL tests will fail (Django will try this connection on ALL tests' run)_
    1. This DB could be one you always have running in a local Postgres instance, or one you spin up in a Docker container just before tests are run
1. If invoking `pytest` within a docker container (e.g. using the `usaspending-test` container), you _must_ mount the host's docker socket. This is declared already in the `docker-compose.yml` file services, but would be done manually with: `-v /var/run/docker.sock:/var/run/docker.sock`

_**NOTE**: Broker source code should be re-fetched and image rebuilt to ensure latest integration is tested_

Re-running the test suite using `pytest -rs` with these dependencies satisfied should yield no more skips of the broker integration tests.

**Example Test Invocations of _Just a Few_ Broker Integration Tests:** (_i.e. using `-k`_)

_From within a container_

_**NOTE**: `DATA_BROKER_DATABASE_URL` is set in the `docker-compose.yml` file (and could pick up `.env` values, if set)_

```shell
docker compose run --rm usaspending-test pytest --capture=no --verbose --tb=auto --no-cov --log-cli-level=INFO -k test_broker_integration
```

_From Developer Desktop_

_**NOTE**: `DATA_BROKER_DATABASE_URL` is set in the `.envrc` file and available in the shell_
```shell
pytest --capture=no --verbose --tb=auto --no-cov --log-cli-level=INFO -k test_broker_integration
```

## Contributing

To submit fixes or enhancements, or to suggest changes, see [CONTRIBUTING.md](CONTRIBUTING.md).

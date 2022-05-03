# <p align="center"><img src="https://www.usaspending.gov/img/logo@2x.png" alt="USAspending API"></p>

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/python/black) [![Build Status](https://travis-ci.com/fedspendingtransparency/usaspending-api.svg?branch=master)](https://travis-ci.com/fedspendingtransparency/usaspending-api) [![Test Coverage](https://codeclimate.com/github/fedspendingtransparency/usaspending-api/badges/coverage.svg)](https://codeclimate.com/github/fedspendingtransparency/usaspending-api/coverage) [![Code Climate](https://codeclimate.com/github/fedspendingtransparency/usaspending-api/badges/gpa.svg)](https://codeclimate.com/github/fedspendingtransparency/usaspending-api)

_This API is utilized by USAspending.gov to obtain all federal spending data which is open source and provided to the public as part of the DATA Act._

![USAspending Landing Page](readme.jpg?raw=true "Readme")

## Creating a Development Environment

Ensure the following dependencies are installed and working prior to continuing:

### Requirements
- [`docker`](https://docs.docker.com/install/) which will handle the other application dependencies.
- [`docker-compose`](https://docs.docker.com/compose/)
- `bash` or another Unix Shell equivalent
    - Bash is available on Windows as [Windows Subsystem for Linux](https://docs.microsoft.com/en-us/windows/wsl/install-win10)
- [`git`](https://git-scm.com/downloads)

#### If not using Docker:
> Using Docker is recommended since it provides a clean environment. Setting up your own local environment requires some technical abilities and experience with modern software tools.

- Command line package manager
    - Windows' WSL bash uses `apt-get`
    - MacOS users will use [`Homebrew`](https://brew.sh/)
    - Linux users already know their package manager (yum, apt, pacman, etc.)
- [`PostgreSQL`](https://www.postgresql.org/download/) version 10.x (with a dedicated `data_store_api` database)
- [`Elasticsearch`](https://www.elastic.co/downloads/elasticsearch) version 7.1
- Python 3.7 environment
  - Highly recommended to use a virtual environment. There are various tools and associated instructions depending on preferences
  - See [Required Python Libraries](#required-python-libraries) for an example using `pyenv`

### Cloning the Repository
Now, navigate to the base file directory where you will store the USAspending repositories

    $ mkdir -p usaspending && cd usaspending
    $ git clone https://github.com/fedspendingtransparency/usaspending-api.git
    $ cd usaspending-api

### Create Your `.env` File
Copy the template `.env` file with comment local runtime environment variables defined. Change as needed for your environment. _This file is git-ignored and will not be committed by git if changed._

```shell
$ cp .env.template .env
```
    
### Build `usaspending-backend` Docker Image
_This image is used as the basis for running application components and running containerized setup services._

```shell
$ docker-compose --profile usaspending build
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
$ docker-compose --profile usaspending up usaspending-db
```
... will create and run a Postgres database.

##### Bring DB Schema Up-to-Date

- `docker-compose run --rm usaspending-manage python3 -u manage.py migrate` will run Django migrations: [https://docs.djangoproject.com/en/2.2/topics/migrations/](https://docs.djangoproject.com/en/2.2/topics/migrations/).

- `docker-compose run --rm usaspending-manage python3 -u manage.py matview_runner --dependencies`  will provision the materialized views which are required by certain API endpoints.

##### Seeding and Loading Database Data
_To just get essential reference data, you can run:_

- `docker-compose run --rm usaspending-manage python3 -u manage.py load_reference_data` will load essential reference data (agencies, program activity codes, CFDA program data, country codes, and others).

_To download a full production snapshot of the database or a subset of the database and loading it into PostgreSQL, use the `pg_restore` tool as described here: [USAspending Database Download](https://files.usaspending.gov/database_download/)_

- Recreate matviews with the command documented in the previous section if this is done

_**Executing individual data-loaders** to load in data is also possible, but requires more familiarity with those ad-hoc scripts and commands, and also requires an external data source (DATA Broker DB, or external file, etc.) from which to load the data._

- For details on loading reference data, DATA Act Broker submissions, and current USAspending data into the API, see [loading_data.md](loading_data.md).
- For details on how our data loaders modify incoming data, see [data_reformatting.md](data_reformatting.md).

### Elasticsearch Setup
Some of the API endpoints reach into Elasticsearch for data.

```shell
$ docker-compose --profile usaspending up usaspending-es`
```
... will create and start a single-node Elasticsearch cluster as a docker container with data persisted to a docker volume.

- The cluster should be reachable via at http://localhost:9200 ("You Know, for Search").

- Optionally, to see log output, use `docker-compose logs usaspending-es` (these logs are stored by docker even if you don't use this).

##### Generate Elasticsearch Indexes
The following will generate two base indexes, one for transactions and one for awards:
 
```shell
$ docker-compose run --rm usaspending-manage python3 -u manage.py elasticsearch_indexer --create-new-index --index-name 01-26-2022-transactions --load-type transaction
$ docker-compose run --rm usaspending-manage python3 -u manage.py elasticsearch_indexer --create-new-index --index-name 01-26-2022-awards --load-type award
```  

## Running the API
```shell
docker-compose --profile usaspending up usaspending-api
```
... will bring up the Django app for the RESTful API

- You can update environment variables in `settings.py` (buckets, elasticsearch, local paths) and they will be mounted and used when you run this.

The application will now be available at `http://localhost:8000`.

Note: if the code was run outside of Docker then compiled Python files will potentially trip up the docker environment. A useful command to run for clearing out the files on your host is:

    find . | grep -E "(__pycache__|\.pyc|\.pyo$)" | xargs rm -rf

#### Using the API

In your local development environment, available API endpoints may be found at `http://localhost:8000/docs/endpoints`

Deployed production API endpoints and docs are found by following links here: `https://api.usaspending.gov`

## Running Tests

### Test Setup

To run all USAspending tests in the docker services run

    docker-compose run --rm -e DATA_BROKER_DATABASE_URL='' usaspending-test

_NOTE: If an env var named `DATA_BROKER_DATABASE_URL` is set, Broker Integration tests will attempt to be run as well. If doing so, Broker dependencies must be met (see below) or ALL tests will fail hard. Running the above command with `-e DATA_BROKER_DATABASE_URL=''` is a precaution to keep them excluded, unless you really want them (see below if so)._

To run tests locally and not in the docker services, you need:

1. **Postgres** A running PostgreSQL database server _(See [Database Setup above](#database-setup))_
1. **Elasticsearch** A running Elasticsearch cluster _(See [Elasticsearch Setup above](#elasticsearch-setup))_
1. **Required Python Libraries** Python package dependencies downloaded and discoverable _(See below)_
1. **Environment Variables** Tell python where to connect to the various data stores _(See below)_

Once these are satisfied, run:

    (usaspending-api) $ pytest

#### Required Python Libraries
Create and activate the virtual environment using `venv`, and ensure the right version of Python 3.7.x is being used (the latest RHEL package available for `python36u`: _as of this writing_)

    $ pyenv install 3.7.2
    $ pyenv local 3.7.2
    $ python -m venv .venv/usaspending-api
    $ source .venv/usaspending-api/bin/activate


Your prompt should then look as below to show you are _in_ the virtual environment named `usaspending-api` (_to exit that virtual environment, simply type `deactivate` at the prompt_).

    (usaspending-api) $

[`pip`](https://pip.pypa.io/en/stable/installing/) `install` application dependencies

    (usaspending-api) $ pip install -r requirements/requirements.txt

#### Environment Variables

##### `.envrc` File
_[Direnv](https://direnv.net/) is a shell extension that automatically runs shell commands in a `.envrc` file (commonly env var `export` commands) when entering or exiting a folder with that file_

Create a `.envrc` file in the repo root, which will be ignored by git. Change credentials and ports as-needed for your local dev environment.

```bash
export DATABASE_URL=postgres://usaspending:usaspender@localhost:5432/data_store_api
export ES_HOSTNAME=http://localhost:9200
export DATA_BROKER_DATABASE_URL=postgres://admin:root@localhost:5435/data_broker
```

If `direnv` does not pick this up after saving the file, type

    $ direnv allow

_Alternatively, you could skip using `direnv` and just export these variables in your shell environment._

##### `.env` File
Declaring `NAME=VALUE` variables in a git-ignored `.env` file is a common way to manage environment variables in a declarative file. Certain tools, like `docker-compose`, will read and honor these variables.

If you copied `.env.template` to `.env`, then review any variables you want to change to be consistent with your local runtime environment.

### Including Broker Integration Tests
Some automated integration tests run against a [Broker](https://github.com/fedspendingtransparency/data-act-broker-backend) database. If certain dependencies to run such integration tests are not satisfied, those tests will bail out and be marked as _Skipped_.
(You can see messages about those skipped tests by adding the `-rs` flag to pytest, like: `pytest -rs`)

To satisfy these dependencies and include execution of these tests, do the following:

1. Ensure the `Broker` source code is checked out alongside this repo at `../data-act-broker-backend`
1. Ensure you have [`Docker`](https://docs.docker.com/install/) installed and running on your machine
1. Ensure you have built the `Broker` backend Docker image by running:

    ```shell
    (usaspending-api) $ docker build -t dataact-broker-backend ../data-act-broker-backend
    ```
1. Ensure you have the `DATA_BROKER_DATABASE_URL` environment variable set, and it points to what will be a live PostgreSQL server (no database required) at the time tests are run.
    1. _WARNING: If this is set at all, then ALL above dependencies must be met or ALL tests will fail (Django will try this connection on ALL tests' run)_
    1. This DB could be one you always have running in a local Postgres instance, or one you spin up in a Docker container just before tests are run
1. If invoking `pytest` within a docker container (e.g. using the `usaspending-test` container), you _must_ mount the host's docker socket. This is declared already in the `docker-compose.yml` file services, but would be done manually with: `-v /var/run/docker.sock:/var/run/docker.sock`

_NOTE: Broker source code should be re-fetched and image rebuilt to ensure latest integration is tested_

Re-running the test suite using `pytest -rs` with these dependencies satisfied should yield no more skips of the broker integration tests.

**Example Test Invocations of _Just a Few_ Broker Integration Tests:** (_i.e. using `-k`_)

_From within a container_

(NOTE: `DATA_BROKER_DATABASE_URL` is set in the `docker-compose.yml` file (and could pick up `.env` values, if set)
```bash
(usaspending-api) $ docker-compose run --rm usaspending-test pytest --capture=no --verbose --tb=auto --no-cov --log-cli-level=INFO -k test_broker_integration
```

_From Developer Desktop_

(NOTE: `DATA_BROKER_DATABASE_URL` is set in the `.envrc` file and available in the shell)
```bash
(usaspending-api) $ pytest --capture=no --verbose --tb=auto --no-cov --log-cli-level=INFO -k test_broker_integration
```

## Contributing
To submit fixes or enhancements, or to suggest changes, see [CONTRIBUTING.md](CONTRIBUTING.md)

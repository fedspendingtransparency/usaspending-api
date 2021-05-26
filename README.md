# <p align="center"><img src="https://www.usaspending.gov/img/logo@2x.png" alt="USAspending API"></p>

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/python/black) [![Build Status](https://travis-ci.com/fedspendingtransparency/usaspending-api.svg?branch=master)](https://travis-ci.com/fedspendingtransparency/usaspending-api) [![Test Coverage](https://codeclimate.com/github/fedspendingtransparency/usaspending-api/badges/coverage.svg)](https://codeclimate.com/github/fedspendingtransparency/usaspending-api/coverage) [![Code Climate](https://codeclimate.com/github/fedspendingtransparency/usaspending-api/badges/gpa.svg)](https://codeclimate.com/github/fedspendingtransparency/usaspending-api)

_This API is utilized by USAspending.gov to obtain all federal spending data which is open source and provided to the public as part of the DATA Act._

![USAspending Landing Page](readme.jpg?raw=true "Readme")

## Creating a Development Environment

Ensure the following dependencies are installed and working prior to continuing:

### Requirements
- [`Docker`](https://docs.docker.com/install/) which will handle the other application dependencies.
- `Bash` or another Unix Shell equivalent
    - Bash is available on Windows as [Windows Subsystem for Linux](https://docs.microsoft.com/en-us/windows/wsl/install-win10)
- [`Git`](https://git-scm.com/downloads)

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

### Database Setup
There are three documented options for setting up a local database in order to run the API:

1. **Local Empty DB**. Use your own local postgres database for the API to use.
2. **Containerized Empty DB**. Create an empty directory on your localhost where all the database files will persist and use the docker-compose file to bring up a containerized postgres database.
3. **Local Populated DB**. Download either the _whole_ database or a database subset from the USAspending website.

#### Option 1: Using a Locally Hosted Postgres Database
Create a Local postgres database called 'data_store_api' and either create a new username and password for the database or use all the defaults. For help, consult:
- [Postgres Setup Help](https://medium.com/coding-blocks/creating-user-database-and-adding-access-on-postgresql-8bfcd2f4a91e)

Make sure to grant whatever user you created for the data_store api database superuser permissions or some scripts will not work:

    postgres=# ALTER ROLE <<role/user you created>> WITH SUPERUSER;

#### Option 2: Using the Docker Compose Postgres Database
See below for basic setup instructions. For help with Docker Compose:
- [Docker Compose](https://docs.docker.com/compose/)


##### Database Setup and Initialization with Docker Compose

- **None of these commands will rebuild a Docker image! Use `--build` if you make changes to the code or want to rebuild the image before running the `up` steps.**

- **If you run a local database**, set `POSTGRES_HOST` in `.env` to `host.docker.internal`. `POSTGRES_PORT` should be changed if it isn't 5432.

    - `docker-compose up usaspending-db` will create and run a Postgres database.

    - `docker-compose run --rm usaspending-manage python3 -u manage.py migrate` will run Django migrations: [https://docs.djangoproject.com/en/2.2/topics/migrations/](https://docs.djangoproject.com/en/2.2/topics/migrations/).

    - `docker-compose run --rm usaspending-manage python3 -u manage.py load_reference_data` will load essential reference data (agencies, program activity codes, CFDA program data, country codes, and others).

    - `docker-compose run --rm usaspending-manage python3 -u manage.py matview_runner --dependencies`  will provision the materialized views which are required by certain API endpoints.

##### Manual Database Setup
- `docker-compose.yaml` contains the shell commands necessary to set up the database manually, if you prefer to have a more custom environment.

#### Option 3: Downloading the database or a subset of the database and loading it into PostgreSQL

For further instructions on how to download, use, and setup the database using a subset of our data please go to:

[USAspending Database Download](https://files.usaspending.gov/database_download/)

### Elasticsearch Setup
Some of the API endpoints reach into Elasticsearch for data.

- `docker-compose up usaspending-es` will create and start a single-node Elasticsearch cluster, using the `ES_CLUSTER_DIR` specified in the `.env` configuration file. We recommend using a folder outside of the usaspending-api project directory so it does not get copied to other containers.

- The cluster should be reachable via at http://localhost:9200 ("You Know, for Search").

- Optionally, to see log output, use `docker-compose logs usaspending-es` (these logs are stored by docker even if you don't use this).

## Running the API
`docker-compose up usaspending-api`

- You can update environment variables in `settings.py` (buckets, elasticsearch, local paths) and they will be mounted and used when you run this.

The application will now be available at `http://localhost:8000`.

Note: if the code was run outside of Docker then compiled Python files will potentially trip up the docker environment. A useful command to run for clearing out the files on your host is:

    find . | grep -E "(__pycache__|\.pyc|\.pyo$)" | xargs rm -rf

### Using the API

In your local development environment, available API endpoints may be found at `http://localhost:8000/docs/endpoints`

Deployed production API endpoints and docs are found by following links here: `https://api.usaspending.gov`

## Loading Data

_Note: it is possible to run ad-hoc commands out of a Docker container once you get the hang of it, see the comments in the Dockerfile._

For details on loading reference data, DATA Act Broker submissions, and current USAspending data into the API, see [loading_data.md](loading_data.md).

For details on how our data loaders modify incoming data, see [data_reformatting.md](data_reformatting.md).

## Running Tests

### Test Setup

To run all tests in the docker services run

    docker-compose run --rm usaspending-test

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
Create a `.envrc` file in the repo root, which will be ignored by git. Change credentials and ports as-needed for your local dev environment.

```bash
export DATABASE_URL=postgres://usaspending:usaspender@localhost:5432/data_store_api
export ES_HOSTNAME=http://localhost:9200
export DATA_BROKER_DATABASE_URL=postgres://admin:root@localhost:5435/data_broker
```

If `direnv` does not pick this up after saving the file, type

    $ direnv allow

_Alternatively, you could skip using `direnv` and just export these variables in your shell environment._

### Including Broker Integration Tests
Some automated integration tests run against a [Broker](https://github.com/fedspendingtransparency/data-act-broker-backend) database. If the dependencies to run such integration tests are not satisfied, those tests will bail out and be marked as _Skipped_.
(You can see messages about those skipped tests by adding the `-rs` flag to pytest, like: `pytest -rs`)

To satisfy these dependencies and include execution of these tests, do the following:
1. Ensure you have [`Docker`](https://docs.docker.com/install/) installed and running on your machine
1. Ensure the `Broker` source code is checked out alongside this repo at `../data-act-broker-backend`
1. Ensure you have the `DATA_BROKER_DATABASE_URL` environment variable set, and pointing to a live PostgreSQL server (no database required)
1. Ensure you have built the `Broker` backend Docker image by running:

```shell
    (usaspending-api) $ docker build -t dataact-broker-backend ../data-act-broker-backend
```

_NOTE: Broker source code should be re-fetched and image rebuilt to ensure latest integration is tested_

Re-running the test suite using `pytest -rs` with these dependencies satisfied should yield no more skips of the broker integration tests.

## Contributing
To submit fixes or enhancements, or to suggest changes, see [CONTRIBUTING.md](CONTRIBUTING.md)

<!-- temp test commit2 -->

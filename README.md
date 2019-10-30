# <p align="center"><img src="https://www.usaspending.gov/img/logo@2x.png" alt="USAspending API"></p>

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/python/black) [![Build Status](https://travis-ci.com/fedspendingtransparency/usaspending-api.svg?branch=master)](https://travis-ci.com/fedspendingtransparency/usaspending-api) [![Test Coverage](https://codeclimate.com/github/fedspendingtransparency/usaspending-api/badges/coverage.svg)](https://codeclimate.com/github/fedspendingtransparency/usaspending-api/coverage) [![Code Climate](https://codeclimate.com/github/fedspendingtransparency/usaspending-api/badges/gpa.svg)](https://codeclimate.com/github/fedspendingtransparency/usaspending-api)

_This API is utilized by USAspending.gov to obtain all federal spending data which is open source and provided to the public as part of the DATA Act._

![USAspending Landing Page](readme.jpg?raw=true "Readme")

## Creating a Development Environment

Ensure the following dependencies are installed and working prior to continuing:

### Requirements
- [`PostgreSQL`](https://www.postgresql.org/download/) 10.x (with a dedicated `data_store_api` database)
- [`direnv`](https://github.com/direnv/direnv#install)
  - For Mac OSX, be sure to put the hook in your `~/.bash_profile`, not `~/.bashrc`
- `Bash` or another Unix Shell equivalent
  - Bash is available on Windows as [Windows Subsystem for Linux](https://docs.microsoft.com/en-us/windows/wsl/install-win10)
- Command line package manager
  - Windows' WSL bash uses `apt-get`
  - OSX users will use [`Homebrew`](https://brew.sh/)

### Cloning the Repository
Now, navigate to the base file directory where you will store the USAspending repositories

    $ mkdir -p usaspending && cd usaspending
    $ git clone https://github.com/fedspendingtransparency/usaspending-api.git
    $ cd usaspending-api

## Database Setup
There are three documented options for setting up a local database in order to run the API:

1. Use your own local postgres database for the API to use.
2. Create an empty directory on your localhost where all the database files will persist and use the docker-compose file to bring up a containerized postgres database.
3. Download either the _whole_ database or a database subset from the USAspending website.

### Option 1: Using a Locally Hosted Postgres Database
Create a Local postgres database called 'data_store_api' and either create a new username and password for the database or use all the defaults. For help, consult:
 - ['Postgres Setup Help'](https://medium.com/coding-blocks/creating-user-database-and-adding-access-on-postgresql-8bfcd2f4a91e)

Make sure to grant whatever user you created for the data_store api database superuser permissions or some scripts will not work:

    postgres=# ALTER ROLE <<role/user you created>> WITH SUPERUSER;

### Option 2: Using the Docker Compose Postgres Database
See below for basic setup instructions. For help with Docker Compose:
 - [Docker Installation](https://docs.docker.com/install/)
 - [Docker Compose](https://docs.docker.com/compose/)


#### Database Setup and Initialization with Docker Compose

- **None of these commands will rebuild a Docker image! Use `--build` if you make changes to the code or want to rebuild the image before running the `up` steps.**

- **If you run a local database**, set `POSTGRES_HOST` in `.env` to `host.docker.internal`. `POSTGRES_PORT` should be changed if it isn't 5432.

	- `docker-compose up usaspending-db` will create and run a Postgres database in the `POSTGRES_CLUSTER_DIR` specified in the `.env` configuration file. We recommend using a folder *outside* of the usaspending-api project directory so it does not get copied to other containers in subsequent steps.

	- `docker-compose run usaspending-manage python -u manage.py migrate` will run Django migrations: [https://docs.djangoproject.com/en/2.2/topics/migrations/](https://docs.djangoproject.com/en/2.2/topics/migrations/).

	- `docker-compose run usaspending-manage python -u manage.py load_reference_data` will load essential reference data (agencies, program activity codes, CFDA program data, country codes, and others).

	- `docker-compose up usaspending-db-sql`, then `docker-compose up usaspending-db-init` will provision the custom materialized views which are required by certain API endpoints.

#### Manual Database Setup
- `docker-compose.yaml` contains the shell commands necessary to set up the database manually, if you prefer to have a more custom environment.

### Option 3: Downloading the database or a subset of the database and loading it into PostgreSQL

For further instructions on how to download, use, and setup the database using a subset of our data please go to:

[USAspending Database Download](https://files.usaspending.gov/database_download/)

## Elasticsearch Setup
Some of the API endpoints reach into Elasticsearch for data.

- `docker-compose up usaspending-es` will create and start a single-node Elasticsearch cluster, using the `ES_CLUSTER_DIR` specified in the `.env` configuration file. We recommend using a folder outside of the usaspending-api project directory so it does not get copied to other containers.

- The cluster should be reachable via at http://localhost:9200 ("You Know, for Search").

- Optionally, to see log output, use `docker-compose logs usaspending-es` (these logs are stored by docker even if you don't use this).

## Running the API
`docker-compose up usaspending-api`

- You can update environment variables in `settings.py` (buckets, elasticsearch, local paths) and they will be mounted and used when you run this.

The application will now be available at `http://localhost:8000`.

## Using the API

In your local development environment, available API endpoints may be found at `http://localhost:8000/docs/endpoints`

Deployed production API endpoints and docs are found by following links here: `https://api.usaspending.gov`

## Loading Data

_Note: it is possible to run ad-hoc commands out of a Docker container once you get the hang of it, see the comments in the Dockerfile._

For details on loading reference data, DATA Act Broker submissions, and current USAspending data into the API, see [loading_data.md](loading_data.md).

For details on how our data loaders modify incoming data, see [data_reformatting.md](data_reformatting.md).

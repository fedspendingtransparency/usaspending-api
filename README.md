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
There are two options for how you want to setup your database in order to run the API.  You can:
    1. Use your own local postgres database for the API to use.
    2. Create an empty directory on your localhost where all the database files will persist and use the docker-compose file to bring up a containerized postgres database

#### Option 1: Using a Locally Hosted Postgres Database
Create a Local postgres database called 'data_store_api' and either create a new username and password for the database or use all the defaults. For help, consult:
 - ['Postgres Setup Help'](https://medium.com/coding-blocks/creating-user-database-and-adding-access-on-postgresql-8bfcd2f4a91e)

Make sure to grant whatever user you created for the data_store api database superuser permissions or some scripts will not work:

    postgres=# ALTER ROLE <<role/user you created>> WITH SUPERUSER;

#### Option 2: Using the Docker Compose Postgres Database
See below for basic setup instructions. For help with Docker Compose:
 - [Docker Installation](https://docs.docker.com/install/)
 - [Docker Compose](https://docs.docker.com/compose/)


### Database Setup and Initialization with Docker Compose

- **None of these commands will rebuild a Docker image! Use `--build` if you make changes to the code or want to rebuild the image before running the `up` steps.**

- **If you run a local database**, set `POSTGRES_HOST` in `.env` to `host.docker.internal`. `POSTGRES_PORT` should be changed if it isn't 5432.

	- `docker-compose up usaspending-db` will create and run a Postgres database in the `POSTGRES_CLUSTER_DIR` specified in the `.env` configuration file. We recommend using a folder *outside* of the usaspending-api project directory so it does not get copied to other containers in subsequent steps.

	- `docker-compose up usaspending-db-migrate` will run Django migrations: [https://docs.djangoproject.com/en/2.2/topics/migrations/]().

	- `docker-compose up usaspending-ref-data` will load essential reference data (agencies, program activity codes, CFDA program data, country codes, and others).

	- `docker-compose up usaspending-db-sql`, then `docker-compose up usaspending-db-init` will provision the custom materialized views which are required by certain API endpoints.

#### Manual Database Setup
- `docker-compose.yaml` contains the shell commands necessary to set up the database manually, if you prefer to have a more custom environment.

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

## Restoring From USAspending Database Subset

If you would like to test API functionality, but do not need the whole usaspending database, feel free to download a random monthly subset of our database from the link below:

[usaspending\_database\_subset](https://usaspending-db.s3.us-gov-west-1.amazonaws.com/usaspending-db-subset_20190812.zip?response-content-disposition=attachment&X-Amz-Security-Token=FQoDYXdzEEoaDBwDbzBuUHP1gRO10iLVA7NEleoUyKHBeEz89I6NeABo0p0iPywjfsemMZTKNkSineyBACHAV3kABIbzEFLxzjLZCbqPFfjzh%2FNI9LG1jeAnwicJfOQrEAH%2FtPtZbVPuJlow2fV3qYvUDBZ3UqJmE5%2Byqnd%2FUzuChGL6ejky33jpT5k9lJaHmiFBHg6k2vK40HSzP9jL9Mk0FRL%2FYBP%2Bi3j8uau9wMs5HmXoJ8zNh0dBenXlPjcjb%2FoEjGjdph3WSv0M57%2FU6iZ%2FEzE85yWXdw2oUEo3RaOss6YoBZlV%2F02t%2FwrcAgRByESsrENqEKtAKOwDQ6r5sdDZl5QxFv8Xrw00AfEIznHlFHjrxwPXJHp%2F5Qo5kSx%2F0abzmAs7daMrNQm%2B%2FHrVzo9B0w9uXB87YkxEHLFzWJcbwFXGSD7KzFHzAxQVCyPjokAfnGLATVt4vdHfUeLs14iCsOQnY8YJMBQvAo9Ga8v%2Fbes4aHnTqMwM34JoIyDuir9YTrkrKdLi1Lxy%2BrwhePuz8O69ApIctbzC1HfkmVbzhhEHe2k%2BGtDsjkbsWAptwZgqcvKovmMfBRdDcXtUpnYQB92DaaL5IycLdGHs9BUM%2FzODCijdVTwwq9ANyMjsSg4KN%2BRTdFZz3djsbLYouOCQ6wU%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20190826T191750Z&X-Amz-SignedHeaders=host&X-Amz-Expires=300&X-Amz-Credential=ASIAZB5UNHKRY363UMQB%2F20190826%2Fus-gov-west-1%2Fs3%2Faws4_request&X-Amz-Signature=d0bd6ab61e5905ffea0506ca25a1aca8b4419ab50b3d77d71f961b98106ff972)

Once you have downloaded the dataset, you should log into your postgres database and run the following commands:

```
psql postgres://usaspending:usaspender@localhost:5432/postgres
DROP DATABASE data_store_api;
CREATE DATABASE data_store_api;
```
With the database still running, run the following command to load the database subset into your newly created *data\_store\_api* database:

_Note: Change {{ path\_to\_database\_subset }} below to be the actual path to your unzipped database subset:_

```
pg_restore  --no-owner --no-privileges --job 16 --dbname \
"postgres://usaspending:usaspender@localhost:5432/data_store_api" \
"{{ path_to_database_subset }}/pruned_data_store_api_dump"
```

Once the pg_restore finishes check the database to see if the tables were loaded using your favorite database client or run the following SQL command:

`SELECT id FROM awards LIMIT 100;`

Also verify that data is being loaded into your elasticsearch cluster by checking one of the indexes.  You can run the following command to check: 

`curl localhost:9200/api/v2/search/spending_by_transaction/`


#### _Helpful Hints/Error Handling_
***

_Note: If you get the error: **pg\_restore: [archiver (db)] could not execute query: ERROR:  schema "public" already exists**, simply stop the pg_restore process and rerun to fix._

_Note: If you get the error: **checkpoints are occurring too frequently (# seconds apart) \n Consider increasing the configuration parameter "max\_wal\_size".** Use the following commands to increase the max\_wal\_size of your database_:

```
# Figure out where your database conf file lives
# Login to your postgres database and run the following command:

SHOW data_directory;

# The output should be something like the one below

      data_directory      
--------------------------
 /var/lib/postgresql/data
 
 # Next, run the following command to update your max_wal_size (Note that if your postgres
 database is running out of a container you will need to copy your postgresql.conf file from 
 your localhost to your container and update the value of the max_wal_size locally):
 
 \! echo max_wal_size='2GB' >> /var/lib/postgresql/data/postgresql.conf
 
 # Verify that the max_wal_size has been updated by restarting your database and running:
 
 show max_wal_size;
 
 # Your new output should look like:
 
 max_wal_size 
--------------
 2GB
(1 row)
```

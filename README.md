# <p align="center"><img src="https://www.usaspending.gov/img/logo@2x.png" alt="USAspending API"></p>

[![Build Status](https://travis-ci.org/fedspendingtransparency/usaspending-api.svg?branch=master)](https://travis-ci.org/fedspendingtransparency/usaspending-api) [![Test Coverage](https://codeclimate.com/github/fedspendingtransparency/usaspending-api/badges/coverage.svg)](https://codeclimate.com/github/fedspendingtransparency/usaspending-api/coverage) [![Code Climate](https://codeclimate.com/github/fedspendingtransparency/usaspending-api/badges/gpa.svg)](https://codeclimate.com/github/fedspendingtransparency/usaspending-api)

_This API is utilized by USAspending.gov to obtain all federal spending data which is open source and provided to the public as part of the DATA Act._

![USAspending Landing Page](readme.jpg?raw=true "Readme")

## Install

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

### Setup
There are two options for how you want to setup your database in order to buildout the API.  You can:
    1. Use your own local postgres database for the API Application to use
    2. Create an empty directory on your localhost where all the database files will persist and use the docker-compose file to bring up a containerized postgres database

#### Using a Locally Hosted Postgres Database
Create a Local postgres database called 'data_store_api' and either create a new username and password for the database or use all the defaults.  If you are confused on how to do this consult: 
 - ['Postgres Setup Help'](https://medium.com/coding-blocks/creating-user-database-and-adding-access-on-postgresql-8bfcd2f4a91e)

Make sure to grant whatever user you created for the data_store api database superuser permissions or some scripts will not work:

    postgres=# ALTER ROLE <<role/user you created>> WITH SUPERUSER;

Also, be sure to remove the **db** section from the services list in the docker-compose.yml file so that your app container does not try connecting to the docker postgres database and change the DATABASE_URL connection strings to match your local database connection.

:bulb: HINT: Be sure to also change the HOST section of the database url to be host.docker.interal in order to point docker to your localhost and not the localhost address of the app container

#### Use the docker-compose Postgres Database
If you would like to use docker-compose to run the database for you be sure to update your docker-compose file to include a pointer to an empty directory on your host machine where you want to store your postgres database information

### Clone the Repository
Now, navigate to the base file directory where you will store the USAspending repositories

    $ mkdir -p usaspending && cd usaspending
    $ git clone https://github.com/fedspendingtransparency/usaspending-api.git
    $ cd usaspending-api

We will be creating usaspending-api using the Dockerfile and docker-compose.  For more information on these consult the following:
 - ['Docker-Installation'](https://docs.docker.com/install/)
 - ['Docker-Compose'](https://docs.docker.com/compose/)

Go into the dockerfile and make the required variable changes to match your information.
Run the following command to buildout the docker container in the usaspending-api directory

    $ docker build . -t usaspendingapi

Once the docker image has been built run docker-compose up to initialize the container

    $ docker-compose up

For now the rest of the commands need to be run inside the docker container. Run the following command to create a bash session inside the container:

    $ docker exec -it <container-name> /bin/bash

Next, test the database connection and upate the `data_store_api` schema

    (container-name) # ./manage.py migrate

Right now, some endpoints won't work because they are dependent on materialized views to run. To create them, run the following from your usaspending-api folder:

    (container-name) # psql -f usaspending_api/database_scripts/matviews/functions_and_enums.sql $DATABASE_URL

The above command should output something similar to the following (if you get an error about not being a superuser or not having the right permissions then go into your local postgres database and make sure that you properly assigned the user you created in the data_store_api database to have superuser permissions):

    CREATE EXTENSION
    DO
    CREATE FUNCTION

Once the command has finished create a directory anywhere outside the usaspending-api directory and run the following within the usaspending-api directory:

    (container-name) # python -u usaspending_api/database_scripts/matview_generator/matview_sql_generator.py --dest=<destination dir>

Now we need to create a readonly role in order to properly load the schema into our database:

    (container-name) # psql $DATABASE_URL -c "CREATE ROLE readonly"

Change your directory path to be the directory you created in the previous step and run:

    (container-name) # cat *.sql | psql $DATABASE_URL -f -

Finally, go back to the usaspending directory and start up the site

    (container-name) # ./manage.py runserver

The application will now be available at `http://localhost:8000`!

## API

In your local development environment, available API endpoints may be found at `http://localhost:8000/docs/endpoints`

Deployed production API endpoints and docs are found by following links here: `https://api.usaspending.gov`

## Loading Data

For details on loading reference data, DATA Act Broker submissions, and current USAspending data into the API, see [loading_data.md](loading_data.md).

For details on how our data loaders modify incoming data, see [data_changes.md](data_changes.md).

## Public Domain License

This project is in the public domain within the United States, and copyright and related rights in the work worldwide are waived through the CC0 1.0 Universal public domain dedication.

All contributions to this project will be released under the CC0 dedication. By submitting a pull request, you are agreeing to comply with this waiver of copyright interest.

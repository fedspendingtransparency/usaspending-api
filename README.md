# USASpending API



[![Build Status](https://travis-ci.org/fedspendingtransparency/usaspending-api.svg?branch=master)](https://travis-ci.org/fedspendingtransparency/usaspending-api)
[![Test Coverage](https://codeclimate.com/github/fedspendingtransparency/usaspending-api/badges/coverage.svg)](https://codeclimate.com/github/fedspendingtransparency/usaspending-api/coverage)[![Code Climate](https://codeclimate.com/github/fedspendingtransparency/usaspending-api/badges/gpa.svg)](https://codeclimate.com/github/fedspendingtransparency/usaspending-api)

This is the API that will drive the new USAspending.gov. It is currently under active development and not stable.

## Getting Started

This API is built on Python 3.5+, Django, and Django Rest Framework.

Assumptions:

* You're able to install software on your local machine
* You have git installed on your machine and are able to clone code repositories from GitHub. If this isn't the case, the easiest way to get started is to install [GitHub Desktop](https://desktop.github.com/ "GitHub desktop"), available for Windows or Mac.
* You're familiar with opening a terminal on your machine and using the command line as needed.

### Install PostgreSQL

[PostgreSQL](https://en.wikipedia.org/wiki/PostgreSQL) is an object-relational database management system (ORDBMS) with an emphasis on extensibility and standards-compliance.

1. Download the correct PostgreSQL installer for your operating system from [EnterpriseDB](http://www.enterprisedb.com/products-services-training/pgdownload) (minimum PostgreSQL version supported is 9.4.x).
2. Run the installer. As you proceed through the installation wizard, note your choices for port number, username, and password.
3. Once PostgreSQL is installed, create the database you will use for this project.

More complete install documentation is available on the PostgreSQL [wiki](https://wiki.postgresql.org/wiki/Detailed_installation_guides).

**Note:** If you're setting up the project on Mac OSX, we recommend using [homebrew](http://brew.sh) to install PostgreSQL.

### Install Memcached
The API performs query caching via Memcached. If you want to experiment with AWS caching in your development environment, you can find instructions for installing memcached on it's [website](https://memcached.org/). You will also need to install requirements via pip from `caching_requirements.txt` in addition to the other baseline requirements.

### Install Python and Create Virtual Environment

The API's backend components currently run on Python 3.5 or higher. These instructions will walk you through the process of installing Python and creating a Python-based virtual environment to house the API. A virtual environment will isolate the project and its libraries from those running on your local system and prevent potential conflicts.

If you already have a Python development environment on your machine and a preferred way of managing it, feel free to skip to the next section. We wrote the directions below for folks who don't have a Python environment up and running yet and need the quickest way to get started.

1. Install Python 3.5 or higher:
    * Windows and OSX users can download a Python installer here: [https://www.python.org/downloads/](https://www.python.org/downloads/ "Python installer downloads")
    * Linux users can install Python using their distribution's package manager.

2. Use pip to install virtualenv (pip is Python's package manager and is automatically installed with Python):

        pip install virtualenv
3. Use pip to install virtualenvwrapper:

        pip install virtualenvwrapper

4. Tell virtualenvwrapper where on your machine to create virtual environments and add it to your profile. This is a one-time virtualenvwrapper setup step, and the process varies by operating system. [This tutorial](http://newcoder.io/begin/setup-your-machine/ "Python: setting up your computer") covers setting up virtualenvwrapper on OSX, Linux, and Windows.
   For Windows users, there may be extra steps needed.  If you run into an error on the import-module step, move the "VirtualEnvWrapper" folder from C:/Python27/Lib/site-packages/Users/*username*/Documents/WindowsPowerShell/Modules/ to C:/Users/*username*/Documents/WindowsPowerShell/Modules/.  Next, in powershell run the command "set-executionpolicy unrestricted".  Finally, in the VirtualEnvWrapper directory, open the file "VirtualEnvWrapperTabExpansion.psm1" and change "Function:TabExpansion" to "Function:TabExpansion2" in line 12.

5. Create a virtual environment for the USAspending API. In this example we've named the environment *usaspending-api*, but you can call it anything:

        mkvirtualenv usaspending-api

    **Note:** If you're running multiple versions of Python on your machine, you can make sure your environment is running the correct Python version by pointing to a specific binary

        mkvirtualenv --python=[path to installed Python executable] usaspending-api

6. You should see some output that looks similar to the example below. Essentially, this command creates and activates a new virtualenv named `usaspending-api` with its own set of Python libraries.  Anything you pip install from this point forward will be installed into the *usaspending-api* environment rather than your machine's global Python environment. Your command line prompt indicates which (if any) virtualenv is active.

 **Note:** in the command below, replace `/usr/local/bin/python3.5` with the path to your local Python executable.

        $ mkvirtualenv --python=/usr/local/bin/python3.5 usaspending-api
        Running virtualenv with interpreter /usr/local/bin/python3.5
        New python executable in usaspending-api/bin/python3.5
        Also creating executable in usaspending-api/bin/python
        Installing setuptools, pip...done.

        (usaspending-api)$

7. This new environment will be active until you run the `deactivate` command. You can re-activate the environment again at any time by typing `workon usaspending-api`.

### USAspending API Backend

Decide where on your machine you want the USAspending API code to live. From the command line, navigate there and clone the USAspending API repository from GitHub to your local environment:

        $ git clone https://github.com/fedspendingtransparency/usaspending-api.git

Navigate to the USAspending API's main folder:

        $ cd usaspending-api


Install the dependencies.  

        $ pip install -r requirements/requirements.txt

Install caching dependencies. (This is optional, and should only be done if you have installed Memcached and require AWS caching for development)

        $ pip install -r requirements/caching_requirements.txt

Next, configure your local settings. The `settings.py` file will check for an environment variable named `DATABASE_URL`. You can export this variable on the command line, or use a library like [autoenv](https://github.com/kennethreitz/autoenv) to load your environment variables when you `cd` into that directory.

Exporting your `DATABASE_URL` environment variable should look something like this:

        $ export DATABASE_URL='postgres://USER:PASSWORD@HOST:PORT/DATABASENAME'


To test your database connection, try running the migrations that come with the project to set up your tables.

        $ python manage.py migrate

You should see output that looks like this:

    (usaspending-api):~/usaspending-api$ ./manage.py migrate
    Operations to perform:
    Apply all migrations: admin, auth, awards, contenttypes, sessions
    Running migrations:
    Applying awards.0001_initial... OK

Once you've done that, you're ready to start the development server

    $ python manage.py runserver

This will run the application at `127.0.0.1:8000` by default. You can change that by passing the host and port number in to the `runserver` command.

you should see something like this:

    Performing system checks...
    System check identified no issues (0 silenced).
    April 17, 2017 - 01:46:02
    Django version 1.10.7, using settings 'usaspending_api.settings'
    Starting development server at http://127.0.0.1:8000/
    Quit the server with CONTROL-C.

Now, go to `http://localhost:8000/api/v1/awards/` to see the API! There aren't any other urls set up at the moment.

## Running the Test Suite

To run the test suite and confirm that everything is working as expected, make sure you're in the top-level project directory and that your `DATABASE_URL` environment variable is set, and
run the following from the command line:


        py.test

The tests will not run against your database in `$DATABASE_URL`, but against a
temporary database named `test_(your db name)`, which they will create.

## ETL Setup

The Django server has extract, transform, and load (ETL) processes built in as custom management commands. To use them, follow these setup steps:

  1. Configure the `DATA_BROKER_DATABASE_URL` environment variable. This is the same process as setting up the API backend. Exporting `DATA_BROKER_DATABASE_URL` should look like this:

  ```
  $ export DATA_BROKER_DATABASE_URL='postgres://USER:PASSWORD@HOST:PORT/DATABASENAME'
  ```

## Loading Data

For details on loading reference data, DATA Act Broker submissions, and current USAspending data into the API, see [loading_data.md](loading_data.md).

For details on how our data loaders modify incoming data, see [data_changes.md](data_changes.md).

## Management Commands

Below is a list of available management commands a brief description of their uses. For more help, check the command's help text.

* `load_reference_data` - Loads all reference data (used on a fresh or flushed db)
* `update_location_usage_flags` - Updates the `place_of_performance_flag` and `recipient_flag` on Location objects based upon their foreign key usages
* `load_submission` - Loads a submission from the broker for the currently set environment
* `loadtas` - Loads tas account information from a file
* `loadagencies` - Loads agency information from a file
* `loadcfda` - Loads CFDA information from a file
* `loadcontracts` - Loads USASpending contract information from a file
* `generate_test_endpoint_responses` - Generates endpoint test responses for testing

## Public Domain License

This project is in the public domain within the United States, and copyright and related rights in the work worldwide are waived through the CC0 1.0 Universal public domain dedication.

All contributions to this project will be released under the CC0 dedication. By submitting a pull request, you are agreeing to comply with this waiver of copyright interest.

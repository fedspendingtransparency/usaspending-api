# USASpending API


[![Build Status](https://travis-ci.org/fedspendingtransparency/usaspending-api.svg?branch=master)](https://travis-ci.org/fedspendingtransparency/usaspending-api)
[![Test Coverage](https://codeclimate.com/github/fedspendingtransparency/usaspending-api/badges/coverage.svg)](https://codeclimate.com/github/fedspendingtransparency/usaspending-api/coverage)[![Code Climate](https://codeclimate.com/github/fedspendingtransparency/usaspending-api/badges/gpa.svg)](https://codeclimate.com/github/fedspendingtransparency/usaspending-api)

This is the API that will drive the new USAspending.gov. It is currently under active development and not stable.

# Getting Started

This API is built on Python 3.5+, Django, and Django Rest Framework.

Assumptions:

* You're able to install software on your local machine
* You have git installed on your machine and are able to clone code repositories from GitHub. If this isn't the case, the easiest way to get started is to install [GitHub Desktop](https://desktop.github.com/ "GitHub desktop"), available for Windows or Mac.
* You're familiar with opening a terminal on your machine and using the command line as needed.

# Install PostgreSQL

[PostgreSQL](https://en.wikipedia.org/wiki/PostgreSQL) is an object-relational database management system (ORDBMS) with an emphasis on extensibility and standards-compliance.

1. Download the correct PostgreSQL installer for your operating system from [EnterpriseDB](http://www.enterprisedb.com/products-services-training/pgdownload) (minimum PostgreSQL version supported is 9.6.x).
2. Run the installer. As you proceed through the installation wizard, note your choices for port number, username, and password.
3. Once PostgreSQL is installed, create the database you will use for this project.

More complete install documentation is available on the PostgreSQL [wiki](https://wiki.postgresql.org/wiki/Detailed_installation_guides).

**Note:** If you're setting up the project on Mac OSX, we recommend using [homebrew](http://brew.sh) to install PostgreSQL.

## Install Memcached
The API performs query caching via Memcached. If you want to experiment with AWS caching in your development environment, you can find instructions for installing memcached on it's [website](https://memcached.org/). You will also need to install requirements via pip from `caching_requirements.txt` in addition to the other baseline requirements.

# Install Python 3.5

The API's backend components currently run on Python 3.5 or higher. These instructions will walk you through the process of installing Python and creating a Python-based virtual environment to house the API. A virtual environment will isolate the project and its libraries from those running on your local system and prevent potential conflicts.

If you already have a Python development environment on your machine and a preferred way of managing it, feel free to skip to the next section. We wrote the directions below for folks who don't have a Python environment up and running yet and need the quickest way to get started.
## Mac users:
### Prerequisite: Homebrew
It is recommended to have Homebrew installed. If `brew help` returns `command not found`, paste the following into the command prompt:
```
$ /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
```

### Using Homebrew to install python3.5
```
$ brew update
$ brew install python3.5
```

## Linux Users: 
```
$ sudo apt-get update
$ sudo apt-get install python3.5
```

##  Windows 
Windows users can download a Python installer here: [https://www.python.org/downloads/](https://www.python.org/downloads/ "Python installer downloads")

## Working with Python
It is important to note that Python 2.7 may also be preinstalled, to ensure the correct interpreter is being used, use the following convention:

Python 3
```
$ python3
```
Python 2
```
$ python
```

# Install direnv
Direnv may be installed from system packages, e.g.
## OSX Homebrew
```
$ brew install direnv
```

## direnv setup
### BASH
Append the following line to `~/.bashrc`
```
$ eval "$(direnv hook bash)"
```

### ZSH
Append the following line to `~/.zshrc`
```
$ eval "$(direnv hook zsh)"
```

# Setup for venv
Python 3.5 will already have the virtual environment manager venv within its standard library. To have venv automatically create virtual environments, add this snippet to your `~/.config/direnv/direnvrc`:
```
layout_python-venv() {
    local python=${1:-python3}
    [[ $# -gt 0 ]] && shift
    unset PYTHONHOME
    if [[ -n $VIRTUAL_ENV ]]; then
        VIRTUAL_ENV=$(realpath "${VIRTUAL_ENV}")
    else
        local python_version
        python_version=$("$python" -c "import platform; print(platform.python_version())")
        if [[ -z $python_version ]]; then
            log_error "Could not detect Python version"
            return 1
        fi
        VIRTUAL_ENV=$PWD/.direnv/python-venv-$python_version
    fi
    export VIRTUAL_ENV
    if [[ ! -d $VIRTUAL_ENV ]]; then
        log_status "no venv found; creating $VIRTUAL_ENV"
        "$python" -m venv "$VIRTUAL_ENV"
    fi
    PATH_add "$VIRTUAL_ENV/bin"
}
```

# USAspending API Backend

## Clone the repo and create the virtual environment
Decide where on your machine you want the USAspending API code to live. From the command line, navigate there and clone the USAspending API repository from GitHub to your local environment:
```
$ git clone https://github.com/fedspendingtransparency/usaspending-api.git
```

To allow automatic virtual environment activation, enter the following:
```
$ echo "layout python-venv python3.5" > usaspending-api/.envrc
$ cd usaspending-api
$ direnv allow
$ source activate
```
The `source activate` command will open the virtual environment for usaspending-api. Anything you pip install from this point forward will be installed into the *usaspending-api* environment rather than your machine's global Python environment. Your command line prompt indicates which (if any) virtual environment is active.

*Remember: you must use `source activate` before you use any pip install commands; You can exit the virtual environment by using `deactivate`*

## Install the dependencies in the virtual environment

Install the dependencies.
```
$ pip install -r requirements/requirements.txt
```

Install caching dependencies. (This is optional, and should only be done if you have installed Memcached and require AWS caching for development)
```
$ pip install -r requirements/caching_requirements.txt
```

Next, configure your local settings. The `settings.py` file will check for an environment variable named `DATABASE_URL`. 

## Set your virtual environment export commands
Export your `DATABASE_URL` environment variable by appending the following to your `.envrc` file
```
$ export DATABASE_URL='postgres://USER:PASSWORD@HOST:PORT/DATABASENAME'
```

To test your database connection, try running the migrations that come with the project to set up your tables.
```
$ python manage.py migrate
```
You should see output that looks like this:
```
(usaspending-api):~/usaspending-api$ ./manage.py migrate
Operations to perform:
Apply all migrations: admin, auth, awards, contenttypes, sessions
Running migrations:
Applying awards.0001_initial... OK
```
Once you've done that, you're ready to start the development server
```
$ python manage.py runserver
```
This will run the application at `127.0.0.1:8000` by default. You can change that by passing the host and port number in to the `runserver` command.

you should see something like this:
```
Performing system checks...
System check identified no issues (0 silenced).
August 22, 2017 - 15:55:33
Django version 1.11.4, using settings 'usaspending_api.settings'
Starting development server at http://127.0.0.1:8000/
Quit the server with CONTROL-C.
```
Now, go to `http://localhost:8000/api/v1/awards/` to see the API! There aren't any other urls set up at the moment.

## Running the Test Suite

To run the test suite and confirm that everything is working as expected, make sure you're in the top-level project directory and that your `DATABASE_URL` environment variable is set, and
run the following from the command line:

```
$ py.test
```
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

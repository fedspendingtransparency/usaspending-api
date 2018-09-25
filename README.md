# <p align="center"><img src="https://www.usaspending.gov/img/logo@2x.png" alt="USAspending API"></p>

[![Build Status](https://travis-ci.org/fedspendingtransparency/usaspending-api.svg?branch=master)](https://travis-ci.org/fedspendingtransparency/usaspending-api) [![Test Coverage](https://codeclimate.com/github/fedspendingtransparency/usaspending-api/badges/coverage.svg)](https://codeclimate.com/github/fedspendingtransparency/usaspending-api/coverage) [![Code Climate](https://codeclimate.com/github/fedspendingtransparency/usaspending-api/badges/gpa.svg)](https://codeclimate.com/github/fedspendingtransparency/usaspending-api)

_This API is utilized by USAspending.gov to obtain all federal spending data which is open source and provided to the public as part of the DATA Act._

![USAspending Landing Page](readme.jpg?raw=true "Readme")

## Install

Ensure the following dependencies are installed and working prior to continuing:

### Requirements
- [`python3`](https://docs.python-guide.org/starting/installation/#python-3-installation-guides)
- [`pyenv`](https://github.com/pyenv/pyenv/#installation) using Python 3.5.x
  - _NOTE: Read full install. `brew install` needs to be followed by additional steps to modify and source your `~/.bash_profile`_
- [`PostgreSQL`](https://www.postgresql.org/download/) 10.x (with a dedicated `data_store_api` database)
- [`direnv`](https://github.com/direnv/direnv#install)
  - For Mac OSX, be sure to put the hook in your `~/.bash_profile`, not `~/.bashrc`
- `Bash` or another Unix Shell equivalent
  - Bash is available on Windows as [Windows Subsystem for Linux](https://docs.microsoft.com/en-us/windows/wsl/install-win10)
- Command line package manager
  - Windows' WSL bash uses `apt-get`
  - OSX users will use [`Homebrew`](https://brew.sh/)

### Setup
Navigate to the base file directory for the USAspending repositories

    $ mkdir -p usaspending && cd usaspending
    $ git clone https://github.com/fedspendingtransparency/usaspending-api.git
    $ cd usaspending-api

Create and activate the virtual environment using `venv`, and ensure the right version of Python 3.5.x is being used (the latest RHEL package available for `python35u`, currently 3.5.5)

    $ pyenv install 3.5.5
    $ pyenv local 3.5.5
    $ python -m venv .venv/usaspending-api
    $ source .venv/usaspending-api/bin/activate


Your prompt should then look as below to show you are _in_ the virtual environment named `usaspending-api` (_to exit that virtual environment, simply type `deactivate` at the prompt_).

    (usaspending-api) $ 

[`pip`](https://pip.pypa.io/en/stable/installing/) `install` application dependencies

:bulb: _(try a different WiFi if you're current one blocks dependency downloads)_

    (usaspending-api) $ pip install -r requirements/requirements.txt

Set environment variables (fill in the connection string placeholders, e.g. `USER`, `PASSWORD`, `HOST`, `PORT`)
*note: default port for PostgreSQL is `5432`

```shell

(usaspending-api) $ echo "export DATABASE_URL='postgres://USER:PASSWORD@HOST:PORT/data_store_api'" >> .envrc

```

Test the database connection and upate the `data_store_api` schema

    (usaspending-api) $ ./manage.py migrate

Start up the site

    (usaspending-api) $ ./manage.py runserver

The application will be available at `http://localhost:8000`

## API

In your local development environment, available API endpoints may be found at `http://localhost:8000/docs/endpoints`

Deployed production API endpoints and docs are found by following links here: `https://api.usaspending.gov`

## Loading Data

For details on loading reference data, DATA Act Broker submissions, and current USAspending data into the API, see [loading_data.md](loading_data.md).

For details on how our data loaders modify incoming data, see [data_changes.md](data_changes.md).

## Public Domain License

This project is in the public domain within the United States, and copyright and related rights in the work worldwide are waived through the CC0 1.0 Universal public domain dedication.

All contributions to this project will be released under the CC0 dedication. By submitting a pull request, you are agreeing to comply with this waiver of copyright interest.

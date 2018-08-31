<p align="center"><img src="https://www.usaspending.gov/img/logo@2x.png" alt="USASpending API"></p>

---

[![Build Status](https://travis-ci.org/fedspendingtransparency/usaspending-api.svg?branch=master)](https://travis-ci.org/fedspendingtransparency/usaspending-api)[![Test Coverage](https://codeclimate.com/github/fedspendingtransparency/usaspending-api/badges/coverage.svg)](https://codeclimate.com/github/fedspendingtransparency/usaspending-api/coverage)[![Code Climate](https://codeclimate.com/github/fedspendingtransparency/usaspending-api/badges/gpa.svg)](https://codeclimate.com/github/fedspendingtransparency/usaspending-api)

> An Python-based RESTful API for U.S. federal spending data available at USASpending.gov

![USASpending Landing Page](readme.jpg?raw=true "Readme")

## Install 

Ensure the following dependencies are installed and working prior to continuing:

### Requirements
- pyenv using Python3.5
- PostgreSQL (with a dedicated usaspending-api database)
- Virtual environment manager 
- direnv

### Setup
Navigate to the base file directory for the USASpending repositories

	$ mkdir usaspending || cd $_
	$ git clone https://github.com/fedspendingtransparency/usaspending-api.git
    $ cd usaspending-api
  
Create and activate the virtual environment, ensure Python3.5.0 is being used

	$ python3 -m venv .
    $ source bin/activate
    (usaspending-api) $ pyenv local 3.5.0

   
[Pip](https://pip.pypa.io/en/stable/installing/)-install the dependencies
	
    (usaspending-api) $ pip install -r requirements/requirements.txt

Set environment variables

	(usaspending-api) $ echo "export DATABASE_URL='postgres://USER:PASSWORD@HOST:PORT/DATABASENAME'" >> .direnv
    direnv: error .envrc is blocked. Run `direnv allow` to approve its content.
    (usaspending-api) $ direnv allow
    
Test the database connection connection

	(usaspending-api) $ ./manage.py migrate

Start up the site

	(usaspending-api) $ ./manage.py runserver
    
The application will be available at `http://localhost:8000`

## API

Available API endpoints may be found at `http://localhost:8000/docs/endpoints`

## Loading Data

For details on loading reference data, DATA Act Broker submissions, and current USAspending data into the API, see [loading_data.md](loading_data.md).

For details on how our data loaders modify incoming data, see [data_changes.md](data_changes.md).

## Public Domain License

This project is in the public domain within the United States, and copyright and related rights in the work worldwide are waived through the CC0 1.0 Universal public domain dedication.

All contributions to this project will be released under the CC0 dedication. By submitting a pull request, you are agreeing to comply with this waiver of copyright interest.

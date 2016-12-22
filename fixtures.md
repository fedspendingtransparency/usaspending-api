# Loading Data

Data is loaded to the USASpending API via a series of Django management commands that are run from a terminal.

## Loading Reference Data

Use the following commands to load reference data into appropriate models (for example, the list of agencies, object classes, etc.). You'll need to load this information before running any DATA Act broker or USAspending loads (see below).

`python manage.py loaddata usaspending_api/references/fixtures/reference_fixture.json`

**Note:** If any of these load commands result in an error due to a missing field (_i.e._, `x has no field named y`), try running it again with the `--ignorenonexistent` parameter.

Additionally, to load the appropriation account data, execute

`python manage.py loadtas usaspending_api/data/tas_list.csv`

## Loading DATA Act Broker Submissions

Run the following Django management command to load a single submission from the DATA Act broker into the USASpending API:

`python manage.py load_submission [broker submission_id]`

## Loading Data From USASpending

Coming soon!

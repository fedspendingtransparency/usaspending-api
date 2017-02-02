# Loading Data

Data is loaded to the USASpending API via a series of Django management commands that are run from a terminal.

## Loading Reference Data

The USASpending API requires a set of reference data. For convenience, the management command `load_reference_data` will perform all of the reference data loading operations. It must be run in the same directory as `manage.py`. To call this command, run:

`python manage.py load_reference_data`

This command will execute the following commands:
* `python manage.py loaddata usaspending_api/references/fixtures/reference_fixture.json` - This contains the majority of referenced data and is loaded via Django fixture.
* `python manage.py loadtas usaspending_api/data/tas_list.csv` - Loads appropriation account data
* `python manage.py load_reference_csv RefCityCountyCode usaspending_api/data/ref_city_county_code.csv Latin-1` - Loads city and county reference data

`python manage.py load_reference_csv RefCityCountyCode usaspending_api/data/ref_city_county_code.csv Latin-1`

## Loading DATA Act Broker Submissions

Run the following Django management command to load a single submission from the DATA Act broker into the USASpending API:

`python manage.py load_submission [broker submission_id]`

## Loading Data From USASpending

Coming soon!

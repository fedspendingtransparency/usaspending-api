# Loading Data

Data is loaded to the USASpending API via a series of commands that are run from a terminal.

There are three types of data loads: reference data, historic USAspending data, and data broker submissions.

## Reference data:
Populates lookup tables like country codes, agency names, and other fairly static information. Currently, developers only need to need to load this information once, when first setting up their local environment.

### How to Load Reference Data
For convenience, the management command `load_reference_data` will perform all of the reference data loading operations. It must be run in the same directory as `manage.py`.

    `python manage.py load_reference_data`

## Historic USAspending
Award data from the current USAspending site. This data comes in two different formats:
* Contracts
* Assistance awards (which include grants, loans, and "other" awards)

**Note:** current USAspending loaders are insert only (not update). So flush current data before running these.

### How to Load Contract Awards
1. Go to the current [USAspending Data Download page](https://www.usaspending.gov/DownloadCenter/Pages/DataDownload.aspx).
2. Choose _Contracts_ in option _2. Select the Spending Type_
3. Select any other download parameters you'd like (agency, date range, etc.)
4. Make sure _csv_ is selected in _5. Select Type of File_
5. Click _Submit_ to download the contract award file
6. Once the file is downloaded, start the load by running:
        `python manage.py load_usaspending_contracts [path-to-contracts-file.csv]``


### How to Load Financial Assistance Awards
1. Go to the current [USAspending Data Download page](https://www.usaspending.gov/DownloadCenter/Pages/DataDownload.aspx).
2. Choose _Grants_, _Loans_, or _Other Financial Assistnace_ in option _2. Select the Spending Type_
3. Select any other download parameters you'd like (agency, date range, etc.)
4. Make sure _csv_ is selected in _5. Select Type of File_
5. Click _Submit_ to download the financial assistance award file
6. Once the file is downloaded, start the load by running:
        `python manage.py load_usaspending_assistance [path-to-contracts-file.csv]`


## DATA Act broker submissions:
Agency DATA Act submissions from the data broker.


### How to Load DATA Act Broker Submissions

Run the following Django management command to load a single submission from the DATA Act broker into the USASpending API.

**Note:** If the specified submission has already been loaded, this command will delete the existing records first. Thus, the `load_submission` command is safe to re-run.

    `python manage.py load_submission [broker submission_id]`
    
### Other data loading related commands

* `python manage.py update_location_usage_flags` - Updates all locations to have proper usage flags. This should be run after any set of submission loads to ensure the flags are properly set.

* `python manage.py load_executive_compensation --all` - Loads executive compensation data for any currently loaded submissions. For more information on other options for this command, reference the command's help text.

* `python manage.py load_subawards --all` - Loads subwards for any currently loaded submissions. For more information on other options for this command, reference the command's help text.

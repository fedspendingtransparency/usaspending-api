# Getting USAspending Data

***Note:*** *The quarterly submission windows for each fiscal year are generally open each fiscal quarter 45 days after the end of the quarter. For example, the Q2 (January 1st - March 31st) submission window is April 1st to May 16th every year. Federal holidays might slightly alter the window depending on impact.*


After an agency certifies a submission on [broker.usaspending.gov](broker.usaspending.gov), they are presented with the option to **publish** their certified submission to [www.usaspending.gov](www.usaspending.gov). After the submission is chosen to be published in the broker:

- The submission ID (unique to each submission) is queued for loading into a clone of the database used by [www.usaspending.gov](www.usaspending.gov)
- If other submissions are currently in the queue, they are loaded first
- An individual submission can take up to 24 hours to load
- The data are published on [www.usaspending.gov](www.usaspending.gov) the day following the quarterly submission deadline
- Any data certified before the close of the quarterly submission window will be available on [www.usaspending.gov](www.usapending.gov) the following day

Advanced users wanting a reference of the _full_ production database archive of the www.usaspending.gov database can access it via a public download available on https://www.usaspending.gov under the _**Download Center > Database Download**_ sub-menu.

# Loading USAspending Data (for Developers)

Data can be loaded to the USAspending API via a series of commands that are run from a terminal.

**Note:** You must set the DATABASE_URL environment variable in order to run any of these loader commands.

There are two types of data loads. Both require USAspending reference data to already be loaded (see below):
- Certified DATA Act submission data from broker.usaspending.gov
- Historic USAspending data

## Reference Data (Required):
Populates lookup tables like country codes, agency names, and other fairly static information. Developers typically only need to need to load this information once, when first setting up their environment.

To load in the reference data, from the same directory as manage.py:

    python manage.py load_reference_data


## DATA Act Certified Submission Data:

To load certified submission data from the broker, you will need a read-only (or higher) connection string to the broker PostgreSQL database. If not running locally, you will also need to ensure your IP address has been whitelisted in the appropriate AWS Security Groups. Set this environment variable before running the **load_submission** command:

    DATA_BROKER_DATABASE_URL=postgres://user:password@url:5432/data_broker

To load a submission from data broker database:

    python manage.py load_submission [submission_id]

This will load data into your USAspending database in the following order:
- File A (Appropriation data)
- File B (Program activity object class data)
- File D2 (Award financial assistance data)
- File D1 (Award procurement data)
- File C (Award financial data)
    - This is matched against award records created after loading D1 and D2
- Any subaward data


## Historic USAspending Data

**This section desperately needs to be updated. The commands listed below are most definitely obsolete.**

Award data from the [current USAspending site](https://www.usaspending.gov/) comes in two different formats:
- Contracts
- Assistance awards (which include grants, loans, and "other" awards)

**Note:** Current USAspending loaders are insert only (not update). Flush any existing data before running these loaders.

### Loading Historic Contract Awards
1. Go to the current [USAspending Data Download page](https://www.usaspending.gov/DownloadCenter/Pages/DataDownload.aspx).
2. Choose _Contracts_ in option _2. Select the Spending Type_
3. Select any other download parameters you'd like (agency, date range, etc.)
4. Make sure _csv_ is selected in _5. Select Type of File_
5. Click _Submit_ to download the contract award file
6. Once the file is downloaded, start the load by running:

    `python manage.py load_usaspending_contracts [path-to-contracts-file.csv]`

### Loading Historic Financial Assistance Awards
1. Go to the current [USAspending Data Download page](https://www.usaspending.gov/DownloadCenter/Pages/DataDownload.aspx).
2. Choose _Grants_, _Loans_, or _Other Financial Assistnace_ in option _2. Select the Spending Type_
3. Select any other download parameters you'd like (agency, date range, etc.)
4. Make sure _csv_ is selected in _5. Select Type of File_
5. Click _Submit_ to download the financial assistance award file
6. Once the file is downloaded, start the load by running:

    `python manage.py load_usaspending_assistance [path-to-contracts-file.csv]`


### Miscellaneous Data Loading Commands

- `python manage.py update_location_usage_flags` - Updates all locations to have proper usage flags. This should be run after any set of submission loads to ensure the flags are properly set.

- `psql -v ON_ERROR_STOP=1 -c '\timing' -f usaspending_api/broker/management/sql/restock_exec_comp.sql $DATABASE_URL` - Loads executive compensation data for any currently loaded submissions. 

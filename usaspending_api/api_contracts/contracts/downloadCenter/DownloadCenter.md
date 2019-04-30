FORMAT: 1A
HOST: https://api.usaspending.gov

# Download Center

These endpoints are used to power USAspending.gov's download center.

# Custom Account Page

These endpoints generate files using the filters on the custom account.

## Custom Account Data [/api/v2/download/accounts/]

This endpoint returns the generated file's metadata.

### Custom Account Data [POST]

+ Request (application/json)
    + Attributes (object)
        + `account_level`: (required, enum[string])
            The account level is used to filter for a specific type of file.
            + Members
                + treasury_account
                + federal_account
        + `file_format`: `csv` (optional, string)
            The file format that should be returned.
            + Default: `csv`
        + `filters`: (required, FilterObject)
            The filters used to filter the data

+ Response 200 (application/json)
    + Attributes
        + results (array[CustomDataResult], fixed-type)

# Data Dictionary
This endpoint powers USAspending.gov's data dictionary page.

## Data Dictionary [/api/v2/references/data_dictionary/]

This endpoint returns data corresponding to the latest data dictionary csv file.

### Data Dictonary [GET]

+ Response 200 (application/json)
    + Attributes
        + document (DataDictionary)

# Data Structures

## CustomDataResult (object)
+ `total_size`: 35.055 (required, number)
    The total size of the file being returned
+ `file_name`: `012_account_balances_20180613140845.zip` (required, string)
+ `total_rows`: 652 (required, number)
+ `total_columns`: 27 (required, number)
+ `url`: `S3/path_to/bucket/012_account_balances_20180613140845.zip` (required, string)
    Where the file lives in S3
+ message (optional, nullable)
+ `status`: `finished` (required, enum[string])
    + Members
        + ready
        + running
        + finished
        + failed
+ `seconds_elapsed`: `10.061132` (required, string)

## FilterObject (object)
+ agency: `all` (optional, string)
    The agency to filter by. This field is an internal id.
    + Default: `all`
+ `federal_account`: `212` (optional, string)
    This field is an internal id.
+ `submission_type`: (required, enum[string])
    + Members
        + account_balances
        + object_class_program_activity
        + award_financial
+ fy: `2017` (required, string)
    The fiscal year to filter by in the format `YYYY`
+ quarter: `1` (required, enum[string])
    + `1`
    + `2`
    + `3`
    + `4`

## DataDictionary (object)
+ metadata (required, DictionaryMetadata)
+ sections: (array[Section], fixed-type)
+ headers: (array[Column], fixed-type)
+ rows: (required, array, fixed-type)
    + `Lorem ipsum`, `dolor sit amet`, `consectetur adipiscing elit` (array[string], nullable)

## Section (object)
+ section: `Data Labels` (required, string)
+ colspan: 2 (required, number)
    The number of columns in the section

## Column (object)
+ raw: `award_file` (required, string)
+ display: `Award File` (required, string)

## DictionaryMetadata (object)
+ `total_rows`: 393 (required, number)
+ `total_columns`: 12 (required, number)
+ `total_size`: `119.32KB` (required, string)
+ `download_location`: `http://files-nonprod.usaspending.gov/docs/DATA+Transparency+Crosswalk.xlsx` (required, string)

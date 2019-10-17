FORMAT: 1A
HOST: https://api.usaspending.gov

# Custom Account Data [/api/v2/download/accounts/]

These endpoints are used to power USAspending.gov's download center.

## POST

Generate files and return metadata using filters on custom account 

+ Request (application/json)
    + Attributes (object)
        + `account_level`: `treasury_account`(required, enum[string])
            The account level is used to filter for a specific type of file.
            + Members
                + `treasury_account`
                + `federal_account`
        + `file_format`: `csv` (optional, string)
            The file format that should be returned.
            + Default: `csv`
        + `filters` (required, FilterObject)
            The filters used to filter the data

+ Response 200 (application/json)
    + Attributes (object)
        + `file_name` (required, string) 
            Is the name of the zipfile containing CSVs that will be generated (file_name is timestamp followed by `_transactions` or `_awards`).
        + `message` (required, string, nullable) 
            A human readable error message if the `status` is `failed`, otherwise it is `null`.
        + `seconds_elapsed` (required, string, nullable) 
            Is the time taken to genereate the file (if `status` is `finished` or `failed`), or time taken so far (if `running`).
        + `status` (required, enum[string]) 
            A string representing the current state of the CSV generation request.
            + Members
                + `failed`
                + `finished`
                + `ready`
                + `running`
        + `total_columns` (required, number, nullable) 
            Is the number of columns in the CSV, or `null` if not finished.
        + `total_rows` (required, number, nullable) 
            Is the number of rows in the CSV, or `null` if not finished.
        + `total_size` (required, number, nullable) 
            Is the estimated file size of the CSV in kilobytes, or `null` if not finished.
        + `url` (required, string) 
            The URL for the file.


# Data Structures

## FilterObject (object)
+ `agency`: `all` (optional, string)
    The agency to filter by. This field is an internal id.
    + Default: `all`
+ `federal_account`: `6282` (optional, string)
    This field is an internal id.
+ `submission_type`: `account_balances`(required, enum[string])
    + Members
        + `account_balances`
        + `object_class_program_activity`
        + `award_financial`
+ `fy`: `2017` (required, string)
    The fiscal year to filter by in the format `YYYY`
+ `quarter`: `1` (required, enum[string])
    + Members
        + `1`
        + `2`
        + `3`
        + `4`

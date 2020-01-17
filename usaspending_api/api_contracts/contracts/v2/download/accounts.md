FORMAT: 1A
HOST: https://api.usaspending.gov

# Custom Account Data [/api/v2/download/accounts/]

These endpoints are used to power USAspending.gov's download center.

## POST

Generate files and return metadata using filters on custom account

+ Request (application/json)
    + Attributes (object)
        + `account_level` (required, enum[string])
            The account level is used to filter for a specific type of file.
            + Members
                + `treasury_account`
                + `federal_account`
        + `file_format` (optional, enum[string])
            The format of the file(s) in the zip file containing the data.
            + Default: `csv`
            + Members
                + `csv`
                + `tsv`
                + `pstxt`
        + `filters` (required, FilterObject)
            The filters used to filter the data
    + Body

            {
                "account_level": "treasury_account",
                "file_format": "csv",
                "filters": {
                    "fy": "2018",
                    "quarter": "1",
                    "submission_type": "account_balances"
                }
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `status_url` (required, string)
            The endpoint used to get the status of a download.
        + `file_name` (required, string)
            Is the name of the zipfile containing CSVs that will be generated (file_name is timestamp followed by `_transactions` or `_awards`).
        + `file_url` (required, string)
            The URL for the file.
        + `download_request` (required, object)
            The JSON object used when processing the download.
    + Body

            {
                "status_url": "http://localhost:8000/api/v2/download/status?file_name=FY2018Q1_All_TAS_AccountBalances_2020-01-13_H21M00S18407575.zip",
                "file_name": "FY2018Q1_All_TAS_AccountBalances_2020-01-13_H21M00S18407575.zip",
                "file_url": "/csv_downloads/FY2018Q1_All_TAS_AccountBalances_2020-01-13_H21M00S18407575.zip",
                "download_request": {
                    "account_level": "treasury_account",
                    "agency": "all",
                    "columns": [],
                    "download_types": [
                        "account_balances"
                    ],
                    "file_format": "csv",
                    "filters": {
                        "fy": 2018,
                        "quarter": 1
                    },
                    "request_type": "account"
                }
            }
            


# Data Structures

## FilterObject (object)
+ `agency` (optional, string)
    The agency to filter by. This field is an internal id.
    + Default: `all`
+ `federal_account`(optional, string)
    This field is an internal id.
+ `submission_type` (required, enum[string])
    + Members
        + `account_balances`
        + `object_class_program_activity`
        + `award_financial`
+ `fy` (required, string)
    The fiscal year to filter by in the format `YYYY`
+ `quarter` (required, enum[string])
    + Members
        + `1`
        + `2`
        + `3`
        + `4`

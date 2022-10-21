FORMAT: 1A
HOST: https://api.usaspending.gov

# Custom Account Data [/api/v2/download/accounts/]

These endpoints are used to power USAspending.gov's download center.

## POST

Generate files and return metadata using filters on custom account

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

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
        + `filters` (required, AdvancedFilterObject)
            The filters used to filter the data
    + Body

            {
                "account_level": "treasury_account",
                "file_format": "csv",
                "filters": {
                    "fy": "2018",
                    "quarter": "1",
                    "submission_types": ["account_balances", "award_financial"],
                    "def_codes": ["L", "M", "O"]
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
                "file_name": "FY2018Q1_All_TAS_AccountData_2020-01-13_H21M00S18407575.zip",
                "file_url": "/csv_downloads/FY2018Q1_All_TAS_AccountData_2020-01-13_H21M00S18407575.zip",
                "download_request": {
                    "account_level": "treasury_account",
                    "agency": "all",
                    "columns": [],
                    "download_types": [
                        "account_balances",
                        "award_financial"
                    ],
                    "file_format": "csv",
                    "filters": {
                        "fy": 2018,
                        "quarter": 1,
                        "def_codes": ["L", "M", "O"]
                    },
                    "request_type": "account"
                }
            }



# Data Structures

## AdvancedFilterObject (object)
+ `agency` (optional, string)
    The agency on which to filter.  This field expects an internal toptier agency identifier also known as the `toptier_agency_id`.
    + Default: `all`
+ `federal_account`(optional, string)
    This field is an internal id.
+ `submission_types` (required, array)
    + (enum[string])
        + `account_balances`
        + `object_class_program_activity`
        + `award_financial`
+ `fy` (required, string)
    The fiscal year to filter by in the format `YYYY`
+ `quarter` (optional, enum[string])
    Either `quarter` or `period` is required.  Do not supply both.   Note that both monthly and quarterly submissions will be included in the resulting download file even if only `quarter` is provided.
    + Members
        + `1`
        + `2`
        + `3`
        + `4`
+ `period` (optional, enum[string])
    Either `quarter` or `period` is required.  Do not supply both.  Agencies cannot submit data for period 1 so it is disallowed as a query filter.   Note that both monthly and quarterly submissions will be included in the resulting download file even if only `period` is provided.
    + Members
        + `2`
        + `3`
        + `4`
        + `5`
        + `6`
        + `7`
        + `8`
        + `9`
        + `10`
        + `11`
        + `12`
+ `def_codes` (optional, array[string])
    The Disaster Emergency Fund Code (def_codes) filter is optional. If no def_codes are provided the request will return records associated with all def_codes. If an array of valid members is provided the request will return records associated with only the def_codes provided.
    + Members
        - `1`
        - `2`
        - `3`
        - `4`
        - `5`
        - `6`
        - `7`
        - `8`
        - `9`
        - `A`
        - `B`
        - `C`
        - `D`
        - `E`
        - `F`
        - `G`
        - `H`
        - `I`
        - `J`
        - `K`
        - `L`
        - `M`
        - `N`
        - `O`
        - `P`
        - `Q`
        - `QQQ`
        - `R`
        - `S`
        - `T`
        - `U`
        - `V`
        - `W`
        - `X`
        - `Y`
        - `Z`

FORMAT: 1A
HOST: https://api.usaspending.gov

# Disaster Funding and Spending Download [/api/v2/download/disaster/]

## POST

Creates a new download job for the requested account and award. Returns a link to a zipped file containing the generated data files.

+ Request (application/json)
    + Attributes (object)
        + `filters` (required, Filters, fixed-type)
        + `file_format` (optional, enum[string])
            The format of the file(s) in the zip file containing the data.
            + Default: `csv`
            + Members
                + `csv`
                + `tsv`
                + `pstxt`
    + Body

            {
                "filters": {
                    "def_codes": ["L", "M", "N", "O", "P"],
                    "fiscal_year": "2020"
                }
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `status_url` (required, string)
            The endpoint used to get the status of a download.
        + `file_name` (required, string)
            Is the name of the zipfile containing CSVs that will be generated (with a timestamp suffix).
        + `file_url` (required, string)
            The URL for the file.
        + `download_request` (required, object)
            The JSON object used when processing the download.
    + Body

            {
                "status_url": "http://localhost:8000/api/v2/download/status?file_name=COVID19_2020-08-17_H21M09S13110251.zip",
                "file_name": "COVID19_2020-08-17_H21M09S13110251.zip",
                "file_url": "/csv_downloads/COVID19_2020-08-17_H21M09S13110251.zip",
                "download_request": {
                    "account_level": "treasury_account",
                    "award_id": 68835500,
                    "columns": [],
                    "download_types": [
                        "contract_federal_account_funding",
                        "contract_transactions",
                        "sub_contracts"
                    ],
                    "file_format": "csv",
                    "filters": {
                        "def_codes": ["L", "M", "N", "O", "P"],
                        "fiscal_year": "2020"
                    },
                    "is_for_assistance": true,
                    "is_for_contract": true,
                    "is_for_idv": false,
                    "limit": 500000,
                    "request_type": "disaster"
                }
            }

# Data Structures

## Filters (object)
+ `def_codes` (required, array[DEFC], fixed-type)
+ `fiscal_year` (required, number)

## DEFC (enum[string])
List of Disaster Emergency Fund (DEF) Codes (DEFC) defined by legislation at the time of writing

### Members
+ `A`
+ `B`
+ `C`
+ `D`
+ `E`
+ `F`
+ `G`
+ `H`
+ `I`
+ `J`
+ `K`
+ `L`
+ `M`
+ `N`
+ `O`
+ `P`
+ `Q`
+ `R`
+ `S`
+ `T`
+ `9`
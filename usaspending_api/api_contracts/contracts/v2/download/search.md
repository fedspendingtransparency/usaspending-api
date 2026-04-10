FORMAT: 1A
HOST: https://api.usaspending.gov

# Award Download [/api/v2/download/search/]

This endpoint is used to search for awards and transactions in the same file.

## POST

This route sends a request to the backend to begin generating a zipfile of award data in CSV form for download.

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes (object)
        + `columns` (optional, array[string])
        + `filters` (required, Filters, fixed-type)
        + `file_format` (optional, enum[string])
            The format of the file(s) in the zip file containing the data.
            + Default: `csv`
            + Members
                + `csv`
                + `tsv`
                + `pstxt`
        + `limit` (optional, number)
        + `spending_level` (optional, enum[string])
            + Members
                + `subawards`
                + `transactions`
                + `awards`
            + Default: `transactions`
    + Body

            {
                "filters": {
                    "agencies": [
                        {
                            "type": "awarding",
                            "tier": "toptier",
                            "name": "Department of Agriculture"
                        }
                    ],
                    "keywords": ["Defense"]
                },
                "columns": [
                    "assistance_award_unique_key",
                    "assistance_transaction_unique_key",
                    "award_id_fain",
                    "award_id_uri",
                    "modification_number",
                    "sai_number",
                    "total_funding_amount"
                ]
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `status_url` (required, string)
            The endpoint used to get the status of a download.
        + `file_name` (required, string)
            Is the name of the zipfile containing CSVs that will be generated (`PrimeAwardsTransactionsAndSubawards` followed by a timestamp).
        + `file_url` (required, string)
            The URL for the file.
        + `download_request` (required, object)
            The JSON object used when processing the download.
    + Body

            {
                "status_url": "http://localhost:8000/api/v2/download/status?file_name=PrimeAwardsTransactionsAndSubawards_2020-01-13_H21M05S48397603.zip",
                "file_name": "PrimeAwardsTransactionsAndSubawards_2020-01-13_H21M05S48397603.zip",
                "file_url": "/csv_downloads/PrimeAwardsTransactionsAndSubawards_2020-01-13_H21M05S48397603.zip",
                "download_request": {
                    "columns": [
                      
                    ],
                    "download_types": [
                        
                    ],
                    "file_format": "csv",
                    "filters": {

                    },
                    "limit": 0,
                    "request_type": "search"
                }
            }

# Data Structures

## Filter Objects

### Filters (object)


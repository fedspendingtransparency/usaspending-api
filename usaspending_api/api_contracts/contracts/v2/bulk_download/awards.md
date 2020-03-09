FORMAT: 1A
HOST: https://api.usaspending.gov

# Bulk Award Download [/api/v2/bulk_download/awards/]

This endpoint is used by the Custom Award Data Download page.

## POST

This route sends a request to the backend to begin generating a zipfile of award data in CSV form for download.

+ Request (application/json)
    + Attributes (object)
        + `filters` (required, Filters, fixed-type)
        + `columns` (optional, array[string])
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
                    "agency": 50,
                    "prime_award_types": ["contracts", "grants"],
                    "sub_award_types": ["procurement"],
                    "date_range": {
                        "start_date": "2019-01-01",
                        "end_date": "2019-12-31"
                    },
                    "date_type": "action_date"
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
                "status_url": "http://localhost:8000/api/v2/download/status?file_name=534_PrimeTransactionsAndSubawards_2020-01-13_H21M04S54995657.zip",
                "file_name": "534_PrimeTransactionsAndSubawards_2020-01-13_H21M04S54995657.zip",
                "file_url": "/csv_downloads/534_PrimeTransactionsAndSubawards_2020-01-13_H21M04S54995657.zip",
                "download_request": {
                    "agency": 50,
                    "columns": [],
                    "download_types": [
                        "prime_awards",
                        "sub_awards"
                    ],
                    "file_format": "csv",
                    "filters": {
                        "agencies": [
                            {
                                "name": "Office of the Federal Coordinator for Alaska Natural Gas Transportation Projects",
                                "tier": "toptier",
                                "type": "awarding"
                            }
                        ],
                        "award_type_codes": [
                            "02",
                            "03",
                            "04",
                            "05",
                            "A",
                            "B",
                            "C",
                            "D"
                        ],
                        "time_period": [
                            {
                                "date_type": "action_date",
                                "end_date": "2019-12-31",
                                "start_date": "2019-01-01"
                            }
                        ]
                    },
                    "request_type": "award"
                }
            }

# Data Structures

## Filter Objects

### Filters (object)
+ `agency` (required, string)
    Agency database id to include, 'all' is also an option to include all agencies
+ `prime_award_types` (optional, array[enum[string]])
    + Members
        + `contracts`
        + `direct_payments`
        + `grants`
        + `idvs`
        + `loans`
        + `other_financial_assistance`
+ `date_range` (required, TimePeriod, fixed-type)
    Object with start and end dates
+ `date_type` (required, enum[string])
    + Members
        + `action_date`
        + `last_modified_date`
+ `keyword` (optional, string)
+ `place_of_performance_locations` (optional, array[Location], fixed-type)
+ `recipient_locations` (optional, array[Location], fixed-type)
+ `sub_agency` (optional, string)
    Sub-agency name to include (based on the agency filter)
+ `sub_award_types` (optional, array[enum[string]])
    + Members
        + `grant`
        + `procurement`

### TimePeriod (object)
+ `start_date` (required, string)
+ `end_date` (required, string)

### Location (object)
+ `country`(required, string)
+ `state` (optional, string)
+ `county` (optional, string)
+ `city` (optional, string)
+ `district` (optional, string)
+ `zip` (optional, string)

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
                    "prime_award_types": [
                        "A",
                        "B",
                        "C",
                        "D",
                        "IDV_A",
                        "IDV_B",
                        "IDV_B_A",
                        "IDV_B_B",
                        "IDV_B_C",
                        "IDV_C",
                        "IDV_D",
                        "IDV_E",
                        "02",
                        "03",
                        "04",
                        "05",
                        "10",
                        "06",
                        "07",
                        "08",
                        "09",
                        "11",
                        "-1"
                    ],
                    "date_type": "action_date",
                    "date_range": {
                        "start_date": "2019-10-01",
                        "end_date": "2020-09-30"
                    },
                    "agencies": [
                        {
                            "type": "funding",
                            "tier": "subtier",
                            "name": "Animal and Plant Health Inspection Service",
                            "toptier_name": "Department of Agriculture"
                        }
                    ]
                },
                "file_format": "csv"
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
                "status_url": "https://api.usaspending.gov/api/v2/download/status?file_name=All_PrimeTransactions_2020-09-16_H15M20S52934397.zip",
                "file_name": "All_PrimeTransactions_2020-09-16_H15M20S52934397.zip",
                "file_url": "https://files.usaspending.gov/generated_downloads/dev/All_PrimeTransactions_2020-09-16_H15M20S52934397.zip",
                "download_request": {
                    "columns": [],
                    "download_types": [
                        "prime_awards"
                    ],
                    "file_format": "csv",
                    "filters": {
                        "agencies": [
                            {
                                "name": "Animal and Plant Health Inspection Service",
                                "tier": "subtier",
                                "toptier_name": "Department of Agriculture",
                                "type": "funding"
                            }
                        ],
                        "prime_and_sub_award_types": {
                            "prime_awards": [
                                "02",
                                "03",
                                "04",
                                "05",
                                "06",
                                "07",
                                "08",
                                "09",
                                "10",
                                "11",
                                "A",
                                "B",
                                "C",
                                "D",
                                "IDV_A",
                                "IDV_B",
                                "IDV_B_A",
                                "IDV_B_B",
                                "IDV_B_C",
                                "IDV_C",
                                "IDV_D",
                                "IDV_E",
                                "-1"
                            ],
                            "sub_awards": []
                        },
                        "time_period": [
                            {
                                "date_type": "action_date",
                                "end_date": "2020-09-30",
                                "start_date": "2019-10-01"
                            }
                        ]
                    },
                    "request_type": "award"
                }
            }

# Data Structures

## Filter Objects

### Filters (object)
+ `agencies` (required, array[Agency], fixed-type)
+ `prime_award_types` (optional, array[enum[string]])
    + Members
        + `IDV_A`
        + `IDV_B`
        + `IDV_B_A`
        + `IDV_B_B`
        + `IDV_B_C`
        + `IDV_C`
        + `IDV_D`
        + `IDV_E`
        + `02`
        + `03`
        + `04`
        + `05`
        + `06`
        + `07`
        + `08`
        + `09`
        + `10`
        + `11`
        + `A`
        + `B`
        + `C`
        + `D`
        + `-1`
+ `date_range` (required, TimePeriod, fixed-type)
    Object with start and end dates
+ `date_type` (required, enum[string])
    + Members
        + `action_date`
        + `last_modified_date`
+ `keyword` (optional, string)
+ `place_of_performance_locations` (optional, array[Location], fixed-type)
+ `place_of_performance_scope` (optional, enum[string])
    + Members
        + `domestic`
        + `foreign`
+ `recipient_locations` (optional, array[Location], fixed-type)
+ `recipient_scope` (optional, enum[string])
    + Members
        + `domestic`
        + `foreign`
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
+ `district_original` (optional, string)
    A 2 character code indicating the congressional district
    * When provided, a `state` must always be provided as well.
    * When provided, a `county` *must never* be provided.
    * When provided, `country` must always be "USA".
    * When provided, `district_current` *must never* be provided.
+ `district_current` (optional, string)
    A 2 character code indicating the current congressional district
    * When provided, a `state` must always be provided as well.
    * When provided, a `county` *must never* be provided.
    * When provided, `country` must always be "USA".
    * When provided, `district_original` *must never* be provided.
+ `zip` (optional, string)

### Agency (object)
+ `name` (required, string)
+ `tier` (required, enum[string])
    + Members
        + `toptier`
        + `subtier`
+ `type` (required, enum[string])
    + Members
        + `funding`
        + `awarding`
+ `toptier_name` (optional, string)
    Provided when the `name` belongs to a subtier agency

FORMAT: 1A
HOST: https://api.usaspending.gov

# Award Download [/api/v2/download/transactions/]

This endpoint is used by the Advanced Search download page.

## POST

This route sends a request to the backend to begin generating a zipfile of transaction data in CSV form for download.

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
    + Body

            {
                "filters": {
                    "keywords": ["Defense"]
                },
                "columns": [
                    "assistance_transaction_unique_key",
                    "award_id_fain",
                    "modification_number",
                    "award_id_uri",
                    "sai_number"
                ]
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
                "status_url": "http://localhost:8000/api/v2/download/status?file_name=PrimeTransactionsAndSubawards_2020-01-13_H21M10S12464980.zip",
                "file_name": "PrimeTransactionsAndSubawards_2020-01-13_H21M10S12464980.zip",
                "file_url": "/csv_downloads/PrimeTransactionsAndSubawards_2020-01-13_H21M10S12464980.zip",
                "download_request": {
                    "agency": "all",
                    "columns": [
                        "assistance_transaction_unique_key",
                        "award_id_fain",
                        "award_id_uri",
                        "modification_number",
                        "sai_number"
                    ],
                    "download_types": [
                        "sub_awards",
                        "transactions"
                    ],
                    "file_format": "csv",
                    "filters": {
                        "award_type_codes": [
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
                        "keywords": [
                            "Defense"
                        ],
                        "time_period": [
                            {
                                "date_type": "action_date",
                                "end_date": "2020-01-13",
                                "start_date": "1000-01-01"
                            }
                        ]
                    },
                    "limit": 500000,
                    "request_type": "award"
                }
            }

# Data Structures

## Filter Objects

### Filters (object)
+ `award_amounts` (optional, array[AwardAmount], fixed-type)
+ `award_ids` (optional, array[string])
    Award IDs surrounded by double quotes (e.g. `"SPE30018FLJFN"`) will perform exact matches as opposed to the default, fuzzier full text matches.  Useful for Award IDs that contain spaces or other word delimiters.
+ `award_type_codes` (optional, array[string])
+ `agencies` (optional, array[Agency], fixed-type)
+ `contract_pricing_type_codes` (optional, array[string])
+ `extent_competed_type_codes` (optional, array[string])
+ `federal_account_ids` (optional, array[string])
+ `keywords` (optional, array[string])
+ `legal_entities` (optional, array[string])
+ `naics_codes` (optional, NAICSCodeObject)
+ `object_class_ids` (optional, array[string])
+ `place_of_performance_locations` (optional, array[Location], fixed-type)
+ `place_of_performance_scope` (optional, string)
+ `program_activity_ids` (optional, array[string])
+ `program_numbers` (optional, array[string])
+ `psc_codes` (optional, enum[PSCCodeObject, array[string]])
    Supports new PSCCodeObject or legacy array of codes.
+ `recipient_locations` (optional, array[Location], fixed-type)
+ `recipient_search_text` (optional, string)
+ `recipient_scope` (optional, string)
+ `recipient_type_names` (optional, array[string])
+ `set_aside_type_codes` (optional, array[string])
+ `time_period` (optional, array[TimePeriod], fixed-type)
+ `tas_codes` (optional, array[TASCodeObject], fixed-type)
+ `treasury_account_components` (optional, array[TreasuryAccountComponentsObject], fixed-type)

### AwardAmount (object)
+ `lower_bound` (optional, number)
+ `upper_bound` (optional, number)

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


### TimePeriod (object)
This TimePeriodObject can fall into different categories based on the request.
+ subaward files in download

    See the Subaward Search category defined in [SubawardSearchTimePeriodObject](../../../search_filters.md#subaward-search-time-period-object)

+ transaction files in download

    See the Transaction Search category defined in [TransactionSearchTimePeriodObject](../../../search_filters.md#transaction-search-time-period-object)

### Location (object)
+ `country`(required, string)
+ `state` (optional, string)
+ `county` (optional, string)
+ `city` (optional, string)
+ `district` (optional, string)
+ `zip` (optional, string)

### NAICSCodeObject (object)
+ `require`: [`33`] (optional, array[string], fixed-type)
+ `exclude`: [`3333`] (optional, array[string], fixed-type)

### PSCCodeObject (object)
+ `require`: [[`Service`, `B`, `B5`]] (optional, array[array[string]], fixed-type)
+ `exclude`: [[`Service`, `B`, `B5`, `B502`]] (optional, array[array[string]], fixed-type)

### TASCodeObject (object)
+ `require`: [[`091`]] (optional, array[array[string]], fixed-type)
+ `exclude`: [[`091`, `091-0800`]] (optional, array[array[string]], fixed-type)

### TreasuryAccountComponentsObject (object)
+ `ata` (optional, string, nullable)
    Allocation Transfer Agency Identifier - three characters
+ `aid` (required, string)
    Agency Identifier - three characters
+ `bpoa` (optional, string, nullable)
    Beginning Period of Availability - four digits
+ `epoa` (optional, string, nullable)
    Ending Period of Availability - four digits
+ `a` (optional, string, nullable)
    Availability Type Code - X or null
+ `main` (required, string)
    Main Account Code - four digits
+ `sub` (optional, string, nullable)
    Sub-Account Code - three digits

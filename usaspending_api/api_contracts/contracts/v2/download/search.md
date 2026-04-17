FORMAT: 1A
HOST: https://api.usaspending.gov

# Search Download [/api/v2/download/search/]

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
                ],
                "spending_level": ["awards","transactions","subawards"]
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
                      "assistance_award_unique_key",
                        "assistance_transaction_unique_key",
                        "award_id_fain",
                        "award_id_uri",
                        "modification_number",
                        "sai_number",
                        "total_funding_amount"
                    ],
                    "download_types": [
                        "elasticsearch_awards",
                        "elasticsearch_transactions",
                        "elasticsearch_sub_awards"
                    ],
                    "file_format": "csv",
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
                        "limit": 0,
                        "request_type": "search"
                }
            }

# Data Structures

## Filter Objects

A more detailed explanation of the available filters can be found [here.](../../../search_filters.md)

### Filters (object)
+ `agencies` (optional, array[Agency], fixed-type)
+ `award_amounts` (optional, array[AwardAmount], fixed-type)
+ `award_ids` (optional, array[string])
    Award IDs surrounded by double quotes (e.g. `"SPE30018FLJFN"`) will perform exact matches as opposed to the default, fuzzier full text matches.  Useful for Award IDs that contain spaces or other word delimiters.
+ `award_type_codes` (optional, array[string])
+ `contract_pricing_type_codes` (optional, array[string])
+ `def_codes` (optional, array[DEFC], fixed-type)
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
+ `recipient_scope` (optional, string)
+ `recipient_search_text` (optional, string)
+ `set_aside_type_codes` (optional, array[string])
+ `recipient_type_names` (optional, array[string])
+ `tas_codes` (optional, array[TASCodeObject], fixed-type)
+ `time_period` (optional, array[TimePeriod], fixed-type)
+ `transaction_keyword_search` (optional, string)
    Filter awards by keywords in the award's transactions.
+ `treasury_account_components` (optional, array[TreasuryAccountComponentsObject], fixed-type)

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

### AwardAmount (object)
+ `lower_bound` (optional, number)
+ `upper_bound` (optional, number)

### DEFC (enum[string])
List of Disaster Emergency Fund (DEF) Codes (DEFC) defined by legislation at the time of writing.
A list of current DEFC can be found [here.](https://files.usaspending.gov/reference_data/def_codes.csv)


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

### NAICSCodeObject (object)
+ `require`: [`33`] (optional, array[string], fixed-type)
+ `exclude`: [`3333`] (optional, array[string], fixed-type)

### PSCCodeObject (object)
+ `require`: [[`Service`, `B`, `B5`]] (optional, array[array[string]], fixed-type)
+ `exclude`: [[`Service`, `B`, `B5`, `B502`]] (optional, array[array[string]], fixed-type)

### TASCodeObject (object)
+ `require`: [[`091`]] (optional, array[array[string]], fixed-type)
+ `exclude`: [[`091`, `091-0800`]] (optional, array[array[string]], fixed-type)

### TimePeriod (object)
While accepting the same format, time period filters are interpreted slightly differently between awards, transactions, and subawards.
- [Award Time Period](../../../search_filters.md#award-search-time-period-Object)
- [Transaction Time Period](../../../search_filters.md#transaction-search-time-period-Object)
- [Subawards Time Period](../../../search_filters.md#subaward-search-time-period-Object)


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

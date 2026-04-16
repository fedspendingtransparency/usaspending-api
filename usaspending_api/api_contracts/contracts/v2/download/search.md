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

### TimePeriod (object)
While accepting the same format, time period filters are interpreted slightly differently between awards, transactions, and subawards.  Relevant information is attached below.
+ [Award Time Period](../../../search_filters.md#award-search-time-period-Object)
+ [Transaction Time Period](../../../search_filters.md#transaction-search-time-period-Object)
+ [Subawards Time Period](../../../search_filters.md#subaward-search-time-period-Object)

FORMAT: 1A
HOST: https://api.usaspending.gov

# Award Download [/api/v2/download/transactions/]

This endpoint is used by the Advanced Search download page.

## POST

This route sends a request to the backend to begin generating a zipfile of transaction data in CSV form for download.
        
+ Request (application/json)
    + Attributes (object)
        + `columns` (optional, array[string])
        + `filters` (required, Filters, fixed-type)
        + `file_format` (optional, string)
            + Default: `csv`
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
                    "sai_number",
                    "total_funding_amount"
                ]
            }

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

## Filter Objects

### Filters (object)
+ `award_amounts` (optional, array[AwardAmount], fixed-type)
+ `award_ids` (optional, array[string])
+ `award_type_codes` (optional, array[string])
+ `agencies` (optional, array[Agency], fixed-type)
+ `contract_pricing_type_codes` (optional, array[string])
+ `elasticsearch_keyword` (optional, string)
+ `extent_competed_type_codes` (optional, array[string])
+ `federal_account_ids` (optional, array[string])
+ `keywords` (optional, array[string])
+ `legal_entities` (optional, array[string])
+ `naics_codes` (optional, array[string])
+ `object_class_ids` (optional, array[string])
+ `place_of_performance_locations` (optional, array[Location], fixed-type)
+ `place_of_performance_scope` (optional, string)
+ `program_activity_ids` (optional, array[string])
+ `program_numbers` (optional, array[string])
+ `psc_codes` (optional, array[string])
+ `recipient_locations` (optional, array[Location], fixed-type)
+ `recipient_search_text` (optional, string)
+ `recipient_scope` (optional, string)
+ `recipient_type_names` (optional, array[string])
+ `set_aside_type_codes` (optional, array[string])
+ `time_period` (optional, array[TimePeriod], fixed-type)

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

### TimePeriod (object)
+ `start_date` (required, string)
+ `end_date` (required, string)
+ `date_type` (optional, enum[string])

### Location (object)
+ `country`(required, string)
+ `state` (optional, string)
+ `county` (optional, string)
+ `city` (optional, string)
+ `district` (optional, string)
+ `zip` (optional, string)

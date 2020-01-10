FORMAT: 1A
HOST: https://api.usaspending.gov

# Award Download [/api/v2/download/awards/]

This endpoint is used by the Advanced Search download page.

## POST

This route sends a request to the backend to begin generating a zipfile of award data in CSV form for download.

+ Request (application/json)
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
                    "assistance_award_unique_key",
                    "award_id_fain",
                    "award_id_uri",
                    "sai_number",
                    "total_funding_amount"
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

# Data Structures

## Filter Objects

### Filters (object)
+ `award_amounts` (optional, array[AwardAmount], fixed-type)
+ `award_ids` (optional, array[string])
    Award IDs surrounded by double quotes (e.g. `"SPE30018FLJFN"`) will perform exact matches as opposed to the default, fuzzier full text matches.  Useful for Award IDs that contain spaces or other word delimiters.
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

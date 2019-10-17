FORMAT: 1A
HOST: https://api.usaspending.gov

# Bulk Award Download [/api/v2/bulk_download/awards/]

This endpoint is used by the Custom Award Data Download page.

## POST

This route sends a request to the backend to begin generating a zipfile of award data in CSV form for download.
        
+ Request (application/json)
    + Attributes (object)
        + `award_levels` (required, array[enum[string]])
            + Members
                + `prime_awards`
                + `sub_awards`
        + `filters` (required, Filters, fixed-type)
        + `columns` (optional, array[string])
        + `file_format` (optional, string)
            + Default: `csv`
    + Body

            {
                "filters": {
                    "agency": 50,
                    "award_types": ["contracts", "grants"],
                    "date_range": {
                        "start_date": "2019-01-01",
                        "end_date": "2019-12-31"
                    },
                    "date_type": "action_date"
                },
                "award_levels": ["prime_awards", "sub_awards"]
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
+ `agency` (required, string) 
    Agency database id to include, 'all' is also an option to include all agencies
+ `award_types` (required, array[enum[string]]) 
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
+ `place_of_performance_locations` (optional, array[Location])
+ `recipient_locations` (optional, array[Location], fixed-type)
+ `sub_agency` (optional, string) 
    Sub-agency name to include (based on the agency filter)

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

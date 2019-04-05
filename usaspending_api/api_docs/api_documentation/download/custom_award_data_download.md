## [Custom Award Data Download](#Custom_Award_Data_Download)
**Route:** `/api/v2/bulk_download/awards/`

**Method:** `POST`

This route sends a request to the backend to begin generating a zipfile of award data in CSV form for download.

### Request example

```
{
    "award_levels": ["prime_awards"],
    "filters": {
        "award_types": ["contracts", "loans"],
        "agency": "1",
        "sub_agency": "Department of the Air Force",
        "date_type": "action_date",
        "date_range": {
            "start_date": "2016-10-01",
            "end_date": "2017-09-30"
        },
        "recipient_locations": [
            {
            "country": "USA",
            "state": "VA",
            "county": "059"
            }
        ],
        "place_of_performance_locations": [
            {
            "country": "USA",
            "state": "VA",
            "county": "059"
            }
       ]
    },
    "columns": [],
    "file_format": "csv"
}
```

### Request example with keyword

```
{
    "award_levels": ["prime_awards"],
    "filters": {
        "keyword": "test"
    }
}
```

### Request Parameters Description
* `award_levels` - *required* - list of award levels ["prime_awards", "sub_awards"]
* `filters` - *required* - a JSON filter object with the following fields
    * `keyword` - *optional* - string to search the elastic search on (overrides all other filters)
    * `award_types` - *optional* - list of award types ("contracts", "grants", "direct_payments", "idvs", "loans", and "other_financial_assistance") (default is all)
    * `agency` - *optional* - agency database id to include, 'all' is also an option to include all agencies
    * `sub_agency` - *optional* - sub-agency name to include (based on the agency filter)
    * `date_type` - *optional* - choice between two types: "action_date", "last_modified_date"
    * `date_range` - *optional* - object with start and end dates
    * `recipient_locations` - *optional* - see the same filter from the [Universal Filters](https://github.com/fedspendingtransparency/usaspending-api/wiki/Search-Filters-v2-Documentation#award-id)
    * `place_of_performance_locations` - *optional* - see the same filter from the [Universal Filters](https://github.com/fedspendingtransparency/usaspending-api/wiki/Search-Filters-v2-Documentation#award-id)
* `columns` - *optional* - columns to select
* `file_format` - *optional* - must be `csv`

### Response (JSON)

```
{
   "status":"ready",
   "total_rows":null,
   "file_name":"5757660_968336105_awards.zip",
   "total_size":null,
   "total_columns":null,
   "message":null,
   "url":"https://s3-us-gov-west-1.amazonaws.com:443/usaspending-bulk-download-staging/19832098_163223743_prime_transactions.zip",
   "seconds_elapsed":null
}
```

* `total_size` is the estimated file size of the file in kilobytes, or `null` if not finished
* `total_columns` is the number of columns in the file, or `null` if not finished
* `total_rows` is the number of rows in the file, or `null` if not finished
* `file_name` is the name of the zipfile containing files that will be generated
    * File name is a timestamp followed by `_awards`
* `status` is a string representing the current state of the file generation request. Possible values are:
    * `ready` - job is ready to be run
    * `running` - job is currently in progress
    * `finished` - job is complete
    * `failed` - job failed to complete

  For this endpoint, `status` will always be `ready`, since the response is returned before generation begins
* `url` - the URL for the file
* `message` - a human readable error message if the `status` is `failed`, otherwise `null`
* `seconds_elapsed` - is time spent generating the files; always null for this endpoint, since the response is returned before generation begins


## [Awards Download](#usaspending-api-documentation)
**Route:** `/v2/download/awards/`

**Method:** `POST`

This route sends a request to the backend to begin generating a zipfile of award data in CSV form for download.

### Request example

```
{
    "filters": {},
    "columns": [
        "award_type",
        "awarding_agency_code"
    ]
}
```

### Request Parameters Description

* `filters` - *required* - a standard Search v2 JSON filter object
* `columns` - *required* - an array of column names (using the `value` string from the `/v2/download/columns` endpoint)
    * API should generate a CSV with columns in the same order as the array
    * An empty columns array returns all available columns

### Response (JSON)

```
{
   "status":"ready",
   "total_rows":null,
   "file_name":"5757660_968336105_awards.zip",
   "total_size":null,
   "total_columns":null,
   "message":null,
   "url":"/Volumes/exlinux/Users/catherine/werk/dataact/usaspending-api/downloads/5757660_968336105_awards.zip",
   "seconds_elapsed":null
}
```

* `total_size` is the estimated file size of the CSV in kilobytes, or `null` if not finished
* `total_columns` is the number of columns in the CSV, or `null` if not finished
* `total_rows` is the number of rows in the CSV, or `null` if not finished
* `file_name` is the name of the zipfile containing CSVs that will be generated
    * File name is a timestamp followed by `_awards`
* `status` is a string representing the current state of the CSV generation request. Possible values are:
    * `ready` - job is ready to be run
    * `running` - job is currently in progress
    * `finished` - job is complete
    * `failed` - job failed to complete

  For this endpoint, `status` will always be `ready`, since the response is returned before generation begins
* `url` - the URL for the file
* `message` - a human readable error message if the `status` is `failed`, otherwise `null`
* `seconds_elapsed` is time spent generating the CSVs; always `null` for this endpoint, since the response is returned before generation begins


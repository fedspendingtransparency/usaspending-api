## [IDV Download](#IDV_Download)
**Route:** `/api/v2/download/idv/`

**Method:** `POST`

This route sends a request to the backend to begin generating a zipfile of IDV data in CSV form for download.

### Request example

```
{
    "award_id": "CONT_AW_1540_-NONE-_DJB30605051_-NONE-"
}
```

### Request Parameters Description

* award_id: (required) ID of IDV to retrieve. This can either be `generated_unique_award_id` or `id` from awards table.

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

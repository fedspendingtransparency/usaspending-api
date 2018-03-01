## [Download Status](#usaspending-api-documentation)
**Route:** `/api/v2/download/status/`

**Method:** `GET`

This route gets the current status of a download job that _that has been requested_ with the `v2/download/awards/` or `v2/download/transaction/` endpoint _that same day_.

### Query Parameters Description

* `file_name` - *required* - Taken from the `file_name` field of the `v2/download/awards/` or `v2/download/transactions` response.

### Response (JSON)


```
{
   "status":"finished",
   "total_rows":3317,
   "file_name":"5757388_622958899_transactions.zip",
   "total_size":3334.475,
   "total_columns":214,
   "message":null,
   "url":"/Volumes/exlinux/Users/catherine/werk/dataact/usaspending-api/downloads/5757388_622958899_transactions.zip",
   "seconds_elapsed":"0.438393"
}
```

* `total_size` is the estimated file size of the CSV in kilobytes, or `null` if not finished
* `total_columns` is the number of columns in the CSV, or `null` if not finished
* `total_rows` is the number of rows in the CSV, or `null` if not finished
* `file_name` is the name of the zipfile containing CSVs that will be generated
    * File name is timestamp followed by `_transactions` or `_awards`
* `status` is a string representing the current state of the CSV generation request. Possible values are:
    * `ready` - job is ready to be run
    * `running` - job is currently in progress
    * `finished` - job is complete
    * `failed` - job failed to complete
* `url` - the URL for the file
* `message` - a human readable error message if the `status` is `failed`, otherwise it is `null`
* `seconds_elapsed` is the time taken to genereate the file (if `status` is `finished` or `failed`), or time taken so far (if `running`)

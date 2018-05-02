## Custom Account Data Download (Beta)
**Route:** `/api/v2/bulk_download/accounts/`

**Method:** `POST`

This route sends a request to the backend to begin generating a zipfile of accounts data in CSV form for download.

### Request example

```
{
    "account_level": "treasury_account",
    "filters": {
        "agency": "3",
        "submission_type": "object_class_program_activity",
        "fy": "2018",
        "quarter": "2"
    },
    "file_format": "csv"
}
```

### Request Parameters Description
* `account_level` - *required* - the account level: `tresury_account` or `federal_account` (must be `treasury_account` for Beta)
* `filters` - *required* - a JSON filter object with the following fields
        * `agency` - *optional* - agency database id to include, `all` is also an option to include all agencies
        * `federal_account` - *optional* - federal account id to include (based on the agency filter), out of scope for Beta
        * `submission_type` - *required* - the file type requested: `account_balances` (File A) or `program_activity_object_class` (File B)
        * `fy` - *required* - fiscal year
        * `quarter` - *required*
* `file_format` - *optional* - must be `csv` for Beta

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
        * File name is a timestamp followed by `_accounts`
* `status` is a string representing the current state of the file generation request. Possible values are:
        * `ready` - job is ready to be run
        * `running` - job is currently in progress
        * `finished` - job is complete
        * `failed` - job failed to complete

    For this endpoint, `status` will always be `ready`, since the response is returned before generation begins
* `url` - the URL for the file
* `message` - a human readable error message if the `status` is `failed`, otherwise `null`
* `seconds_elapsed` - is time spent generating the files; always null for this endpoint, since the response is returned before generation begins


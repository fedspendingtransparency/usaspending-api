## Custom Account Data Download (Beta)
**Route:** `/api/v2/download/accounts/`

**Method:** `POST`

This route sends a request to the backend to begin generating a zipfile of accounts data in CSV form for download.

### Request example

```
{
    "account_level": "treasury_account",
    "filters": {
        "agency": "22",
        "federal_account": "15",
        "budget_function": "800",
        "budget_subfunction": "801",
        "submission_type": "award_financial",
        "fy": "2018",
        "quarter": "2"
    },
    "file_format": "csv"
}
```

### Request Parameters Description
* `account_level` - *required* - the account level: `treasury_account` or `federal_account`
* `filters` - *required* - a JSON filter object with the following fields
    * `agency` - *optional* - agency database id, `all` is also an option to include all agencies
    * `federal_account` - *optional* - federal account id, `all` is also an option to include all federal_accounts
    * `budget_function` - *optional* - budget function code, `all` is also an option to include all budget functions
    * `budget_subfunction` - *optional* - budget subfunction code, `all` is also an option to include all budget subfunctions
    * `submission_type` - *required* - the file type requested. Possible values are:
        * `account_balances` - Account Balances
        * `program_activity_object_class` - Account Breakdown by Program Activity & Object Class
        * `award_financial` - Account Breakdown by Award
    * `fy` - *required* - fiscal year
    * `quarter` - *required* - fiscal quarter
* `file_format` - *optional* - must be `csv`
* `columns` - *optional* - columns to select

### Response (JSON)

```
{
    "status":"ready",
    "total_rows":null,
    "file_name":"020_treasury_account_account_breakdown_by_award.zip",
    "total_size":null,
    "total_columns":null,
    "message":null,
    "url":"https://s3-us-gov-west-1.amazonaws.com:443/usaspending-bulk-download-staging/020_treasury_account_account_breakdown_by_award.zip",
    "seconds_elapsed":null
}
```

* `total_size` - estimated file size of the file in kilobytes, or `null` if not finished
* `total_columns` - number of columns in the file, or `null` if not finished
* `total_rows` - number of rows in the file, or `null` if not finished
* `file_name` - name of the zipfile containing files that will be generated
    * File name is a concatenation of the `agency_code`, `account_level`, and `file_type`, followed by the `file_format`
* `status` - a string representing the current state of the file generation request. Possible values are:
    * `ready` - job is ready to be run
    * `running` - job is currently in progress
    * `finished` - job is complete
    * `failed` - job failed to complete

    For this endpoint, `status` will always be `ready`, since the response is returned before generation begins
* `url` - the URL for the file
* `message` - a human readable error message if the `status` is `failed`, otherwise `null`
* `seconds_elapsed` - time spent generating the files; always null for this endpoint, since the response is returned before generation begins


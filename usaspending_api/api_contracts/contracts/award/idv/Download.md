FORMAT: 1A
HOST: https://api.usaspending.gov

# IDV Download [/api/v2/download/idv/]

This end point returns a zipped file containing IDV data.

## POST

+ Request (application/json)
    + Attributes
        + `award_id`: `CONT_AW_9700_-NONE-_N0018918D0057_-NONE-` (required, string)
+ Response 200 (application/json)
    + Attributes
        + results (IDVDownloadResponse)

# Data Structures

## IDVDownloadResponse (object)
+ total_size: 35.055 (nullable, number)
    The total size of the file being returned
+ file_name: `012_account_balances_20180613140845.zip` (required, string)
+ total_rows: 652 (nullable, number)
+ total_columns: 27 (nullable, number)
+ url: `xyz/path_to/bucket/012_account_balances_20180613140845.zip` (required, string)
    Where the file lives in S3
+ message (optional, nullable)
+ status (required, enum[string])
    + Members
        + ready
        + running
        + finished
        + failed
+ seconds_elapsed: `10.061132` (required, string)

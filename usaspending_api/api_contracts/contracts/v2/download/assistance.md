FORMAT: 1A
HOST: https://api.usaspending.gov

# Assistance Download [/api/v2/download/assistance/]

## POST

Returns a zipped file containing contract award data

+ Request (application/json)
    + Attributes
        + `award_id`: `ASST_NON_12FA00PY52375933_12D2` (required, string)
+ Response 200 (application/json)
    + Attributes
        + `results` (AssistanceDownloadResponse)

# Data Structures

## AssistanceDownloadResponse (object)
+ `total_size`: 35.055 (number, nullable)
    The total size of the file being returned
+ `file_name`: `ASST_12FA00PY52375933_12D2.zip` (required, string)
+ `total_rows`: 652 (number, nullable)
+ `total_columns`: 27 (number, nullable)
+ `url`: `xyz/path_to/bucket/ASST_12FA00PY52375933_12D2.zip` (required, string)
    Where the file lives in S3
+ `message` (optional, string, nullable)
+ `status` (required, enum[string])
    + Members
        + `ready`
        + `running`
        + `finished`
        + `failed`
+ `seconds_elapsed`: `10.061132` (required, string)

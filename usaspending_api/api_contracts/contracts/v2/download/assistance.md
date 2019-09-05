FORMAT: 1A
HOST: https://api.usaspending.gov

# Assistance Download [/api/v2/download/assistance/]

## POST

Returns a link to a zipped file containing contract award data

+ Request (application/json)
    + Attributes
        + `award_id`: `ASST_NON_12FA00PY52375933_12D2` (required, string)
+ Response 200 (application/json)
    + Attributes
        + `results` (AssistanceDownloadResponse)

# Data Structures

## AssistanceDownloadResponse (object)
+ `total_size` (number, nullable)
    The total size of the file being returned
+ `file_name` (required, string)
+ `total_rows` (number, nullable)
+ `total_columns` (number, nullable)
+ `url` (required, string)
    Where the file lives in S3
+ `message` (optional, string, nullable)
+ `status` (required, enum[string])
    + Members
        + `ready`
        + `running`
        + `finished`
        + `failed`
+ `seconds_elapsed` (required, string)

FORMAT: 1A
HOST: https://api.usaspending.gov

# Assistance Download [/api/v2/download/assistance/]

## POST

Returns a link to a zipped file containing contract award data

+ Request (application/json)
    + Attributes
        + `award_id`: `ASST_NON_12FA00PY52375933_12D2` (required, string)
+ Response 200 (application/json)
    + Attributes (AssistanceDownloadResponse)
    + Body
        {
            "status": "finished",
            "file_name": "ASST_12FA00PY52375933_20190905135023230073.zip",
            "total_rows": 2,
            "total_columns": 86,
            "seconds_elapsed": "6.871675",
            "message": null,
            "url": "/Users/emilybrents/Documents/data_act/usaspending-api/csv_downloads/ASST_12FA00PY52375933_20190905135023230073.zip",
            "total_size": 58.983
        }

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

FORMAT: 1A
HOST: https://api.usaspending.gov

# IDV Download [/api/v2/download/idv/]

## POST

Returns a link to a zipped file containing IDV data

+ Request (application/json)
    + Attributes
        + `award_id`: `CONT_IDV_BBGBPA08452513_9568` (required, string)
+ Response 200 (application/json)
    + Attributes (IDVDownloadResponse)
    + Body
        {
            "status": "finished",
            "file_name": "IDV_DTNH2216C00007_20190905145159064212.zip",
            "seconds_elapsed": "6.799396",
            "total_columns": 276,
            "total_rows": 3,
            "total_size": 62.094,
            "url": "/Users/emilybrents/Documents/data_act/usaspending-api/csv_downloads/IDV_DTNH2216C00007_20190905145159064212.zip",
            "message": null
        }

# Data Structures

## IDVDownloadResponse (object)
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

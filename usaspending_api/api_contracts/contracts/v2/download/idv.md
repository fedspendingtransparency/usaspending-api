FORMAT: 1A
HOST: https://api.usaspending.gov

# IDV Download [/api/v2/download/idv/]

## POST

Creates a new download job for the requested award and returns a link to a zipped file containing IDV data

+ Request (application/json)
    + Attributes (object)
        + `award_id`: `CONT_IDV_BBGBPA08452513_9568` (required, string)
+ Response 200 (application/json)
    + Attributes (object)
        + `file_name` (required, string) 
            Is the name of the zipfile containing CSVs that will be generated (file_name is timestamp followed by `_transactions` or `_awards`).
        + `message` (required, string, nullable) 
            A human readable error message if the `status` is `failed`, otherwise it is `null`.
        + `seconds_elapsed` (required, string, nullable) 
            Is the time taken to genereate the file (if `status` is `finished` or `failed`), or time taken so far (if `running`).
        + `status` (required, enum[string]) 
            A string representing the current state of the CSV generation request.
            + Members
                + `failed`
                + `finished`
                + `ready`
                + `running`
        + `total_columns` (required, number, nullable) 
            Is the number of columns in the CSV, or `null` if not finished.
        + `total_rows` (required, number, nullable) 
            Is the number of rows in the CSV, or `null` if not finished.
        + `total_size` (required, number, nullable) 
            Is the estimated file size of the CSV in kilobytes, or `null` if not finished.
        + `url` (required, string) 
            The URL for the file.
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



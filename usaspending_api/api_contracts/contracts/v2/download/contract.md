FORMAT: 1A
HOST: https://api.usaspending.gov

# Contract Download [/api/v2/download/contract/]

## POST

Creates a new download job for the requested award and returns a link to a zipped file containing contract award data

+ Request (application/json)
    + Attributes
        + `award_id`: `CONT_AWD_UZ02_9700_SPM2DV11D9200_9700` (required, string)
+ Response 200 (application/json)
    + Attributes
        + `total_size` (required, number)
            The total size of the file being returned
        + `file_name` (required, string)
        + `total_rows` (required, number)
        + `total_columns` (required, number)
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

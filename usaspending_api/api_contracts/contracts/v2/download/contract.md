FORMAT: 1A
HOST: https://api.usaspending.gov

# Contract Download [/api/v2/download/contract/]

## POST

Creates a new download job for the requested award and returns a link to a zipped file containing contract award data

+ Request (application/json)
    + Attributes (object)
        + `award_id`: `CONT_AWD_UZ02_9700_SPM2DV11D9200_9700` (required, string)
        + `file_format` (optional, enum[string])
            The format of the file(s) in the zip file containing the data.
            + Default: `csv`
            + Members
                + `csv`
                + `tsv`
                + `pstxt`
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
                "file_name": "ASST_12FA00PY52375933_20190905135023230073.zip",
                "total_rows": 2,
                "total_columns": 86,
                "seconds_elapsed": "6.871675",
                "message": null,
                "url": "/Users/emilybrents/Documents/data_act/usaspending-api/csv_downloads/ASST_12FA00PY52375933_20190905135023230073.zip",
                "total_size": 58.983
            }

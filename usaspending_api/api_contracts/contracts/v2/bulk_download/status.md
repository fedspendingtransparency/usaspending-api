FORMAT: 1A
HOST: https://api.usaspending.gov

# Bulk Download Status [/api/v2/bulk_download/status{?file_name}]

## GET

This route gets the current status of a download job.
        
+ Request (application/json)
    + Parameters
        + `file_name`: `012_PrimeTransactions_2020-01-13_H20M58S34486877.zip` (required, string) 
            Taken from the `file_name` field of a download endpoint response.

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
        + `file_url` (required, string)
            The URL for the file.
    + Body
            
            {
                "status": "finished",
                "message": null,
                "file_name": "012_PrimeTransactions_2020-01-13_H20M58S34486877.zip",
                "file_url": "/usaspending-api/csv_downloads/012_PrimeTransactions_2020-01-13_H20M58S34486877.zip",
                "total_size": 3.169,
                "total_columns": 276,
                "total_rows": 0,
                "seconds_elapsed": "4.145662"
            }
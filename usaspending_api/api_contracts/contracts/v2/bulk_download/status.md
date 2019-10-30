FORMAT: 1A
HOST: https://api.usaspending.gov

# Bulk Download Status [/api/v2/bulk_download/status{?file_name}]

## GET

This route gets the current status of a download job.
        
+ Request (application/json)
    + Parameters
        + `file_name`: `all_prime_transactions_subawards_20191017223212534453.zip` (required, string) 
        Taken from the `file_name` field of a download endpoint response.

+ Response 200 (application/json)
    + Attributes
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

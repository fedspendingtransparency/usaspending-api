FORMAT: 1A
HOST: https://api.usaspending.gov

# IDV Download [/api/v2/download/idv/]

## POST

Creates a new download job for the requested award and returns a link to a zipped file containing IDV data

+ Request (application/json)
    + Attributes (object)
        + `award_id`: `CONT_IDV_BBGBPA08452513_9568` (required, string)
        + `file_format` (optional, enum[string])
            The format of the file(s) in the zip file containing the data.
            + Default: `csv`
            + Members
                + `csv`
                + `tsv`
                + `pstxt`
+ Response 200 (application/json)
    + Attributes (object)
        + `status_url` (required, string)
            The endpoint used to get the status of a download.
        + `file_name` (required, string)
            Is the name of the zipfile containing CSVs that will be generated (file_name is timestamp followed by `_transactions` or `_awards`).
        + `file_url` (required, string)
            The URL for the file.
        + `download_request` (required, object)
            The JSON object used when processing the download.


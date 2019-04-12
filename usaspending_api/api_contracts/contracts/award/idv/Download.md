FORMAT: 1A
HOST: https://api.usaspending.gov

# IDV Download [/api/v2/awards/idvs/download/]

This end point returns a zipped file containing IDV data.

## GET

+ Request (application/json)
    + Attributes (object)
        + `award_id`: `CONT_AW_9700_-NONE-_N0018918D0057_-NONE-` (required, string)
+ Response 200 (application/json)
    + Attributes (IDVDownloadResponse)

# Data Structures

## IDVDownloadResponse (object)
+ url: www.maxwellkendall.com (required, string)

FORMAT: 1A
HOST: https://api.usaspending.gov

# Contract Download [/api/v2/download/contract/]

## POST

Returns a link to a zipped file containing contract award data

+ Request (application/json)
    + Attributes
        + `award_id`: `CONT_AWD_UZ02_9700_SPM2DV11D9200_9700` (required, string)
+ Response 200 (application/json)
    + Attributes
        + `results` (ContractDownloadResponse)

# Data Structures

## ContractDownloadResponse (object)
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

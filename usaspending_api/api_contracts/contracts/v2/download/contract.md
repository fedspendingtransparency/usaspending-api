FORMAT: 1A
HOST: https://api.usaspending.gov

# Contract Download [/api/v2/download/contract/]

## POST

Returns a zipped file containing contract award data

+ Request (application/json)
    + Attributes
        + `award_id`: `CONT_AWD_UZ02_9700_SPM2DV11D9200_9700` (required, string)
+ Response 200 (application/json)
    + Attributes
        + `results` (ContractDownloadResponse)

# Data Structures

## ContractDownloadResponse (object)
+ `total_size`: 35.055 (number, nullable)
    The total size of the file being returned
+ `file_name`: `CONT_AWD_UZ02_9700_SPM2DV11D9200_9700.zip` (required, string)
+ `total_rows`: 652 (number, nullable)
+ `total_columns`: 27 (number, nullable)
+ `url`: `xyz/path_to/bucket/CONT_AWD_UZ02_9700_SPM2DV11D9200_9700.zip` (required, string)
    Where the file lives in S3
+ `message` (optional, string, nullable)
+ `status` (required, enum[string])
    + Members
        + `ready`
        + `running`
        + `finished`
        + `failed`
+ `seconds_elapsed`: `10.061132` (required, string)

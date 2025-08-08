FORMAT: 1A
HOST: https://api.usaspending.gov

# Program Activities [/api/v2/federal_accounts/{federal_account_code}/program_activities/{?limit,page,order,sort}]

This route returns program activities that the specified federal account has allotted money toward.

## GET

+ Parameters
    + `federal_account_code`: `431-0500` (required, string)
        Federal account code consisting of the AID and main account code
    + `limit`: 10 (optional, number)
        The maximum number of results to return in the response
    + `page`: 1 (optional, number)
        The response page to return (the record offset is (`page` - 1) * `limit`).
    + `sort` (optional, enum[string])
        + Default: `code`
        + Members
            + `code`
            + `name`
            + `type`
    + `order` (optional, enum[string])
        + Default: `desc`
        + Members
            + `asc`
            + `desc`

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, array[ProgramActivities], fixed-type)
        + `page_metadata` (required, PageMetadata, fixed-type)
            Information used for pagination of results.
    + Body

            {
                "results": [
                    {
                        "code": "0000",
                        "name": "OTHER/UNKNOWN",
                        "type": "PAC/PAN"
                    },
                    {
                        "code": "0001",
                        "name": "TECHNICAL AND SCIENTIFIC ACTIVITIES",
                        "type": "PAC/PAN"
                    },
                    {
                        "code": "0001",
                        "name": "TECHNICAL AND SCIENTIFIC ACTIVITIES",
                        "type": "PARK"
                    }
                ],
                "page_metadata": {
                    "limit": 10,
                    "page": 1,
                    "next": null,
                    "previous": null,
                    "hasNext": false,
                    "hasPrevious": false,
                    "total": 3
                }
            }

# Data Structures

## ProgramActivities (object)
+ `code` (required, string)
+ `name` (required, string)
+ `type` (required, enum[string], fixed-type)
    Whether the Program Activity values are from the older Program Activity Code / Name (PAC/PAN) or the Program Activity Reporting Key (PARK)
    + Members
        + `PAC/PAN`
        + `PARK`

## PageMetadata (object)
+ `limit` (required, number)
+ `page` (required, number)
+ `next` (required, number, nullable)
+ `previous` (required, number, nullable)
+ `hasNext` (required, boolean)
+ `hasPrevious` (required, boolean)
+ `total` (required, number)

FORMAT: 1A
HOST: https://api.usaspending.gov

# Agency Reporting Differences [/api/v2/reporting/agencies/{agency_code}/differences?{fiscal_year,fiscal_period,page,limit,order,sort}]

This endpoint is used to power USAspending.gov's about the data obligation differences modal. This data can be used to better understand the way an agency submits data.

## GET

This endpoint returns an overview of government agency obligation differences data.

+ Parameters
    + `agency_code`: `020` (required, string)
        The specific agency code.
    + `fiscal_year`: 2020 (required, number)
        The fiscal year.
    + `fiscal_period`: 10 (required, number)
        The fiscal period.
    + `page` (optional, number)
        The page of results to return based on the limit.
        + Default: 1
    + `limit` (optional, number)
        The number of results to include per page.
        + Default: 10
    + `order`: `desc` (optional, enum[string])
        The direction (`asc` or `desc`) that the `sort` field will be sorted in.
        + Default: `desc`
        + Members
            + `asc`
            + `desc`
    + `sort`: `difference` (optional, enum[string])
        A data field that will be used to sort the response array.
        + Default: `difference`
        + Members
            + `difference`
            + `file_a_obligations`
            + `file_b_obligations`
            + `tas`

+ Response 200 (application/json)

    + Attributes (object)
        + `page_metadata` (required, PageMetaDataObject, fixed-type)
        + `results` (required, array[ObligationDifferences], fixed-type)
    + Body

            {

                "page_metadata": {
                  "page": 1,
                  "hasNext": false,
                  "hasPrevious": false,
                  "total": 2
                },
                "results": [
                  {
                    "tas": "210-1503",
                    "file_a_obligations": 234543543,
                    "file_b_obligations": 456438768,
                    "difference": -221895225
                  },
                  {
                    "tas": "012-0212",
                    "file_a_obligations": 43637623,
                    "file_b_obligations": 20486582,
                    "difference": 23151041
                  }
                ]
            }

# Data Structures

## PageMetaDataObject (object)
+ `page` (required, number)
+ `hasNext` (required, boolean)
+ `hasPrevious` (required, boolean)
+ `total` (required, number)

## ObligationDifferences (object)
+ `tas` (required, string)
+ `file_a_obligations` (required, number)
+ `file_b_obligations` (required, number)
+ `difference` (required, number)

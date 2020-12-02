FORMAT: 1A
HOST: https://api.usaspending.gov

# Agency Reporting Differences [/api/v2/reporting/agencies/{agency_code}/differences/{?fiscal_year,fiscal_period,page,limit,order,sort}]

This endpoint is used to power USAspending.gov's About the Data \| Agencies reported balance and spending differences over a submission period

## GET

This endpoint returns an overview of government agency obligation differences data.

+ Parameters
    + `agency_code`: `020` (required, string)
        The specific agency code.
    + `fiscal_year`: 2020 (required, number)
        The fiscal year.
    + `fiscal_period`: 10 (required, number)
        The fiscal period. Valid values: 2-12 (2 = November ... 12 = September)
        For retriving quarterly data, provide the period which equals 'quarter * 3' (e.g. Q2 = P6)
    + `page` (optional, number)
        The page of results to return based on the limit.
        + Default: 1
    + `limit` (optional, number)
        The number of results to include per page.
        + Default: 10
    + `order` (optional, enum[string])
        The direction (`asc` or `desc`) that the `sort` field will be sorted in.
        + Default: `desc`
        + Members
            + `asc`
            + `desc`
    + `sort` (optional, enum[string])
        A data field that will be used to sort the response array.
        + Default: `difference`
        + Members
            + `difference`
            + `file_a_obligation`
            + `file_b_obligation`
            + `tas`

+ Response 200 (application/json)

    + Attributes (object)
        + `page_metadata` (required, PaginationMetadata, fixed-type)
        + `results` (required, array[ObligationDifferences], fixed-type)
    + Body

            {

                "page_metadata": {
                    "page": 1,
                    "next": 2,
                    "previous": 0,
                    "hasNext": false,
                    "hasPrevious": false,
                    "total": 2,
                    "limit": 10
                },
                "results": [
                    {
                        "tas": "210-1503",
                        "file_a_obligation": 234543543,
                        "file_b_obligation": 456438768,
                        "difference": -221895225
                    },
                    {
                        "tas": "012-0212",
                        "file_a_obligation": 43637623,
                        "file_b_obligation": 20486582,
                        "difference": 23151041
                    }
                ]
            }

# Data Structures

## PaginationMetadata (object)
+ `page` (required, number)
+ `next` (required, number, nullable)
+ `previous` (required, number, nullable)
+ `hasNext` (required, boolean)
+ `hasPrevious` (required, boolean)
+ `total` (required, number)
+ `limit` (required, number)

## ObligationDifferences (object)
+ `tas` (required, string)
+ `file_a_obligation` (required, number)
+ `file_b_obligation` (required, number)
+ `difference` (required, number)

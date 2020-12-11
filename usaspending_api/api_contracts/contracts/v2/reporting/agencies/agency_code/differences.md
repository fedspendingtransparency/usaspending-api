FORMAT: 1A
HOST: https://api.usaspending.gov

# Agency Reporting Differences [/api/v2/reporting/agencies/{toptier_code}/differences/{?fiscal_year,fiscal_period,page,limit,order,sort}]

This endpoint is used to power USAspending.gov's About the Data \| Agencies reported balance and spending differences over a submission period

## GET

This endpoint returns an overview of government agency obligation differences data.

+ Parameters
    + `toptier_code`: `020` (required, string)
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
        + Default: `tas`
        + Members
            + `difference`
            + `file_a_obligation`
            + `file_b_obligation`
            + `tas`

+ Response 200 (application/json)

    + Attributes (object)
        + `page_metadata` (required, PaginationMetadata, fixed-type)
        + `results` (required, array[ObligationDifferences], fixed-type)
        + `messages` (required, array[string], fixed-type)
            An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.
    + Body

            {

                "page_metadata": {
                    "page": 1,
                    "total": 10,
                    "limit": 2,
                    "next": 2,
                    "previous": null,
                    "hasNext": true,
                    "hasPrevious": false
                },
                "results": [
                    {
                        "tas": "011-X-8345-000",
                        "file_a_obligation": 47425.37,
                        "file_b_obligation": 240066.32,
                        "difference": -192640.95
                    },
                    {
                        "tas": "011-X-8245-000",
                        "file_a_obligation": 428508.11,
                        "file_b_obligation": 2358478.83,
                        "difference": -1929970.72
                    }
                ],
                "messages": []
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

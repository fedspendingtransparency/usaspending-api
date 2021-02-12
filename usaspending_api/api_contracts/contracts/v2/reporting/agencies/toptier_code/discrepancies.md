FORMAT: 1A
HOST: https://api.usaspending.gov

# Agency Reporting Discrepancies [/api/v2/reporting/agencies/{toptier_code}/discrepancies/{?fiscal_year,fiscal_period,page,limit,order,sort}]

This endpoint is used to power USAspending.gov's About the Data \| Agencies TAS discrepencies over a submission period

## GET

This endpoint returns an overview of government agency TAS discrepancies data.

+ Parameters
    + `toptier_code`: `020` (required, string)
        The specific agency's toptier code.
    + `fiscal_year`: 2020 (required, number)
        The fiscal year (2017 or later).
    + `fiscal_period`: 10 (required, number)
        The fiscal period. Valid values: 2-12 (2 = November ... 12 = September)
        For retrieving quarterly data, provide the period which equals 'quarter * 3' (e.g. Q2 = P6)
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
        + Default: `amount`
        + Members
            + `amount`
            + `tas`

+ Response 200 (application/json)

    + Attributes (object)
        + `page_metadata` (required, PaginationMetadata, fixed-type)
        + `results` (required, array[TASDiscrepancies], fixed-type)
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
                        "amount": 234543543
                    },
                    {
                        "tas": "012-0212",
                        "amount": 43637623
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

## TASDiscrepancies (object)
+ `tas` (required, string)
+ `amount` (required, number)

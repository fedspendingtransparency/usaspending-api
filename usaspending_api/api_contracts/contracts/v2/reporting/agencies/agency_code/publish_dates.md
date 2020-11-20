FORMAT: 1A
HOST: https://api.usaspending.gov

# Agency Reporting Publish Dates [/api/v2/reporting/agencies/{agency_code}/publish_dates?{fiscal_year,fiscal_period,search,page,limit,order,sort}]

This endpoint is used to power USAspending.gov's about the data submission history modal. This data can be used to better understand the ways agencies submit data.

## GET

This endpoint returns an overview of government agencies submission data.

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
    + `order` (optional, enum[string])
        The direction (`asc` or `desc`) that the `sort` field will be sorted in.
        + Default: `desc`
        + Members
            + `asc`
            + `desc`
    + `sort` (optional, enum[string])
        A data field that will be used to sort the response array.
        + Default: `publication_date`
        + Members
            + `publication_date`
            + `certification_date`

+ Response 200 (application/json)

    + Attributes (object)
        + `results` (required, array[AgencyData], fixed-type)
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
                        "publication_date": "2020-10-11T11:59:21Z",
                        "certification_date": "2020-10-22T11:59:21Z",
                    },
                    {
                        "publication_date": "2020-07-10T11:59:21Z",
                        "certification_date": "2020-07-11T11:59:21Z",
                    }
                ]
            }

# Data Structures

## PageMetadata (object)
+ `page` (required, number)
+ `next` (required, number, nullable)
+ `previous` (required, number, nullable)
+ `hasNext` (required, boolean)
+ `hasPrevious` (required, boolean)
+ `total` (required, number)
+ `limit` (required, number)

## SubmissionHistory (object)
+ `publication_date` (required, string, nullable)
+ `certification_date` (required, string, nullable)

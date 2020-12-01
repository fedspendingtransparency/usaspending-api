FORMAT: 1A
HOST: https://api.usaspending.gov

# Agency Reporting Publish Dates [/api/v2/reporting/agencies/{agency_code}/publish_dates?{fiscal_year,search,page,limit,order,sort}]

This endpoint is used to power USAspending.gov's about the data submission history modal. This data can be used to better understand the ways agencies submit data.

## GET

+ Parameters

    + `fiscal_year`: 2020 (required, number)
        The fiscal year.
    + `search` (optional, string)
        The agency name to filter on.
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
        + Default: `current_total_budget_authority_amount`
        + Members
            + `name`
            + `abbreviation`
            + `code`
            + `current_total_budget_authority_amount`
            + `period`

+ Response 200 (application/json)

    + Attributes (object)
        + `page_metadata` (required, PageMetaDataObject, fixed-type)
        + `results` (required, array[Agency], fixed-type)
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
                        "name": "Department of Health and Human Services",
                        "abbreviation": "DHHS",
                        "code": "020",
                        "current_total_budget_authority_amount": 8361447130497.72,
                        "periods": [
                            "period": 2,
                            "quarter": 1,
                            "submission_dates": {
                                "publication_date" : "2020-01-20T11:59:21Z",
                                "certification_date" : "2020-01-21T10:58:21Z"
                            },
                            "quarterly": false,
                            "submitted": true
                        ]
                    },
                    {
                        "name": "Department of Treasury",
                        "abbreviation": "DOT",
                        "code": "021",
                        "current_total_budget_authority_amount": 8361447130497.72,
                        "periods": [
                            "period": 2,
                            "quarter": 1,
                            "submission_dates": {
                                "publication_date" : "2020-01-20T11:59:21Z",
                                "certification_date" : "2020-01-21T10:58:21Z"
                            },
                            "quarterly": false,
                            "submitted": true
                        ]
                    }
                ]
            }

# Data Structures

## PageMetaDataObject (object)
+ `page` (required, number)
+ `next` (required, number, nullable)
+ `previous` (required, number, nullable)
+ `hasNext` (required, boolean)
+ `hasPrevious` (required, boolean)
+ `total` (required, number)
+ `limit` (required, number)

## SubmissionDates
+ `publication_date` (required, string, nullable)
+ `certification_date` (required, string, nullable)

## Period (object)
+ `period` (required, number)
+ `quarter` (required, number)
+ `submission_dates` (required, object[SubmissionDates], nullable)
+ `quarterly` (required, boolean)
+ `submitted` (required, boolean)

## Agency (object)
+ `name` (required, string)
+ `abbreviation` (required, string)
+ `code` (required, string)
+ `current_total_budget_authority_amount` (required, number)
+ `periods` (required, array[Period], fixed-type)

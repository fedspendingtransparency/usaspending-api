FORMAT: 1A
HOST: https://api.usaspending.gov

# Agency Reporting Publish Dates [/api/v2/reporting/agencies/publish_dates/{?fiscal_year,filter,page,limit,order,sort}]

This endpoint is used to power USAspending.gov's About the Data \| Agencies submission publication dates.

## GET

+ Parameters
    + `fiscal_year`: 2020 (required, number)
        The fiscal year.
    + `filter` (optional, string)
        The agency name or abbreviation to filter on (partial match, case insesitive).
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
            + `agency_name`
            + `abbreviation`
            + `toptier_code`
            + `current_total_budget_authority_amount`
            + `publication_date`
                When using publication_date, provide the desired fiscal period (2-12) after a comma
                example: &sort=publication_date,10

+ Response 200 (application/json)

    + Attributes (object)
        + `page_metadata` (required, PaginationMetadata, fixed-type)
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
                        "agency_name": "Department of Health and Human Services",
                        "abbreviation": "DHHS",
                        "toptier_code": "020",
                        "current_total_budget_authority_amount": 8361447130497.72,
                        "periods": [{
                            "period": 2,
                            "quarter": 1,
                            "submission_dates": {
                                "publication_date" : "2020-01-20T11:59:21Z",
                                "certification_date" : "2020-01-21T10:58:21Z"
                            },
                            "quarterly": false
                        }]
                    },
                    {
                        "agency_name": "Department of Treasury",
                        "abbreviation": "DOT",
                        "toptier_code": "021",
                        "current_total_budget_authority_amount": 8361447130497.72,
                        "periods": [{
                            "period": 2,
                            "quarter": 1,
                            "submission_dates": {
                                "publication_date" : "2020-01-20T11:59:21Z",
                                "certification_date" : "2020-01-21T10:58:21Z"
                            },
                            "quarterly": false
                        }]
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

## SubmissionDates (object)
+ `publication_date` (required, string, nullable)
+ `certification_date` (required, string, nullable)

## Period (object)
+ `period` (required, number)
+ `quarter` (required, number)
+ `submission_dates` (required, array[SubmissionDates], nullable)
+ `quarterly` (required, boolean)

## Agency (object)
+ `agency_name` (required, string)
+ `abbreviation` (required, string)
+ `toptier_code` (required, string)
+ `current_total_budget_authority_amount` (required, number)
+ `periods` (required, array[Period], fixed-type)

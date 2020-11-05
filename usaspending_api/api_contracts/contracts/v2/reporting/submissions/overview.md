FORMAT: 1A
HOST: https://api.usaspending.gov

# Submissions Overview [/api/v2/reporting/agencies/submissions/overview?{fiscal_year,search,page,limit,order,sort}]

This endpoint is used to power USAspending.gov's about the data agencies page submission overview tab. This data can be used to better understand the ways agencies submit data.

## GET

This endpoint returns an overview of government agencies submission data.

+ Parameters

    + `fiscal_year`: 2020 (required, string)
        The fiscal year.
        + Default: `All`.
    + `search`: treasury (optional, string)
        The agency name to filter on.
    + `page`: 1 (optional, number)
        The page of results to return based on the limit.
        + Default: 1
    + `limit`: 5 (optional, number)
        The number of results to include per page.
        + Default: 10
    + `order`: `desc` (optional, enum[string])
        The direction (`asc` or `desc`) that the `sort` field will be sorted in.
        + Default: `desc`
        + Members
            + `asc`
            + `desc`
    + `sort`: `current_total_budget_authority_amount` (optional, enum[string])
        A data field that will be used to sort the response array.
        + Default: `current_total_budget_authority_amount`
        + Members
            + `name`
            + `current_total_budget_authority_amount`
            + `period`

+ Response 200 (application/json)

    + Attributes (object)
        + `page_metadata` (required, PageMetaDataObject, fixed-type)
        + `results` (required, array[SubmissionOverview], fixed-type)
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
                    "name": "Department of Health and Human Services",
                    "abbreviation": "DHHS",
                    "code": "020",
                    "current_total_budget_authority_amount": 8361447130497.72,
                    "periods": [
                        "period": 2,
                        "quarter": 1,
                        "date": "2020-01-20T11:59:21Z",
                        "certified": true,
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
                        "date": "2020-01-20T11:59:21Z",
                        "certified": false,
                        "quarterly": false,
                        "submitted": true
                    ]
                  }
                ]
            }

# Data Structures

## PageMetaDataObject (object)
+ `page` (required, number)
+ `hasNext` false (required, boolean)
+ `hasPrevious` false (required, boolean)
+ `total` (required, number)

## Period (object)
+ `period` (required, number),
+ `quarter` (required, number),
+ `date` (required, Date, nullable),
+ `certified` (required, boolean),
+ `quarterly` (required, boolean),
+ `submitted` (required, boolean)

## SubmissionOverview (object)
+ `name` (required, string)
+ `abbreviation` (required, string)
+ `code` (required, string)
+ `current_total_budget_authority_amount` (required, number)
+ `periods` (required, array[Period], fixed-type)

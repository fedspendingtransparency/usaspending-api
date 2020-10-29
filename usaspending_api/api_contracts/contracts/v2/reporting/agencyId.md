FORMAT: 1A
HOST: https://api.usaspending.gov

# Agency ID [/api/v2/reporting/agencies/{agency_code}/?{page,limit,order,sort}]

This endpoint is used to power USAspending.gov's about the data agency page. This data can be used to better understand the way an agency submits data.

## GET

This endpoint returns an overview of government agency submission data.

+ Parameters
    + `agency_code`: `020` (required, string)
        The specific agency code.
    + `page`: 1 (optional, number)
        The page of results to return based on the limit.
        + Default: 1
    + `limit`: 5 (optional, number)
        The number of results to include per page.
        + Default: 10
    + `order`: `desc` (optional, string)
        The direction (`asc` or `desc`) that the `sort` field will be sorted in.
        + Default: `desc`.
    + `sort`: `current_total_budget_authority_amount` (optional, string)
        A data field that will be used to sort the response array.
        + Default: `current_total_budget_authority_amount`.

+ Response 200 (application/json)

    + Attributes (object)
        + `page_metadata` (required, PageMetaDataObject, fixed-type)
        + `results` (required, array[AgencyData], fixed-type)
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
                    "fiscal_year": 2020,
                    "fiscal_period": 12,
                    "current_total_budget_authority_amount": 8361447130497.72,
                    "recent_publication_date": "01/10/2020 11:59:21",
                    "recent_publication_date_certified": false,
                    "descrepency_count": 20,
                    "obligation_difference": 436376232652.87
                  },
                  {
                    "name": "Department of Treasury",
                    "abbreviation": "DOT",
                    "code": "021",
                    "fiscal_year": 2020,
                    "fiscal_period": 9,
                    "current_total_budget_authority_amount": 8361447130497.72,
                    "recent_publication_date": null,
                    "recent_publication_date_certified": true,
                    "descrepency_count": 10,
                    "obligation_difference": 436376232652.87
                  }
                ]
            }

# Data Structures

## PageMetaDataObject (object)
+ `page`: (required, number)
+ `hasNext`: false (required, boolean)
+ `hasPrevious`: false (required, boolean)
+ `total`: (required, number)

## AgencyData (object)
+ `name`: (required, string)
+ `abbreviation`: (required, string)
+ `code`: (required, string)
+ `fiscal_year`: (required, number)
+ `fiscal_period`: (required, number)
+ `current_total_budget_authority_amount`: (required, number)
+ `recent_publication_date`: (required, date, nullable)
+ `recent_publication_date_certified`: (required, boolean)
+ `descrepency_count`: (required, number)
    A count of agency TAS in GTAS not in file A.
+ `obligation_difference`: (required, number)
    The difference in file A and file B obligations.

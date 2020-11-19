FORMAT: 1A
HOST: https://api.usaspending.gov

# Agency Reporting Overview [/api/v2/reporting/agencies/{agency_code}/overview?{page,limit,order,sort}]

This endpoint is used to power USAspending.gov's about the data agency page. This data can be used to better understand the way an agency submits data.

## GET

This endpoint returns an overview of government agency submission data.

+ Parameters
    + `agency_code`: `020` (required, string)
        The specific agency code.
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
            + `code`
            + `current_total_budget_authority_amount`
            + `discrepancy_count`
            + `fiscal_year`
            + `fiscal_period`
            + `name`
            + `obligation_difference`
            + `recent_publication_date`
            + `recent_publication_date_certified`

+ Response 200 (application/json)

    + Attributes (object)
        + `page_metadata` (required, PageMetaDataObject, fixed-type)
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
                        "fiscal_year": 2020,
                        "fiscal_period": 12,
                        "current_total_budget_authority_amount": 8361447130497.72,
                        "recent_publication_date": "2020-01-10T11:59:21Z",
                        "recent_publication_date_certified": false,
                        "discrepancy_count": 20,
                        "obligation_difference": 436376232652.87
                    },
                    {
                        "fiscal_year": 2020,
                        "fiscal_period": 9,
                        "current_total_budget_authority_amount": 8361447130497.72,
                        "recent_publication_date": null,
                        "recent_publication_date_certified": true,
                        "discrepancy_count": 10,
                        "obligation_difference": 436376232652.87
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

## AgencyData (object)
+ `fiscal_year` (required, number)
+ `fiscal_period` (required, number)
+ `current_total_budget_authority_amount` (required, number)
+ `recent_publication_date` (required, string, nullable)
+ `recent_publication_date_certified` (required, boolean)
+ `discrepancy_count` (required, number)
    A count of agency TAS in GTAS not in file A.
+ `obligation_difference` (required, number)
    The difference in file A and file B obligations.

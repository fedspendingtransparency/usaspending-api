FORMAT: 1A
HOST: https://api.usaspending.gov

# Agency Reporting Overview [/api/v2/reporting/agencies/{agency_code}/overview/{?page,limit,order,sort}]

This endpoint is used to power USAspending.gov's About the Data \| Agencies agency details table.

## GET

This endpoint returns an overview of government agency submission data.

+ Parameters
    + `agency_code`: `020` (required, string)
        The specific agency.
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
            + `current_total_budget_authority_amount`
            + `fiscal_period`
            + `fiscal_year`
            + `missing_tas_accounts_count`
            + `obligation_difference`
            + `recent_publication_date_certified`
            + `recent_publication_date`
            + `tas_obligation_not_in_gtas_total`

+ Response 200 (application/json)

    + Attributes (object)
        + `page_metadata` (required, PaginationMetadata, fixed-type)
        + `results` (required, array[AgencyData], fixed-type)
        + `messages` (optional, array[string])
            An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.

    + Body

            {
                "messages": [],
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
                        "tas_account_discrepancies_totals": {
                            "gtas_obligation_total": 66432,
                            "tas_accounts_total": 2342,
                            "tas_obligation_not_in_gtas_total": 343345,
                            "missing_tas_accounts_count": 10
                        },
                        "obligation_difference": 436376232652.87
                    },
                    {
                        "fiscal_year": 2020,
                        "fiscal_period": 9,
                        "current_total_budget_authority_amount": 8361447130497.72,
                        "recent_publication_date": null,
                        "recent_publication_date_certified": true,
                        "tas_account_discrepancies_totals": {
                            "gtas_obligation_total": 66432,
                            "tas_accounts_total": 23903,
                            "tas_obligation_not_in_gtas_total": 11543,
                            "missing_tas_accounts_count": 10
                        },
                        "obligation_difference": 436376232652.87
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

## TASTotals (object)
+ `gtas_obligation_total` (required, number)
+ `tas_accounts_total` (required, number)
+ `tas_obligation_not_in_gtas_total` (required, number)
+ `missing_tas_accounts_count` (required, number)

## AgencyData (object)
+ `fiscal_year` (required, number)
+ `fiscal_period` (required, number)
+ `current_total_budget_authority_amount` (required, number)
+ `recent_publication_date` (required, string, nullable)
+ `recent_publication_date_certified` (required, boolean)
+ `recent_publication_date_certified` (required, boolean)
+ `tas_account_discrepancies_totals` (required, array[TASTotals], fixed-type)
+ `obligation_difference` (required, number)
    The difference in file A and file B obligations.

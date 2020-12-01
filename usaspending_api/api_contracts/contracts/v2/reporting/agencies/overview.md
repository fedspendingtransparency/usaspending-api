FORMAT: 1A
HOST: https://api.usaspending.gov

# Agencies Reporting Overview [/api/v2/reporting/agencies/overview/{?fiscal_year,fiscal_period,search,page,limit,order,sort}]

This endpoint is used to power USAspending.gov's About the Data \| Agencies Overview table. This data can be used to better understand the ways agencies submit data.

## GET

This endpoint returns an overview list of government agencies submission data.

+ Parameters

    + `fiscal_year`: 2020 (required, number)
        The fiscal year.
    + `fiscal_period`: 10 (required, number)
        The fiscal period. Valid values: 2-12 (2 = November ... 12 = September)
        For retriving quarterly data, provide the period which equals 'quarter * 3' (e.g. Q2 = P6)
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
            + `code`
            + `current_total_budget_authority_amount`
            + `missing_tas_accounts_total`
            + `name`
            + `obligation_difference`
            + `recent_publication_date`
            + `recent_publication_date_certified`

+ Response 200 (application/json)

    + Attributes (object)
        + `page_metadata` (required, PaginationMetadata, fixed-type)
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
                        "name": "Department of Health and Human Services",
                        "abbreviation": "DHHS",
                        "code": "020",
                        "current_total_budget_authority_amount": 8361447130497.72,
                        "recent_publication_date": "2020-01-10T11:59:21Z",
                        "recent_publication_date_certified": false,
                        "tas_account_discrepancies_totals": {
                            "gtas_obligation_total": 55234,
                            "missing_tas_accounts_total": 20
                        },
                        "obligation_difference": 436376232652.87
                    },
                    {
                        "name": "Department of Treasury",
                        "abbreviation": "DOT",
                        "code": "021",
                        "current_total_budget_authority_amount": 8361447130497.72,
                        "recent_publication_date": null,
                        "recent_publication_date_certified": true,
                        "tas_account_discrepancies_totals": {
                            "tas_obligations_total": 66432,
                            "tas_obligations_not_in_gtas_total": 11543,
                            "tas_accounts_total": 10
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
+ `tas_obligations_total` (required, number)
+ `tas_obligations_not_in_gtas_total` (required, number)
+ `tas_accounts_total` (required, number)

## AgencyData (object)
+ `name` (required, string)
+ `abbreviation` (required, string)
+ `code` (required, string)
+ `current_total_budget_authority_amount` (required, number)
+ `recent_publication_date` (required, string, nullable)
+ `recent_publication_date_certified` (required, boolean)
+ `tas_account_discrepancies_totals` (required, array[TASTotals], fixed-type)
+ `obligation_difference` (required, number)
    The difference in file A and file B obligations.

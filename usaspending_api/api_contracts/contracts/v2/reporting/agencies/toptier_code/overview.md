FORMAT: 1A
HOST: https://api.usaspending.gov

# Agency Reporting Overview [/api/v2/reporting/agencies/{toptier_code}/overview/{?page,limit,order,sort}]

This endpoint is used to power USAspending.gov's About the Data \| Agencies agency details table.

## GET

This endpoint returns an overview of government agency submission data.

+ Parameters
    + `toptier_code`: `020` (required, string)
        The specific agency's toptier code.
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
            + `tas_accounts_total`
            + `obligation_difference`
            + `percent_of_total_budgetary_resources`
            + `recent_publication_date`
            + `recent_publication_date_certified`
            + `tas_obligation_not_in_gtas_total`
            + `unlinked_contract_award_count`
            + `unlinked_assistance_award_count`

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
                        "fiscal_period": 7,
                        "current_total_budget_authority_amount": 192340342676.75,
                        "total_budgetary_resources": 13105812389531.01,
                        "percent_of_total_budgetary_resources": 1.47,
                        "recent_publication_date": "2020-08-13T13:40:21.181994Z",
                        "recent_publication_date_certified": true,
                        "tas_account_discrepancies_totals": {
                            "gtas_obligation_total": 62668762422.57,
                            "tas_accounts_total": 62659366681.07,
                            "tas_obligation_not_in_gtas_total": 9395741.5,
                            "missing_tas_accounts_count": 9
                        },
                        "obligation_difference": 12581114.45,
                        "unlinked_contract_award_count": 2,
                        "unlinked_assistance_award_count": 5,
                        "assurance_statement_url": "https://files.usaspending.gov/agency_submissions/Raw%20DATA%20Act%20Files/2020/P07/020%20-%20Department%20of%20the%20Treasury%20(TREAS)/2020-P07-020_Department%20of%20the%20Treasury%20(TREAS)-Assurance_Statement.txt"
                    },
                    {
                        "fiscal_year": 2020,
                        "fiscal_period": 6,
                        "current_total_budget_authority_amount": 192243648186.87,
                        "total_budgetary_resources": 10127796351165.57,
                        "percent_of_total_budgetary_resources": 1.90,
                        "recent_publication_date": "2020-05-14T13:08:23.624634Z",
                        "recent_publication_date_certified": true,
                        "tas_account_discrepancies_totals": {
                            "gtas_obligation_total": 42417596510.72,
                            "tas_accounts_total": 42417271495.34,
                            "tas_obligation_not_in_gtas_total": 325015.38,
                            "missing_tas_accounts_count": 2
                        },
                        "obligation_difference": 14702670.23,
                        "unlinked_contract_award_count": 0,
                        "unlinked_assistance_award_count": 0,
                        "assurance_statement_url": "https://files-nonprod.usaspending.gov/agency_submissions/Raw%20DATA%20Act%20Files/2020/Q2/020%20-%20Department%20of%20the%20Treasury%20(TREAS)/2020-Q2-020_Department%20of%20the%20Treasury%20(TREAS)-Assurance_Statement.txt"
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
+ `gtas_obligation_total` (required, number, nullable)
+ `tas_accounts_total` (required, number, nullable)
+ `tas_obligation_not_in_gtas_total` (required, number, nullable)
+ `missing_tas_accounts_count` (required, number, nullable)

## AgencyData (object)
+ `fiscal_year` (required, number)
+ `fiscal_period` (required, number)
+ `current_total_budget_authority_amount` (required, number, nullable)
+ `total_budgetary_resources` (required, number, nullable)
+ `percent_of_total_budgetary_resources` (required, number, nullable)
+ `recent_publication_date` (required, string, nullable)
+ `recent_publication_date_certified` (required, boolean)
+ `tas_account_discrepancies_totals` (required, array[TASTotals], fixed-type)
+ `obligation_difference` (required, number, nullable)
    The difference in file A and file B obligations.
+ `unlinked_contract_award_count` (required, number, nullable)
+ `unlinked_assistance_award_count` (required, number, nullable)
+ `assurance_statement_url` (required, string, nullable)
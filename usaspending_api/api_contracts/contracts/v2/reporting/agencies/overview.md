FORMAT: 1A
HOST: https://api.usaspending.gov

# Agencies Reporting Overview [/api/v2/reporting/agencies/overview/{?fiscal_year,fiscal_period,filter,page,limit,order,sort}]

This endpoint is used to power USAspending.gov's About the Data \| Agencies Overview table. This data can be used to better understand the ways agencies submit data.

## GET

This endpoint returns an overview list of government agencies submission data.

+ Parameters

    + `fiscal_year`: 2020 (required, number)
        The fiscal year.
    + `fiscal_period`: 10 (required, number)
        The fiscal period. Valid values: 2-12 (2 = November ... 12 = September)
        For retriving quarterly data, provide the period which equals 'quarter * 3' (e.g. Q2 = P6)
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
            + `toptier_code`
            + `current_total_budget_authority_amount`
            + `tas_accounts_total`
            + `missing_tas_accounts_count`
            + `agency_name`
            + `obligation_difference`
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
                        "agency_id": 123,
                        "current_total_budget_authority_amount": 8361447130497.72,
                        "recent_publication_date": "2020-01-10T11:59:21Z",
                        "recent_publication_date_certified": false,
                        "tas_account_discrepancies_totals": {
                            "gtas_obligation_total": 55234,
                            "tas_accounts_total": 23923,
                            "tas_obligation_not_in_gtas_total": 11543,
                            "missing_tas_accounts_count": 20
                        },
                        "obligation_difference": 436376232652.87,
                        "unlinked_contract_award_count": 3,
                        "unlinked_assistance_award_count": 2,
                        "assurance_statement_url": "https://files-nonprod.usaspending.gov/agency_submissions/Raw%20DATA%20Act%20Files/2020/P09/075%20-%20Department%20of%20Health%20and%20Human%20Services%20(HHS)/2020-P09-075_Department%20of%20Health%20and%20Human%20Services%20(HHS)-Assurance_Statement.txt"
                    },
                    {
                        "agency_name": "Department of Treasury",
                        "abbreviation": "DOT",
                        "toptier_code": "021",
                        "agency_id": 789,
                        "current_total_budget_authority_amount": 8361447130497.72,
                        "recent_publication_date": null,
                        "recent_publication_date_certified": true,
                        "tas_account_discrepancies_totals": {
                            "gtas_obligation_total": 66432,
                            "tas_accounts_total": 23913,
                            "tas_obligation_not_in_gtas_total": 11543,
                            "missing_tas_accounts_count": 10
                        },
                        "obligation_difference": 436376232652.87,
                        "unlinked_contract_award_count": 0,
                        "unlinked_assistance_award_count": 0,
                        "assurance_statement_url": "https://files-nonprod.usaspending.gov/agency_submissions/Raw%20DATA%20Act%20Files/2020/P09/020%20-%20Department%20of%20the%20Treasury%20(TREAS)/2020-P09-020_Department%20of%20the%20Treasury%20(TREAS)-Assurance_Statement.txt"
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
+ `agency_name` (required, string)
+ `abbreviation` (required, string)
+ `toptier_code` (required, string)
+ `agency_id` (required, number, nullable)
+ `current_total_budget_authority_amount` (required, number, nullable)
+ `recent_publication_date` (required, string, nullable)
+ `recent_publication_date_certified` (required, boolean)
+ `tas_account_discrepancies_totals` (required, array[TASTotals], fixed-type)
+ `obligation_difference` (required, number, nullable)
    The difference in File A and File B obligations.
+ `unlinked_contract_award_count` (required, number, nullable)
+ `unlinked_assistance_award_count` (required, number, nullable)
+ `assurance_statement_url` (required, string, nullable)
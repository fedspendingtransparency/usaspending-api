FORMAT: 1A
HOST: https://api.usaspending.gov

# Agency Profile

These endpoints are used to power USAspending.gov's agency profile pages. This data can be used to better understand the different ways that a specific agency spends money.

# Group Landing Page

These endpoints support the Agency Profile landing page that lists all available toptier agencies that have profile pages on USAspending.gov.

## Toptier Agency List [/api/v2/references/toptier_agencies/{?sort,order}]

This endpoint returns a list of toptier agencies, their budgetary resources, and and the percent of the total government budget authority this agency accounts for.

+ Parameters
    + sort: percentage_of_total_budget_authority (optional, string) - A data field that will be used to sort the response array.
    + order: desc (optional, string) - The direction (`asc` or `desc`) that the `sort` field will be sorted in.

### Get Toptier Agencies [GET]

+ Response 200 (application/json)
    + Attributes (object)
        + results (required, array[ListAgencyObject], fixed-type)

## Data Structures

### ListAgencyObject (object)

+ agency_id: 456 (required, number)
    This is the unique USAspending.gov identifier for the agency. You will need to use this ID in other endpoints when requesting detailed information about this specific agency.
+ agency_name: Department of the Treasury (required, string)
+ abbreviation: TREAS (optional, string)
+ budget_authority_amount: 1899160740172.16 (required, number)
+ percentage_of_total_budget_authority: 0.22713302022147824 (required, number)
    `percentage_of_total_budget_authority` is the percentage of the agency's budget authority compared to the total government budget authority, expressed as a decimal value.

# Group Profile Page

These endpoints support the individual Agency Profile pages that display data for a specific agency.

## Agency Overview [/api/v2/references/agency/{id}/]

This endpoint returns a high-level overview of a specific government agency, given its USAspending.gov `id`.

+ Parameters
    + id: 456 (required, number)
        The unique USAspending.gov agency identifier. This ID is the `agency_id` value returned in the `/api/v2/references/toptier_agencies/` endpoint.

### Get Agency Overview [GET]

+ Response 200 (application/json)
    + Attributes
        + results (required, AgencyOverview)

## Financial Balances [/api/v2/financial_balances/agencies/{?fiscal_year,funding_agency_id}]

This endpoint returns aggregated balances for a specific government agency in a given fiscal year.

+ Parameters
    + fiscal_year: 2017 (required, number)
        The fiscal year that you are querying data for.
    + funding_agency_id: 456 (required, number)
        The unique USAspending.gov agency identifier. This ID is the `agency_id` value returned in the `/api/v2/references/toptier_agencies/` endpoint.

### Get Financial Balances [GET]

+ Response 200 (application/json)
    + Attributes (object)
        + results (required, array[FinancialBalance], fixed-type)

## Major Object Classes [/api/v2/financial_spending/major_object_class/{?fiscal_year,funding_agency_id}]

This endpoint returns the total amount that a specific agency has obligated to major object classes in a given fiscal year.

+ Parameters
    + fiscal_year: 2017 (required, number)
        The fiscal year that you are querying data for.
    + funding_agency_id: 456 (required, number)
        The unique USAspending.gov agency identifier. This ID is the `agency_id` value returned in the `/api/v2/references/toptier_agencies/` endpoint.

### Get Major Object Classes [GET]

+ Response 200 (application/json)
    + Attributes
        + results (required, array[MajorObjectClass], fixed-type)

## Major Object Classes [/api/v2/financial_spending/object_class/{?fiscal_year,funding_agency_id,major_object_class_code}]

This endpoint returns the total amount that a specific agency has obligated to minor object classes within a specific major object class in a given fiscal year.

+ Parameters
    + fiscal_year: 2017 (required, number)
        The fiscal year that you are querying data for.
    + funding_agency_id: 456 (required, number)
        The unique USAspending.gov agency identifier. This ID is the `agency_id` value returned in the `/api/v2/references/toptier_agencies/` endpoint.
    + major_object_class_code: 30 (required, number)
        The major object class code returned in `/api/v2/financial_spending/major_object_class/`.

### Get Minor Object Classes [GET]

+ Response 200 (application/json)
    + Attributes (object)
        + results (required, array[MinorObjectClass], fixed-type)

## Federal Accounts [/api/v2/federal_obligations/{?fiscal_year,funding_agency_id,limit,page}]

This endpoint returns the amount that the specific agency has obligated to various federal accounts in a given fiscal year.

+ Parameters
    + fiscal_year: 2017 (required, number)
        The fiscal year that you are querying data for.
    + funding_agency_id: 456 (required, number)
        The unique USAspending.gov agency identifier. This ID is the `agency_id` value returned in the `/api/v2/references/toptier_agencies/` endpoint.
    + limit: 10 (optional, number)
        The maximum number of results to return in the response.
    + page: 1 (optional, number)
        The response page to return (the record offset is (`page` - 1) * `limit`).

### Get Federal Accounts [GET]

+ Response 200 (application/json)
    + Attributes
        + results (required, array[FederalAccount], fixed-type)
        + page_metadata (required, PageMetadataObject)

# Data Structures

## PageMetadataObject (object)
+ page: 1 (required, number)
+ hasNext: false (required, boolean)
+ hasPrevious: false (required, boolean)

## AgencyOverview (object)
+ active_fy: `2017` (required, string)
    The fiscal year that the data in the response reflects.
+ active_fq: `2` (required, string)
    The latest quarter of the `active_fy` that the data in the response reflects.
+ agency_name: Department of the Treasury (required, string)
+ mission: `Maintain a strong economy and create economic and job opportunities by promoting the conditions that enable economic growth and stability at home and abroad, strengthen national security by combating threats and protecting the integrity of the financial system, and manage the U.S. Government's finances and resources effectively.` (required, string)
    The agency's mission statement.
+ icon_filename: `DOT.jpg` (required, string)
    The file name of the agency's logo. If no logo is available, this will be an empty string. Images can be found at `https://www.usaspending.gov/graphics/agency/[file]`.
+ website: `https://www.treasury.gov` (required, string)
+ budget_authority_amount: 1899160740172.16 (required, string)
+ current_total_budget_authority_amount: 8361447130497.72 (required, string)
+ obligated_amount: 524341511584.82 (required, string)
+ outlay_amount: 523146830716.62 (required, string)

## FinancialBalance (object)
+ budget_authority_amount: 1899160740172.16 (required, string)
+ obligated_amount: 524341511584.82 (required, string)
+ outlay_amount: 523146830716.62 (required, string)


## MajorObjectClass (object)
+ major_object_class_code: 30 (required, string)
+ major_object_class_name: Acquisition of assets (required, string)
+ obligated_amount: 216067467.29 (required, string)

## MinorObjectClass (object)
+ object_class_code: 310 (required, string)
+ object_class_name: Equipment (required, string)
+ obligated_amount: 209728763.65 (required, string)

## FederalAccount (object)
+ account_title: `Refunding Internal Revenue Collections (Indefinite), Treasury` (required, string)
+ id: 1105 (required, string)
    The USAspending.gov unique identifier for the federal account. You will need to use this ID when making API requests for details about specific federal accounts.
+ obligated_amount: 185853348344.60 (required, string)

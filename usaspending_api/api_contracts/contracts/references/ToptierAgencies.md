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

## PageMetadataObject (object)
+ count (required, number)
+ page (required, number)
+ has_next_page (required, boolean)
+ has_previous_page (required, boolean)
+ next (required, nullable , string)
+ current (required, string)
+ previous (required, nullable, string)

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
+ account_title (required, string)
+ account_number (required, string)
+ id (required, string)
    The USAspending.gov unique identifier for the federal account. You will need to use this ID when making API requests for details about specific federal accounts.
+ obligated_amount (required, string)

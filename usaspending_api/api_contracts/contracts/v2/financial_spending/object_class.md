FORMAT: 1A
HOST: https://api.usaspending.gov

# Minor Object Classes [/api/v2/financial_spending/object_class/{?fiscal_year,funding_agency_id,major_object_class_code}]

These endpoints are used to power USAspending.gov's agency profile pages. This data can be used to better understand the different ways that a specific agency spends money.

## GET

This endpoint returns the total amount that a specific agency has obligated to minor object classes within a specific major object class in a given fiscal year.

+ Parameters

    + `fiscal_year`: 2017 (required, number)
        The fiscal year that you are querying data for.
    + `funding_agency_id`: 456 (required, number)
        The unique USAspending.gov agency identifier. This ID is the `agency_id` value returned in the `/api/v2/references/toptier_agencies/` endpoint.
    + `major_object_class_code`: 30 (required, number)
        The major object class code returned in `/api/v2/financial_spending/major_object_class/`.
        
+ Response 200 (application/json)

    + Attributes (object)
        + `results` (required, array[MinorObjectClass], fixed-type)

# Data Structures

## PageMetadataObject (object)
+ `count` (required, number)
+ `page` (required, number)
+ `has_next_page` (required, boolean)
+ `has_previous_page` (required, boolean)
+ `next` (required, nullable , string)
+ `current` (required, string)
+ `previous` (required, nullable, string)

## AgencyOverview (object)
+ `active_fy`: `2017` (required, string)
    The fiscal year that the data in the response reflects.
+ `active_fq`: `2` (required, string)
    The latest quarter of the `active_fy` that the data in the response reflects.
+ `agency_name`: `Department of the Treasury` (required, string)
+ `mission`: `Maintain a strong economy and create economic and job opportunities by promoting the conditions that enable economic growth and stability at home and abroad, strengthen national security by combating threats and protecting the integrity of the financial system, and manage the U.S. Government's finances and resources effectively.` (required, string)
    The agency's mission statement.
+ `icon_filename`: `DOT.jpg` (required, string)
    The file name of the agency's logo. If no logo is available, this will be an empty string. Images can be found at `https://www.usaspending.gov/graphics/agency/[file]`.
+ `website`: `https://www.treasury.gov` (required, string)
+ `budget_authority_amount`: `1899160740172.16` (required, string)
+ `current_total_budget_authority_amount`: `8361447130497.72` (required, string)
+ `obligated_amount`: `524341511584.82` (required, string)
+ `outlay_amount`: `523146830716.62` (required, string)

## MinorObjectClass (object)
+ `object_class_code`: `310` (required, string)
+ `object_class_name`: `Equipment` (required, string)
+ `obligated_amount`: `209728763.65` (required, string)

## FederalAccount (object)
+ `account_title` (required, string)
+ `account_number` (required, string)
+ `id` (required, string)
    The USAspending.gov unique identifier for the federal account. You will need to use this ID when making API requests for details about specific federal accounts.
+ `obligated_amount` (required, string)

FORMAT: 1A
HOST: https://api.usaspending.gov

# Toptier Agency Profile [/api/v2/references/toptier_agencies/{?sort,order}]

These endpoints are used to power USAspending.gov's agency profile pages. This data can be used to better understand the different ways that a specific agency spends money.

## GET

This endpoint returns a list of toptier agencies, their budgetary resources, and and the percent of the total government budget authority this agency accounts for.

+ Parameters
    + `sort`: `percentage_of_total_budget_authority` (optional, string) - A data field that will be used to sort the response array.
    + `order`: `desc` (optional, string) - The direction (`asc` or `desc`) that the `sort` field will be sorted in.
    
+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, array[ListAgencyObject], fixed-type)

## Data Structures

### ListAgencyObject (object)

+ `abbreviation` (required, string)
+ `active_fq` (required, string)
+ `active_fy` (required, string)
+ `agency_id` (required, number)
    `agency_id` is the unique USAspending.gov identifier for the agency. You will need to use this ID in other endpoints when requesting detailed information about this specific agency.
+ `agency_name` (required, string)
+ `budget_authority_amount` (required, number)
+ `congressional_justification_url` (required, string, nullable)
+ `current_total_budget_authority_amount` (required, number)
+ `obligated_amount` (required, number)
+ `outlay_amount` (required, number)
+ `percentage_of_total_budget_authority` (required, number)
    `percentage_of_total_budget_authority` is the percentage of the agency's budget authority compared to the total government budget authority, expressed as a decimal value.
+ `toptier_code` (required, string)
+ `agency_slug` (required, string) is the name of the agency in lowercase with dashes to be used for profile link construction

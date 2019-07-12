FORMAT: 1A
HOST: https://api.usaspending.gov

# Agency Profile

These endpoints are used to power USAspending.gov's agency profile pages. This data can be used to better understand the different ways that a specific agency spends money.

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

# Data Structures

## AgencyOverview (object)
+ `active_fy` (required, string)
    The fiscal year that the data in the response reflects.
+ `active_fq` (required, string)
    The latest quarter of the `active_fy` that the data in the response reflects.
+ `agency_name` (required, string)
+ `mission` (required, string)
    The agency's mission statement.
+ `icon_filename` (required, string)
    The file name of the agency's logo. If no logo is available, this will be an empty string. Images can be found at `https://www.usaspending.gov/graphics/agency/[file]`.
+ `website` (required, string)
+ `congressional_justification_url` (required, nullable, string)
+ `budget_authority_amount` (required, string)
+ `current_total_budget_authority_amount` (required, string)
+ `obligated_amount` (required, string)
+ `outlay_amount` (required, string)

FORMAT: 1A
HOST: https://api.usaspending.gov

# Agency Overview [/api/v2/references/agency/{id}/]

These endpoints are used to power USAspending.gov's agency profile pages. This data can be used to better understand the different ways that a specific agency spends money.

## GET

This endpoint returns a high-level overview of a specific government agency, given its USAspending.gov `id`.

+ Parameters

    + `id`: 456 (required, number)
        The unique USAspending.gov agency identifier. This ID is the `agency_id` value returned in the `/api/v2/references/toptier_agencies/` endpoint.

+ Response 200 (application/json)

    + Attributes
        + `results` (required, object)
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

    + Body

            {
                "results": {
                    "agency_name": "Department of Health and Human Services",
                    "active_fy": "2020",
                    "active_fq": "1",
                    "outlay_amount": "436376232652.87",
                    "obligated_amount": "452507150028.76",
                    "budget_authority_amount": "1958993691342.83",
                    "current_total_budget_authority_amount": "8361447130497.72",
                    "mission": "It is the mission of the U.S. Department of Health & Human Services (HHS) to enhance and protect the health and well-being of all Americans. We fulfill that mission by providing for effective health and human services and fostering advances in medicine, public health, and social services.",
                    "website": "https://www.hhs.gov/",
                    "icon_filename": "DHHS.jpg",
                    "congressional_justification_url": "https://www.hhs.gov/cj"
                }
            }
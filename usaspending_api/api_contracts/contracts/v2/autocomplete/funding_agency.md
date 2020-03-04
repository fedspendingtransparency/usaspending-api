FORMAT: 1A
HOST: https://api.usaspending.gov

# Funding Agency Autocomplete [/api/v2/autocomplete/funding_agency/]

This endpoint is used by the Funding Agency autocomplete filter on the Advanced Search page.

## POST

This route sends a request to the backend to retrieve funding agencies matching the specified search text.  Agencies are returned in the order
they are expected to be displayed.

+ Request (application/json)
    + Attributes (object)
        + `limit` (optional, number)
            + Default: 10
        + `search_text` (required, string)
    + Body

            {
                "search_text": "Defense"
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, array[FundingAgencyMatch], fixed-type)

# Data Structures

## FundingAgencyMatch (object)
+ `id` (optional, number)
+ `toptier_flag` (required, boolean)
+ `toptier_agency` (required, object)
    + `id` (required, number)
    + `toptier_code` (required, string)
    + `abbreviation` (required, string, nullable)
    + `name` (required, string)
+ `subtier_agency` (required, object)
    + `abbreviation` (required, string, nullable)
    + `name` (required, string)

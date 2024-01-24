FORMAT: 1A
HOST: https://api.usaspending.gov

# Funding Agency Autocomplete [/api/v2/autocomplete/funding_agency/]

*Deprecated: Please see the following API contract for the new funding endpoint [usaspending_api/api_contracts/contracts/v2/autocomplete/funding_agency_office.md](./funding_agency_office.md).*


This endpoint is used by the Funding Agency autocomplete filter on the Advanced Search page.

## POST

This route sends a request to the backend to retrieve funding agencies matching the specified search text.

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

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
        + `messages` (required, array[string], fixed-type)
        An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.

# Data Structures

## FundingAgencyMatch (object)
+ `id` (optional, number)
+ `toptier_flag` (required, boolean)
+ `toptier_agency` (required, object)
    + `toptier_code` (required, string)
    + `abbreviation` (required, string, nullable)
    + `name` (required, string)
+ `subtier_agency` (required, object)
    + `abbreviation` (required, string, nullable)
    + `name` (required, string)

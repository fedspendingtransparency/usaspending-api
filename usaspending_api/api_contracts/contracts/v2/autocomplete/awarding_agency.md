FORMAT: 1A
HOST: https://api.usaspending.gov

# Awarding Agency Autocomplete [/api/v2/autocomplete/awarding_agency/]

*Deprecated: Please see the following API contract for the new awarding and funding endpoints (respectively) [usaspending_api/api_contracts/contracts/v2/autocomplete/awarding_agency_office.md](./awarding_agency_office.md) and [usaspending_api/api_contracts/contracts/v2/autocomplete/funding_agency_office.md](./funding_agency_office.md)*


This endpoint is used by the Awarding Agency autocomplete filter on the Advanced Search page.

## POST

This route sends a request to the backend to retrieve awarding agencies matching the specified search text.

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes (object)
        + `limit` (optional, number)
        + `search_text` (required, string)
    + Body

            {
                "search_text": "Defense"
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, array[AwardingAgencyMatchObject], fixed-type)

# Data Structures

## AwardingAgencyMatchObject (object)
+ `id` (required, number)
+ `toptier_flag` (required, boolean)
+ `toptier_agency` (required, object)
    + `toptier_code` (required, string)
    + `abbreviation` (required, string, nullable)
    + `name` (required, string)
+ `subtier_agency` (required, object)
    + `abbreviation` (required, string, nullable)
    + `name` (required, string)

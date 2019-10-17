FORMAT: 1A
HOST: https://api.usaspending.gov

# Awarding Agency Autocomplete [/api/v2/autocomplete/awarding_agency/]

This endpoint is used by the Advanced Search page.

## POST

This route sends a request to the backend to retrieve awarding agencies matching the specified search text.        

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
        + `results` (required, array[AwardingAgencyMatchObject], fixed-type)

# Data Structures

## AwardingAgencyMatchObject (object)
+ `id` (required, number)
+ `subtier_agency` (required, object)
    + `abbreviation` (required, string, nullable)
    + `name` (required, string)
    + `subtier_code` (required, string)
+ `toptier_agency` (required, object)
    + `abbreviation` (required, string, nullable)
    + `name` (required, string)
    + `cgac_code` (required, string)
+ `toptier_flag` (required, boolean)

FORMAT: 1A
HOST: https://api.usaspending.gov

# Awarding Agency and Office Autocomplete [/api/v2/autocomplete/awarding_agency_office/]

This endpoint can be used to autocomplete Awarding Agency and Office searches on the Advanced Search page. It will return
any agencies, sub-agencies, and offices that match the search text. Additionally, any matching agencies will include associated
offices and sub-agencies, any matching sub-agencies will include associated agencies and offices, and any matching offices will include
associated agencies and sub-agencies.

## POST

This route sends a request to the backend to retrieve awarding agencies and offices matching the specified search text.

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
        + `results` (required, array[AwardingAgencyOfficeMatchObject], fixed-type)

# Data Structures

## AwardingAgencyOfficeMatchObject (object)
+ `toptier_agency` (required, object)
    + `toptier_code` (required, string)
    + `abbreviation` (required, string, nullable)
    + `name` (required, string)
    + `subtier_agency` (required, array[SubAgencyChildObject])
    + `office` (required, array[OfficeChildObject])
+ `subtier_agency` (required, object)
    + `abbreviation` (required, string, nullable)
    + `name` (required, string)
    + `toptier_agency` (required, array[AgencyChildObject])
    + `office` (required, array[OfficeChildObject])
+ `office` (required, object)
    + `code` (required, string, nullable)
    + `name` (required, string)
    + `toptier_agency` (required, array[AgencyChildObject])
    + `subtier_agency` (required, array[SubAgencyChildObject])

## SubAgencyChildObject (object)
+ `id` (required, number)
+ `abbreviation` (required, string, nullable)
+ `name` (required, string)

## AgencyChildObject (object)
+ `id` (required, number)
+ `toptier_code` (required, string)
+ `abbreviation` (required, string, nullable)
+ `name` (required, string)

## OfficeChildObject (object)
+ `id` (required, number)
+ `code` (required, string, nullable)
+ `name` (required, string)
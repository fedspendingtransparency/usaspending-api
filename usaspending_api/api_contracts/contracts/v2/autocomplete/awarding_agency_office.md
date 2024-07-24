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
            Search text can be an office or agency name, or it can be an agency abbreviation. For example, try searching for DoD and Department of Defence.
    + Body

            {
                "search_text": "Defense"
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, array[AwardingAgencyOfficeMatchObject], fixed-type)
        + `messages` (required, array[string], fixed-type)
        An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.

# Data Structures

## AwardingAgencyOfficeMatchObject (object)
+ `toptier_agency` (required, object)
    + `abbreviation` (required, string, nullable)
    + `code` (required, string)
    + `name` (required, string)
    + `subtier_agencies` (required, array[SubAgenciesChildObject])
    + `offices` (required, array[OfficesChildObject])
+ `subtier_agency` (required, object)
    + `abbreviation` (required, string, nullable)
    + `code` (required, string)
    + `name` (required, string)
    + `toptier_agency` (required, AgenciesChildObject)
    + `offices` (required, array[OfficesChildObject])
+ `office` (required, object)
    + `code` (required, string, nullable)
    + `name` (required, string)
    + `toptier_agency` (required, AgenciesChildObject)
    + `subtier_agency` (required, SubAgenciesChildObject)

## SubAgenciesChildObject (object)
+ `abbreviation` (required, string, nullable)
+ `code` (required, string, nullable)
+ `name` (required, string)

## AgenciesChildObject (object)
+ `abbreviation` (required, string, nullable)
+ `code` (required, string)
+ `name` (required, string)

## OfficesChildObject (object)
+ `code` (required, string)
+ `name` (required, string)
FORMAT: 1A
HOST: https://api.usaspending.gov

# Funding Agency and Office Autocomplete [/api/v2/autocomplete/funding_agency_office/]

This endpoint can be used to autocomplete Funding Agency and Office searches on the Advanced Search page. It will return
any agencies, sub-agencies, and offices that match the search text. Additionally, any matching agencies will include associated
offices and sub-agencies, any matching sub-agencies will include associated agencies and offices, and any matching offices will include
associated agencies and sub-agencies.

## POST

This route sends a request to the backend to retrieve funding agencies and offices matching the specified search text.

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
        + `results` (required, array[FundingAgencyOfficeMatchObject], fixed-type)

# Data Structures

## FundingAgencyOfficeMatchObject (object)
+ `toptier_agency` (required, object)
    + `toptier_code` (required, string)
    + `abbreviation` (required, string, nullable)
    + `name` (required, string)
    + `subtier_agencies` (required, array[SubAgenciesChildObject])
    + `offices` (required, array[OfficesChildObject])
+ `subtier_agency` (required, object)
    + `id` (required, number)
    + `abbreviation` (required, string, nullable)
    + `name` (required, string)
    + `toptier_agencies` (required, array[AgenciesChildObject])
    + `offices` (required, array[OfficesChildObject])
+ `office` (required, object)
    + `id` (required, number)
    + `code` (required, string, nullable)
    + `name` (required, string)
    + `toptier_agencies` (required, array[AgenciesChildObject])
    + `subtier_agencies` (required, array[SubAgenciesChildObject])

## SubAgenciesChildObject (object)
+ `id` (required, number)
+ `abbreviation` (required, string, nullable)
+ `name` (required, string)

## AgenciesChildObject (object)
+ `id` (required, number)
+ `toptier_code` (required, string)
+ `abbreviation` (required, string, nullable)
+ `name` (required, string)

## OfficesChildObject (object)
+ `id` (required, number)
+ `code` (required, string, nullable)
+ `name` (required, string)
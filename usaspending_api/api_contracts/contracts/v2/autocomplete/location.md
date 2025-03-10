FORMAT: 1A
HOST: https://api.usaspending.gov

# Location Autocomplete [/api/v2/autocomplete/location/]

This endpoint is used by the Location autocomplete filter on the Advanced Search page.

## POST

This route sends a request to the backend to retrieve locations matching the specified search text.

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
                "search_text": "Den"
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, Locations, fixed-type)
        + `messages` (required, array[string], fixed-type) An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.
    + Body

            {
                "results": {
                    "countries": [
                        "Denmark"
                        "Sweden"
                    ],
                    "cities": [
                        "Denver, Colorado, United States",
                        "Gadsden, Alabama, United States",
                        "Camden, Arkansas, United States"
                    ],
                    "counties": [
                        {
                            "county_fips": "12345",
                            "county_name": "DENVER COUNTY, COLORADO, UNITED STATES"
                        },
                        {
                            "county_fips": "67890",
                            "county_name": "CAMDEN COUNTY, GEORGIA, UNITED STATES"
                        }
                    ]
                },
                "messages": [""]
            }

# Data Structures

## Locations (object)
+ `countries` (optional, array[string])
+ `states` (optional, array[string])
+ `cities` (optional, array[string])
+ `counties` (optional, array[CountyMatch])
+ `zip_codes` (optional, array[string])
+ `districts_original` (optional, array[string])
+ `districts_current` (optional, array[string])

## CountyMatch (object)
+ `county_fips` (required, string) The 5 digit FIPS code (2 digit state FIPS code + 3 digit county FIPS code)
+ `county_name` (required, string)

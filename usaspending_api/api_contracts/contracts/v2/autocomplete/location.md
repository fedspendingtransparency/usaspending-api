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
            + Default: 5
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
                        {
                            "country_name": "Denmark"
                        },
                        {
                            "country_name": "Sweden"
                        }
                    ],
                    "cities": [
                        {
                            "city_name": "Denver",
                            "state_name": "Colorado",
                            "country_name": "United States"
                        },
                        {
                            "city_name": "Gadsden",
                            "state_name": "Alabama",
                            "country_name": "United States"
                        },
                        {
                            "city_name": "Camden",
                            "state_name": "Arkansas",
                            "country_name": "United States"
                        }
                    ]
                },
                "messages": [""]
            }

# Data Structures

## Locations (object)
+ `countries` (optional, array[CountryMatch])
+ `states` (optional, array[StateMatch])
+ `cities` (optional, array[CityMatch])
+ `counties` (optional, array[CountyMatch])
+ `zip_codes` (optional, array[ZipCodeMatch])
+ `districts_original` (optional, array[OriginalCongressionalDistrictMatch])
+ `districts_current` (optional, array[CurrentCongressionalDistrictMatch])

## CountryMatch (object)
+ `country_name` (required, string)

## StateMatch (object)
+ `state_name` (required, string)
+ `country_name` (required, string)

## CityMatch (object)
+ `city_name` (required, string)
+ `state_name` (required, string, nullable)
    This is `NULL` for foreign cities.
+ `country_name` (required, string)

## CountyMatch (object)
+ `county_fips` (required, string)
    The 5 digit FIPS code (2 digit state FIPS code + 3 digit county FIPS code)
+ `county_name` (required, string)
+ `state_name` (required, string)
+ `country_name` (required, string)

## ZipCodeMatch (object)
+ `zip_code` (required, number)
    The 5 digit zip code
+ `state_name` (required, string)
+ `country_name` (required, string)

## OriginalCongressionalDistrictMatch (object)
+ `original_cd` (required, string)
+ `state_name` (required, string)
+ `country_name` (required, string)

## CurrentCongressionalDistrictMatch (object)
+ `current_cd` (required, string)
+ `state_name` (required, string)
+ `country_name` (required, string)

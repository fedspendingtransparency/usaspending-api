FORMAT: 1A
HOST: https://api.usaspending.gov

# Advanced Search City Autocomplete [/api/v2/autocomplete/city/]

This end point returns a list of cities for a given limit, country, search string, and optional state code.

## POST

List of cities matching search criteria

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes (object)
        + `search_text` (required, string)
        + `limit` (required, number)
        + `filter` (optional, object)
            + `country_code` (required, string)
            + `scope` (required, enum[string])
                + Members
                    + `primary_place_of_performance`
                    + `recipient_location`
            + `state_code` (optional, string)
    + Body

            {
                "search_text": "Springfield",
                "limit": 40,
                "filter": {
                    "country_code": "USA",
                    "scope": "recipient_location",
                    "state_code": "VA"
                }
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `count`(required, number)
        + `results` (required, array[CityMatch], fixed-type)

# Data Structures

## CityMatch (object)
+ `city_name` (required, string)
+ `state_code` (required, string, nullable)

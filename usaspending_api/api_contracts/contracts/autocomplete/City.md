FORMAT: 1A
HOST: https://api.usaspending.gov

# City

This end point returns a list of cities for a given limit, country, search string, and optional state code.

## Advanced Search City Autocomplete [/api/v2/autocomplete/city/]

Returns a list of cities matching search criteria

### Advanced Search City Autocomplete [POST]

+ Request (application/json)
    + Attributes (object)
        + search_text: `Springfield` (required, string)
        + limit: 40 (required, number)
        + filter (object)
            + country_code: `USA` (required, string)
            + scope: `recipient_location` (required, enum[string])
                + `primary_place_of_performance`
                + `recipient_location`
            + state_code: `SC` (optional, string)

+ Response 200 (application/json)
    + Attributes (object)
        + count: 10 (required, number)
        + results (required, array[AutocompleteCityResult], fixed-type)

# Data Structures

## AutocompleteCityResult (object)
+ city_name: `Springfield` (required, string)
+ state_code: `VA` (required, string, nullable)

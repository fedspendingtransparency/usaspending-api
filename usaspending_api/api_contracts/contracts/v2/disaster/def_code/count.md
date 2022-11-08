FORMAT: 1A
HOST: https://api.usaspending.gov

# Count of DEF Codes Receiving Disaster/Emergency Funding [/api/v2/disaster/def_code/count/]

This endpoint provides the count of DEFC which received disaster/emergency funding per the requested filters.

## POST

This endpoint returns a count of DEFC

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes
        + `filter` (required, Filter, fixed-type)

    + Body

            {
                "filter": {
                    "def_codes": ["L", "M", "N"]
                }
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `count` (required, number)
    + Body

            {
                "count": 5
            }

# Data Structures

## Filter (object)
+ `def_codes` (required, array[DEFC], fixed-type)

## DEFC (enum[string])
List of Disaster Emergency Fund (DEF) Codes (DEFC) defined by legislation at the time of writing.
A list of current DEFC can be found [here.](https://files.usaspending.gov/reference_data/def_codes.csv)

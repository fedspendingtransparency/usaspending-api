FORMAT: 1A
HOST: https://api.usaspending.gov

# Short Endpoint Name [/api/v2/agency/test/]

Temproary test endpoint

## GET

Test Ednpoint to ensure the app is configured properly

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "string"
            }


+ Response 200 (application/json)
    + Attributes
        + `status` (required, string)

    + Body

            {
                "status": "up"
            }


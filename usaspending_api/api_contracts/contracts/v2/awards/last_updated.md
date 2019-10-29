FORMAT: 1A
HOST: https://api.usaspending.gov

# Awards Last Updated [/api/v2/awards/last_updated/]

## GET

This route sends a request to the backend to retrieve the last updated date for the Award data.
        
+ Response 200 (application/json)
    + Attributes
        + `last_updated` (required, string) An empty string if there is no Award data available to check against

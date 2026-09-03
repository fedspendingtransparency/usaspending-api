FORMAT: 1A
HOST: https://api.usaspending.gov

# States

This endpoint returns U.S. states, districts, and territories found in the `state_data` table. 
The table contains multiple entries per state; this endpoint returns only the latest entry for each FIPS code.

## GET
+ Response 200 (application/json)
  + Attributes (object)
    + `results` (required, array[State], fixed-type)
  
  + Body

    {
        "results": [
          {
              "fips": "01"
              "code": "AL",
              "name": "ALABAMA"
            },{
              "fips": "02"
              "code": "AK",
              "name": "ALASKA"
            },{
              "fips": "11"
              "code": "DC",
              "name": "DISTRICT OF COLUMBIA"
            },{
              "fips": "72"
              "code": "PR",
              "name": "PUERTO RICO"
            }
          ]
  }     

# Data Structures
## State (object)
+ `fips` (required, string)
    Two-digit FIPS code for the state, district or territory
+ `code` (required, string)
    Two-digit USPS state code
+ `name` (required, string)
    uppercase full name of the state, district or territory

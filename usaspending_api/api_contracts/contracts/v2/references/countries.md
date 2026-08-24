FORMAT: 1A
HOST: https://api.usaspending.gov

# Countries

This endpoint returns countries found in the `ref_country_code` table on USAspending.

## GET
+ Response 200 (application/json)
  + Attributes (object)
    + `results` (required, array[Country], fixed-type)
  
  + Body

            {
                "results": [
                  {
                      "code": "USA",
                      "name": "UNITED STATES"
                    },{
                      "code": "CAN",
                      "name": "CANADA"
                    },{
                      "code": "MEX",
                      "name": "MEXICO"
                    },{
                      "code": "GBR",
                      "name": "UNITED KINGDOM"
                    },{
                      "code": "FRA",
                      "name": "FRANCE"
                    }
                  ]
          }     

# Data Structures
## Country (object)
+ `code` (required, string)
    Three-character country code (ISO 3166-1 alpha-3 format)
+ `name` (required, string)
    Uppercase full name of the country

FORMAT: 1A
HOST: https://api.usaspending.gov

# Assistance Listing [/api/v2/references/assistance_listing/{CODE}?filter]

## GET

This endpoint returns a list of Tier 1 Assistance Listings which is the code (first two numbers of CFDA code), the count of Tier 2 children, and the Tier 2 children. Tier 2 
includes the full CFDA code and description.

+ Request
    + Parameters
        + `code`:  `code=11` (optional, number) Must be a two-digit number.
        + `filter`: `filter=11.00` (optional, string) This will filter the CFDAs by their description or code. 

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, array[AssistanceListingObject], fixed-type)
  
    + Body
  
            [
                {
                    "code": "11",
                    "description": null,
                    "count" 2,
                    "children": [
                        {
                            "code": "11.001",
                            "description": "Census Bureau Data Products"
                        },
                        {
                            "code": "11.002",
                            "description": "census Customer Services"
                        }
                    ]
                },
                {
                    "code": "16",
                    "description": null,
                    "count" 1,
                    "children": [
                        {
                            "code": "16.001",
                            "description": "Law Enforcement Assistance Narcotics and Dangerous Drugs Laboratory Analysis"
                        }
                    ]
                }
            ]

## Data Structures

### AssistanceListingObject (object)
+ `code` (required, string)
+ `description` (required, string)
+ `count` (optional, number)
+ `children` (optional, array[AssistanceListingObject], fixed-type)


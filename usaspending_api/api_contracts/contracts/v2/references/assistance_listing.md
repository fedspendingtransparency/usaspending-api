FORMAT: 1A
HOST: https://api.usaspending.gov

# Assistance Listing [/api/v2/references/assistance_listing/{code}?filter]

## GET

This endpoint returns a list of Tier 1 Assistance Listings which is the code (first two numbers of CFDA code), the count of Tier 2 children, and the Tier 2 children. Tier 2 
includes the full CFDA code and description.

+ Request
    + Parameters
        + `code`:  `code=11` (optional, number) Must be a two-digit number.
        + `filter`: `filter=11.00` (optional, string) This will filter the CFDAs by their description or code. 
        + `page`: `page=3` (optional, number) The page of results to return based on the limit.
        + `sort`: `sort=description` (optional, enum) `code` and `description` are the only valid inputs. The default is `code`.
        + `limit`: `limit=15` (optional, number) How many results appear on a page. The default is 10.
        + `order`: `order=asc` (optional, string) What order results are returned in. The default is descending.

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, array[AssistanceListingObject], fixed-type)
  
    + Body
  
            [
                {
                    "code": "98",
                    "description": null,
                    "count": 12
                },
                {
                    "code": "97",
                    "description": null,
                    "count": 147
                },
                {
                    "code": "96",
                    "description": null,
                    "count": 12
                },
                {
                    "code": "95",
                    "description": null,
                    "count": 9
                }
            ]

## Data Structures

### AssistanceListingObject (object)
+ `code` (required, string)
+ `description` (required, string)
+ `count` (optional, number) Only shows if a code is not provided
+ `children` (optional, array[AssistanceListingObject], fixed-type) Only shows if a filter is provided without a code

## PageMetadata (object)
+ `limit` (required, number)
+ `page` (required, number)
+ `next` (required, number, nullable)
+ `previous` (required, number, nullable)
+ `hasNext` (required, boolean)
+ `hasPrevious` (required, boolean)
+ `total` (required, number)


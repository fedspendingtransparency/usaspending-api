FORMAT: 1A
HOST: https://api.usaspending.gov

# CFDA Programs Spending Disaster/Emergency Funding via Loans [/api/v2/disaster/cfda/loans/]

This endpoint provides insights on the CFDA Programs' loans from disaster/emergency funding per the requested filters.

## POST

Returns loan spending details of CFDA Programs receiving supplemental funding budgetary resources

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes
        + `filter` (required, Filter, fixed-type)
        + `pagination` (optional, Pagination, fixed-type)

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, array[Result], fixed-type)
        + `page_metadata` (required, PageMetadata, fixed-type)


    + Body

            {
                "results": [
                    {
                        "id": "43",
                        "code": "090",
                        "description": "Description text of 090, for humans",
                        "children": [],
                        "count": 54,
                        "face_value_of_loan": 89.01,
                        "obligation": 45,
                        "outlay": 3244,
                        "resource_link": "https://beta.sam.gov/fal/25b529f3b5f94b6c939bc0ae8424ae6c/view"
                    },
                    {
                        "id": "41",
                        "code": "012",
                        "description": "Description text of 012, for humans",
                        "children": [],
                        "count": 2,
                        "face_value_of_loan": 50,
                        "obligation": 123,
                        "outlay": 3249,
                        "resource_link": null
                    }
                ],
                "page_metadata": {
                    "page": 1,
                    "next": 2,
                    "previous": null,
                    "hasNext": true,
                    "hasPrevious": false,
                    "total": 23,
                    "limit": 2
                }
            }

# Data Structures

## Filter (object)
+ `def_codes` (required, array[DEFC], fixed-type)
+ `award_type_codes` (optional, array[string], fixed-type)
    + Only accepts loan award type `07` or `08` in the array, since this endpoint is specific to loans
    + Default: `["07", "08"]`
    + Members
        + `07`
        + `08`
+ `query` (optional, string)
    A "keyword" or "search term" to filter down results based on this text snippet

## Pagination (object)
+ `page` (optional, number)
    Requested page of results
    + Default: 1
+ `limit` (optional, number)
    Page Size of results
    + Default: 10
+ `order` (optional, enum[string])
    Indicates what direction results should be sorted by. Valid options include asc for ascending order or desc for descending order.
    + Default: `desc`
    + Members
        + `desc`
        + `asc`
+ `sort` (optional, enum[string])
    Optional parameter indicating what value results should be sorted by
    + Default: `id`
    + Members
        + `id`
        + `code`
        + `description`
        + `count`
        + `face_value_of_loan`
        + `obligation`
        + `outlay`

## Result (object)
+ `id` (required, string)
+ `code` (required, string)
+ `description` (required, string)
+ `children` (optional, array[Result], fixed-type)
+ `count` (required, number)
+ `face_value_of_loan` (required, number, nullable)
+ `obligation` (required, number, nullable)
+ `outlay` (required, number, nullable)
+ `resource_link` (required, string, nullable)
    Link to an external website with more information about this result.


## PageMetadata (object)
+ `page` (required, number)
+ `next` (required, number, nullable)
+ `previous` (required, number, nullable)
+ `hasNext` (required, boolean)
+ `hasPrevious` (required, boolean)
+ `total` (required, number)
+ `limit` (required, number)

## DEFC (enum[string])
List of Disaster Emergency Fund (DEF) Codes (DEFC) defined by legislation at the time of writing

### Members
+ `A`
+ `B`
+ `C`
+ `D`
+ `E`
+ `F`
+ `G`
+ `H`
+ `I`
+ `J`
+ `K`
+ `L`
+ `M`
+ `N`
+ `O`
+ `P`
+ `Q`
+ `R`
+ `S`
+ `T`
+ `9`

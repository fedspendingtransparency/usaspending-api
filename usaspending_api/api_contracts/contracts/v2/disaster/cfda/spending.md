FORMAT: 1A
HOST: https://api.usaspending.gov

# CFDA Programs Spending Disaster/Emergency Funding [/api/v2/disaster/cfda/spending/]

This endpoint provides insights on the CFDA Programs which received disaster/emergency funding per the requested filters.

## POST

Returns spending details of CFDA receiving supplimental funding budgetary resources

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes
        + `filter` (required, Filter, fixed-type)
        + `spending_facets` (required, array[Spending], fixed-type)
        + `pagination` (optional, Pagination, fixed-type)

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, array[Result], fixed-type)
        + `pagination_metadata` (required, PageMetadata, fixed-type)


    + Body

            {
                "results": [
                    {
                        "id": 43,
                        "code": "090",
                        "description": "Description text of 090, for humans,
                        "children": [],
                        "count": 54,
                        "award_obligation": 89.01,
                        "award_outlay": 70.98
                    },
                    {
                        "id": 41,
                        "code": "012",
                        "description": "Description text of 012, for humans,
                        "children": [],
                        "count": 2,
                        "award_obligation": 50,
                        "award_outlay": 10
                    }
                ],
                "pagination_metadata": {
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
+ `fiscal_year` (required, number)
+ `award_type_codes` (required, array[AwardTypeCodes], fixed-type)
+ `query` (optional, string)
    A "keyword" or "search term" to filter down results based on this text snippet

## Pagination (object)
+ `page` (optional, number)
    Requested page of results
    + Default: 1
+ `size` (optional, number)
    Page Size of results
    + Default: 10
+ `order` (optional, enum[string])
    Indicates what direction results should be sorted by. Valid options include asc for ascending order or desc for descending order.
    + Default: `desc`
    + Members
        + `desc`
        + `asc`
+ `sort` (optional, string)
    Optional parameter indicating what value results should be sorted by. Valid options are any of the fields in the JSON objects in the response. Defaults to the first field provided.


## Result (object)
+ `id`
+ `code`
+ `description`
+ `children` (optional, array[Result])
+ `count` (required, number, fixed-type)
+ `award_obligation` (optional, number, nullable)
+ `award_outlay` (optional, number, nullable)
+ `total_obligation` (optional, number, nullable)
+ `total_outlay` (optional, number, nullable)

## PageMetadata (object)
+ `page` (required, number)
+ `next` (required, number, nullable)
+ `previous` (required, number, nullable)
+ `hasNext` (required, boolean)
+ `hasPrevious` (required, boolean)
+ `total` (required, number)
+ `limit` (required, number)

## Spending (enum[string])
+ `award_obligation`
    Obligation amount captured by File D award data
+ `award_outlay`
    Outlay amount captured by File D award data
+ `total_obligation`
    Total obligation amount captured by File C award data
+ `total_outlay`
    Total outlay amount captured by File C award data


# Data Structures

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

## AwardTypeCodes (enum[string])
List of procurement and assistance award type codes supported by USAspending.gov

### Members
+ `02`
+ `03`
+ `04`
+ `05`
+ `06`
+ `07`
+ `08`
+ `09`
+ `10`
+ `11`
+ `A`
+ `B`
+ `C`
+ `D`
+ `IDV_A`
+ `IDV_B_A`
+ `IDV_B_B`
+ `IDV_B_C`
+ `IDV_B`
+ `IDV_C`
+ `IDV_D`
+ `IDV_E`

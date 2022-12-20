FORMAT: 1A
HOST: https://api.usaspending.gov

# Federal Account Spending Disaster/Emergency Funding via Loans [/api/v2/disaster/federal_account/loans/]

This endpoint provides insights on the Federal Accounts awarding loans from disaster/emergency funding per the requested filters.

## POST

Returns loan spending details of Federal Accounts receiving supplemental funding budgetary resources

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes
        + `filter` (required, Filter, fixed-type)
        + `pagination` (optional, Pagination, fixed-type)
    + Body


            {
                "filter": {
                    "def_codes": ["L", "M", "N", "O", "P", "U"]
                },
                "pagination": {
                    "limit": 10,
                    "page": 1,
                    "sort": "award_count",
                    "order": "desc"
                },
                "spending_type": "total"
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `totals` (required, Totals, fixed-type)
        + `results` (required, array[Result], fixed-type)
        + `page_metadata` (required, PageMetadata, fixed-type)
    + Body


            {
                "totals": {
                    "award_count": 4574,
                    "face_value_of_loan": 290416885040.71,
                    "obligation": 364037369840.58,
                    "outlay": 290416885040.71
                },
                "results": [
                    {
                        "id": "43",
                        "code": "090",
                        "description": "Description text of 090, for humans",
                        "children": [],
                        "award_count": 54,
                        "face_value_of_loan": 89.01,
                        "obligation": 23,
                        "outlay": 5
                    },
                    {
                        "id": "41",
                        "code": "012",
                        "description": "Description text of 012, for humans",
                        "children": [],
                        "award_count": 2,
                        "face_value_of_loan": 50,
                        "obligation": 12,
                        "outlay": 4
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
        + `award_count`
        + `face_value_of_loan`
        + `obligation`
        + `outlay`

## Totals (object)
+ `award_count` (required, number, nullable)
+ `face_value_of_loan` (required, number)
+ `obligation` (required, number)
+ `outlay` (required, number)

## Result (object)
+ `id` (required, string)
+ `code` (required, string)
+ `description` (required, string)
+ `children` (optional, array[Result], fixed-type)
+ `award_count` (required, number)
+ `face_value_of_loan` (required, number, nullable)
+ `obligation` (required, number, nullable)
+ `outlay` (required, number, nullable)

## PageMetadata (object)
+ `page` (required, number)
+ `next` (required, number, nullable)
+ `previous` (required, number, nullable)
+ `hasNext` (required, boolean)
+ `hasPrevious` (required, boolean)
+ `total` (required, number)
+ `limit` (required, number)

## DEFC (enum[string])
List of Disaster Emergency Fund (DEF) Codes (DEFC) defined by legislation at the time of writing.
When filtering on `award_type_codes` this will filter on File D records that have at least one File C with the provided DEFC
and belong to CARES Act DEFC.
A list of current DEFC can be found [here.](https://files.usaspending.gov/reference_data/def_codes.csv)

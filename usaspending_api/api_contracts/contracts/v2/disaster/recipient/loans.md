FORMAT: 1A
HOST: https://api.usaspending.gov

# Recipient Spending Disaster/Emergency Funding via Loans [/api/v2/disaster/recipient/loans/]

This endpoint provides insights on the recipient loans from disaster/emergency funding per the requested filters.

## POST

Returns loan spending details of recipients receiving supplemental funding budgetary resources

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
                    "def_codes": ["L", "M", "N", "O", "P", "U"],
                    "award_type_codes": ["07", "08"]
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
                    "face_value_of_laon": "37369840.0",
                    "obligation": 364037369840.58,
                    "outlay": 290416885040.71
                },
                "results": [
                    {
                        "code": "987654321",
                        "award_count": 2,
                        "description": "RECIPIENT 3",
                        "face_value_of_loan": 100.0,
                        "id": ["d2894d22-67fc-f9cb-4005-33fa6a29ef86-C", "d2894d22-67fc-f9cb-4005-33fa6a29ef86-R"],
                        "obligation": 2200.0,
                        "outlay": 1100.0
                    },
                    {
                        "code": "456789123",
                        "award_count": 1,
                        "description": "RECIPIENT 2",
                        "face_value_of_loan": 200.0,
                        "id": ["3c92491a-f2cd-ec7d-294b-7daf91511866-R"],
                        "obligation": 20.0,
                        "outlay": 0.0
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
+ `award_type_codes` (optional, array[enum[string]], fixed-type)
    Only accepts loan award type `07` or `08` in the array, since this endpoint is specific to loans
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
    + Default: `description`
    + Members
        + `description`
        + `award_count`
        + `face_value_of_loan`
        + `obligation`
        + `outlay`

## Totals (object)
+ `award_count` (required, number)
+ `face_value_of_laon` (required, number)
+ `obligation` (required, number)
+ `outlay` (required, number)

## Result (object)
+ `id` (required, array[string], fixed-type)
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
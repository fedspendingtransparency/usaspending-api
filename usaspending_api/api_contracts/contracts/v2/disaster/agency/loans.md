FORMAT: 1A
HOST: https://api.usaspending.gov

# Agencies Spending Disaster/Emergency Funding via Loans [/api/v2/disaster/agency/loans/]

This endpoint provides insights on the Agencies awarding loans from disaster/emergency funding per the requested filters.

## POST

Returns loan spending details of Agencies receiving supplemental funding budgetary resources

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
                    "face_value_of_loan": 290416885040.71,
                    "obligation": 364037369840.58,
                    "outlay": 290416885040.71
                },
                "results": [
                    {
                        "id": 121,
                        "code": "485",
                        "description": "Corporation for National and Community Service",
                        "children": [],
                        "award_count": 1,
                        "obligation": 16791.43,
                        "outlay": 0.0,
                        "face_value_of_loan": 0.0
                    },
                    {
                        "id": 118,
                        "code": "514",
                        "description": "U.S. Agency for Global Media",
                        "children": [],
                        "award_count": 1,
                        "obligation": 221438.82,
                        "outlay": 0.0,
                        "face_value_of_loan": 0.0
                    }
                ],
                "page_metadata": {
                    "page": 1,
                    "total": 12,
                    "limit": 2,
                    "next": 2,
                    "previous": null,
                    "hasNext": true,
                    "hasPrevious": false
                }
            }

# Data Structures

## Filter (object)
+ `def_codes` (required, array[DEFC], fixed-type)
+ `award_type_codes` (optional, array[enum[string]], fixed-type)
    Only accepts loan award type `07` or `08` in the array, since this endpoint is specific to loans.

    If ANY award type codes are provided, loan amounts will be summed for the distinct set of toptier agencies,
    whose subtier agencies funded loan awards linked to `FinancialAccountsByAwards` records (which are derived from DABS File C).

    If this parameter is not provided, loan amounts will be summed for a different set of agencies:
    the distinct set of toptier agencies "owning" appropriations accounts used in funding _any_ award spending
    for this disaster (i.e. from agencies "owning" Treasury Account Symbol (TAS) accounts on `FinancialAccountsByAwards`
    records, which are derived from DABS File C).

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
+ `obligation` (required, number, nullable)
+ `outlay` (required, number, nullable)
+ `face_value_of_loan` (required, number, nullable)

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
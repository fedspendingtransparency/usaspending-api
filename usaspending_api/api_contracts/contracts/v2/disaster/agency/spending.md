FORMAT: 1A
HOST: https://api.usaspending.gov

# Agencies Spending Disaster/Emergency Funding [/api/v2/disaster/agency/spending/]

This endpoint provides insights on the Agencies which received disaster/emergency funding per the requested filters.

## POST

Returns spending details of Agencies receiving supplemental funding budgetary resources

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes
        + `filter` (required, Filter, fixed-type)
        + `spending_type` (required, enum[string], fixed-type)
            Toggle if the outlay and obligation response values are total or only from awards only from awards
            + Members
                + `total`
                + `award`
        + `pagination` (optional, Pagination, fixed-type)

    + Body


            {
                "filter": {
                    "def_codes": ["L", "M", "N", "O", "P", "U"],
                    "award_type_codes": ["02", "03", "04", "05", "07", "08", "10", "06", "09", "11", "A", "B", "C", "D", "IDV_A", "IDV_B", "IDV_B_A", "IDV_B_B", "IDV_B_C", "IDV_C", "IDV_D", "IDV_E", "-1"]
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
                    "obligation": 364037369840.58,
                    "outlay": 290416885040.71
                },
                "results": [
                    {
                        "id": 168,
                        "code": "349",
                        "description": "District of Columbia Courts",
                        "children": [],
                        "award_count": null,
                        "obligation": 148906061.27,
                        "outlay": 143265869.34,
                        "total_budgetary_resources": 18087913735.71
                    },
                    {
                        "id": 130,
                        "code": "413",
                        "description": "National Council on Disability",
                        "children": [],
                        "award_count": null,
                        "obligation": 225185.66,
                        "outlay": 697329.0,
                        "total_budgetary_resources": 183466350.0
                    }
                ],
                "page_metadata": {
                    "page": 1,
                    "total": 62,
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
+ `award_type_codes` (optional, array[AwardTypeCodes], fixed-type)
    Only to be used when `"spending_type": "award"`.

    If ANY award type codes are provided, obligation and outlay spending amounts will be summed for the distinct set of toptier
    agencies, whose subtier agencies funded awards -- awards of the type given by `award_type_codes` -- linked to
    `FinancialAccountsByAwards` records (which are derived from DABS File C).

    If this parameter is not provided, obligation and outlay spending amounts will be summed for a different set of agencies:
    the distinct set of toptier agencies "owning" appropriations accounts used in funding _any_ award spending for this disaster
    (i.e. from agencies "owning" Treasury Account Symbol (TAS) accounts on `FinancialAccountsByAwards` records, which are derived from DABS File C).
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
        + `total_budgetary_resources`
        + `obligation`
        + `outlay`

## Totals (object)
+ `award_count` (optional, number)
+ `total_budgetary_resources` (optional, number)
+ `obligation` (required, number)
+ `outlay` (required, number)

## Result (object)
+ `id` (required, string)
+ `code` (required, string)
+ `description` (required, string)
+ `children` (optional, array[Result], fixed-type)
+ `award_count` (required, number, nullable)
+ `obligation` (required, number, nullable)
+ `outlay` (required, number, nullable)
+ `total_budgetary_resources` (required, number, nullable)

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
+ `-1`

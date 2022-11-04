FORMAT: 1A
HOST: https://api.usaspending.gov

# Federal Account Spending Disaster/Emergency Funding [/api/v2/disaster/federal_account/spending/]

This endpoint provides insights on the Federal Account and TAS which received disaster/emergency funding per the requested filters.

## POST

Returns spending details of Federal Account and TAS receiving supplemental funding budgetary resources

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes
        + `filter` (required, Filter, fixed-type)
        + `spending_type` (required, enum[string], fixed-type)
            Toggle if the outlay and obligation response values are total or only from awards
            + Members
                + `total`
                + `award`
        + `pagination` (optional, Pagination, fixed-type)
    + Body


            {
                "filter": {
                    "def_codes": [
                        "L", "M", "N", "O", "P", "U"
                    ]
                },
                "spending_type": "total",
                "pagination": {
                    "page": 1,
                    "limit": 2,
                    "order": "desc",
                    "sort": "id"
                }
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
                        "id": "43",
                        "code": "090",
                        "description": "Description text",
                        "children": [{
                                "id": 78470,
                                "code": "020-X-1894-000",
                                "description": "Air Carrier Worker Support, Departmental Offices, Department of Treasury",
                                "award_count": 1,
                                "obligation": 49162967964.08,
                                "outlay": 34401350618.2,
                                "total_budgetary_resources": null
                            }],
                        "award_count": 54,
                        "obligation": 89.01,
                        "outlay": 70.98,
                        "total_budgetary_resources": null
                    },
                    {
                        "id": "41",
                        "code": "012",
                        "description": "Description text",
                        "children": [{
                                "id": 78461,
                                "code": "020-2020/2020-1892-000",
                                "description": "Coronavirus Relief Fund, Departmental Offices, Treasury",
                                "award_count": 1,
                                "obligation": 293266617784.74,
                                "outlay": 293266617784.74,
                                "total_budgetary_resources": null
                            }],
                        "award_count": 2,
                        "obligation": 50,
                        "outlay": 10,
                        "total_budgetary_resources": null
                    }
                ],
                "page_metadata": {
                    "page": 1,
                    "total": 915,
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
+ `id` (required, number)
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

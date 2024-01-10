FORMAT: 1A
HOST: https://api.usaspending.gov

# CFDA Programs Spending Disaster/Emergency Funding [/api/v2/disaster/cfda/spending/]

This endpoint provides insights on the CFDA Programs which received disaster/emergency funding per the requested filters.

## POST

Returns spending details of CFDA receiving supplemental funding budgetary resources

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
                    "def_codes": ["L", "M", "N", "O", "P", "U"],
                    "award_type_codes": ["02", "03", "04", "05", "07", "08", "10", "06", "09", "11", "-1"]
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
                        "code": "20.200",
                        "award_count": 1,
                        "description": "CFDA 2",
                        "id": 200,
                        "obligation": 20.0,
                        "outlay": 0.0,
                        "resource_link": "www.example.com/200",
                        "cfda_federal_agency": "OFFICE OF FEDERAL STUDENT AID, EDUCATION, DEPARTMENT OF",
                        "cfda_objectives": "To provide loan capital directly from the Federal government",
                        "cfda_website": "http://www.ifap.ed.gov/.",
                        "applicant_eligibility": "The applicant must be a U.S. citizen, national, or",
                        "beneficiary_eligibility": "Vocational, undergraduate, and graduate"
                    },
                    {
                        "code": "10.100",
                        "award_count": 1,
                        "description": "CFDA 1",
                        "id": 100,
                        "obligation": 2.0,
                        "outlay": 0.0,
                        "resource_link": null,
                        "cfda_federal_agency": "OFFICE OF FEDERAL STUDENT AID, EDUCATION, DEPARTMENT OF",
                        "cfda_objectives": "To provide loan capital directly from the Federal government",
                        "cfda_website": "http://www.ifap.ed.gov/.",
                        "applicant_eligibility": "The applicant must be a U.S. citizen, national, or",
                        "beneficiary_eligibility": "Vocational, undergraduate, and graduate"
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
+ `award_type_codes` (optional, array[AwardTypeCodes], fixed-type)
    Defaults to all Award Type Codes. Applicable only when requested `award` spending.
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
        + `obligation`
        + `outlay`

## Totals (object)
+ `award_count` (required, number)
+ `obligation` (required, number)
+ `outlay` (required, number)

## Result (object)
+ `id` (required, string)
+ `code` (required, string)
+ `description` (required, string)
+ `award_count` (required, number)
+ `obligation` (required, number, nullable)
+ `outlay` (required, number, nullable)
+ `resource_link` (required, string, nullable)
    Link to an external website with more information about this result.
+ `cfda_federal_agency` (required, string)
+ `cfda_objectives` (required, string)
+ `cfda_website` (required, string, nullable)
    Link to an external website with more information about this program.
+ `applicant_eligibility` (required, string)
+ `beneficiary_eligibility` (required, string)

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

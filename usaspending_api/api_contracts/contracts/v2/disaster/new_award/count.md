FORMAT: 1A
HOST: https://api.usaspending.gov

# Count of New Awards for the disaster/emergency funding [/api/v2/disaster/new_award/count/]

This endpoint provides the count of New Awards which received disaster/emergency funding per the requested filters.

## POST

This endpoint returns a count of New Awards

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes
        + `def_codes` (required, array[DEFC], fixed-type)
        + `fiscal_year` (required, number)
        + `spending_type` (required, enum[string], fixed-type)
            + Members
                + `total`
                + `award`
        + `award_type_codes` (optional, array[AwardTypeCodes], fixed-type)

+ Response 200 (application/json)
    + Attributes (object)
        + `count` (required, number)
    + Body

            {
                "count": 5
            }

# Data Structures

## DEFC (enum)
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

## AwardTypeCodes (enum)
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

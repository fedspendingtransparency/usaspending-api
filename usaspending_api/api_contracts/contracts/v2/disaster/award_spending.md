FORMAT: 1A
HOST: https://api.usaspending.gov

# Aggregated Award Spending From Disaster/Emergency Funding [/api/v2/disaster/award_spending/]

This endpoint provides account data obligation and outlay spending aggregations of all (File D) Awards which received disaster/emergency funding per the requested filters.

## POST

This endpoint provides the Account obligation and outlay aggregations of Awards

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes
        + `def_codes` (required, array[DEFC], fixed-type)
        + `fiscal_year` (required, number)
        + `award_type_codes` (required, array[AwardTypeCodes], fixed-type)

+ Response 200 (application/json)
    + Attributes (object)
        + `obligation` (required, number)
        + `outlay` (required, number)
    + Body

            {
                "obligation": 32984563875,
                "outlay": 15484564321
            }


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

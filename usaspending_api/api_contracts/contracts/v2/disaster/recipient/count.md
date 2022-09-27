FORMAT: 1A
HOST: https://api.usaspending.gov

# Count of Recipients Receiving Disaster/Emergency Funding [/api/v2/disaster/recipient/count/]

This endpoint provides the count of Recipients which received disaster/emergency funding per the requested filters.

## POST

This endpoint returns a count of Recipients

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes
        + `filter` (required, Filter, fixed-type)

    + Body

            {
                "filter": {
                    "def_codes": ["L", "M", "N", "O", "P", "U"]
                }
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `count` (required, number)
    + Body

            {
                "count": 5
            }

# Data Structures

## Filter (object)
+ `def_codes` (required, array[DEFC], fixed-type)
+ `award_type_codes` (optional, array[AwardTypeCodes], fixed-type)
    Defaults to all Award Type Codes.

## DEFC (enum[string])
List of Disaster Emergency Fund (DEF) Codes (DEFC) defined by legislation at the time of writing

### Members
- `1`
- `2`
- `3`
- `4`
- `5`
- `6`
- `7`
- `8`
- `9`
- `A`
- `B`
- `C`
- `D`
- `E`
- `F`
- `G`
- `H`
- `I`
- `J`
- `K`
- `L`
- `M`
- `N`
- `O`
- `P`
- `Q`
- `QQQ`
- `R`
- `S`
- `T`
- `U`
- `V`
- `W`
- `X`
- `Y`
- `Z`

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

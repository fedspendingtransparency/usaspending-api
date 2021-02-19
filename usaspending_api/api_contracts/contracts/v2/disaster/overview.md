FORMAT: 1A
HOST: https://api.usaspending.gov

# Disaster Spending Overview [/api/v2/disaster/overview/{?def_codes}]

This endpoint provides funding and spending details from emergency/disaster supplemental funding legislation.

## GET

+ Request (application/json)
    + Parameters
        + `def_codes` (optional, array[DEFC])
           Comma-delimited list of DEF codes to limit results to.

+ Response 200 (application/json)
    + Attributes (object)
        + `funding` (required, array[Funding], fixed-type)
        + `total_budget_authority` (required, number)
        + `spending` (required, Spending, fixed-type)

    + Body

            {
                "funding": [
                    {
                        "def_code": "L",
                        "amount": 7410000000
                    },
                    {
                        "def_code": "M",
                        "amount": 11230000000
                    }
                ],
                "total_budget_authority": 2300000000000,
                "spending": {
                    "award_obligations": 866700000000,
                    "award_outlays": 413100000000,
                    "total_obligations": 963000000000,
                    "total_outlays": 459000000000
                }
            }

# Data Structures
## Funding (object)
+ `def_code` (required, string)
    DEF Code providing the source funding
+ `amount` (required, number)
    Aggregation amount under the DEFC

## Spending (object)
+ `award_obligations` (required, number, nullable)
    Amount of Disaster Spending which has been awarded and obligated.
    Note: `total_obligations` - `award_obligations` = "Other Obligations"
+ `award_outlays` (required, number, nullable)
    Amount of Disaster Spending which has been awarded, obligated, and outlayed.
    Note: `award_obligations` - `award_outlays` = "Award Obligated But Not Yet Outlayed"
+ `total_obligations` (required, number, nullable)
    Total amount of Disaster Spending which has been obligated.
    Note: `total_budget_authority` - `total_obligations` = "Remaining Balance"
+ `total_outlays` (required, number, nullable)
    Total amount of Disaster Spending which has been obligated and outlayed.
    Note: (`total_obligations` - `award_obligations`) - (`total_outlays` - `award_outlays`) = "Other Obligated But Not Yet Outlayed"

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
+ `U`
+ `9`

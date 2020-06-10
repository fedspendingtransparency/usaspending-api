FORMAT: 1A
HOST: https://api.usaspending.gov

# Disaster Spending Overview [/api/v2/disaster/overview/]

This endpoint provides funding and spending details from emergency/disaster supplemental funding legislation.

## GET


+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "string"
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `funding` (required, array[Funding], fixed-type)
        + `total_budget_authority` (required, number)
        + `spending` (required, Spending, fixed-type)

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
    Note: `award_obligations` - `award_outlays` = "Other Obligated But Not Yet Outlayed"
+ `total_obligations` (required, number, nullable)
    Total amount of Disaster Spending which has been obligated.
    Note: `total_budget_authority` - `total_obligations` = "Remaining Balance"
+ `total_outlays` (required, number, nullable)
    Total amount of Disaster Spending which has been obligated and outlayed.
    Note: `total_obligations` - `total_outlays` = "Other Obligated But Not Yet Outlayed"

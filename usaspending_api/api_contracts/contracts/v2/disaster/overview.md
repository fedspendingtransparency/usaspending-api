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
+ `code` (required, string)
    DEF Code providing the funding
+ `title` (required, string)
    DEF title from legislation
+ `amount` (required, number)
    Aggregation amount under the DEFC

## Spending (object)
+ `award_obligations_not_outlayed` (required, number, nullable)
+ `award_obligations` (required, number, nullable)
+ `award_outlays` (required, number, nullable)
+ `other_obligations_not_outlayed` (required, number, nullable)
+ `other_obligations` (required, number, nullable)
+ `other_outlays` (required, number, nullable)
+ `remaining_balance` (required, number, nullable)

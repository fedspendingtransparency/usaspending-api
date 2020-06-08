FORMAT: 1A
HOST: https://api.usaspending.gov

# Disaster Spending Overview [/api/v2/disaster/overview/]

This endpoint provides funding and spending details from emergency/disaster supplimental funding legislation.

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

## Spending (object)

FORMAT: 1A
HOST: https://api.usaspending.gov

# List of Available Program Activities [/api/v2/federal_accounts/{federal_account_id}/spending_over_time]

This endpoint supports the Federal Account page and allow for listing spending by the Federal Account.

## POST

This endpoint returns a list of spending amounts for each time period by the specified federal account.

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Parameters
        + `federal_account_id`: 5 (required, number)
            Database id for a federal account.
    + Attributes
        + `group` (optional, enum[string])
            + Members
                + `fiscal_year`
                + `quarter`
                + `month`
            + Default
                `fiscal_year`
        + `filters` (required, Filter)

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, array[TimePeriod], fixed-type)
    + Body

            {
                "results": [
                    {
                        "fiscal_year": 2019,
                        "quarter": 1,
                        "aggregates": {
                            "obligations_incurred": 84882167133.77,
                            "obligations": 407767674.42,
                            "outlay": 80776067405.98,
                            "unobligated_balance":314054713231.07
                        }
                    }, {
                        "fiscal_year": 2019,
                        "quarter": 2,
                        "aggregates": {
                            "obligations_incurred": 84882167133.77,
                            "obligations": 407767674.42,
                            "outlay": 80776067405.98,
                            "unobligated_balance":314054713231.07
                        }
                    }, {
                        "fiscal_year": 2019,
                        "quarter": 3,
                        "aggregates": {
                            "obligations_incurred": 84882167133.77,
                            "obligations": 407767674.42,
                            "outlay": 80776067405.98,
                            "unobligated_balance":314054713231.07
                        }
                    }, {
                        "fiscal_year": 2019,
                        "quarter": 4,
                        "aggregates": {
                            "obligations_incurred": 84882167133.77,
                            "obligations": 407767674.42,
                            "outlay": 80776067405.98,
                            "unobligated_balance":314054713231.07
                        }
                    }
                ]
            }

# Data Structures

## TimePeriod (object)
+ `fiscal_year` (required, number)
+ `quarter` (required, number, nullable)
+ `aggregates` (required, array[Aggregates], fixed-type)

## Aggregates (object)
+ `obligations_incurred` (required, number, nullable)
+ `obligations` (required, number)
+ `outlay` (required, number)
+ `unobligated_balance` (required, number)

## Filter (object)
+ `fiscal_years` (optional, array[number], fixed-type)
+ `program_actitities` (optional, array[string], fixed-type)
+ `object_classes` (optional, array[string], fixed-type)

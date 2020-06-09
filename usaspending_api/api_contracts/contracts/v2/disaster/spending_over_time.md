FORMAT: 1A
HOST: https://api.usaspending.gov

# Disaster Spending Over Time [/api/v2/disaster/spending_over_time/]

This endpoint provides award spending data from emergency/disaster funding grouped by time period.

## POST

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "string"
            }
    + Attributes (object)
        + `group` (required, enum[string])
            + Members
                + `fiscal_year`
                + `quarter`
                + `period`
            + Default
                + `period`
        + `defc` (required, array[string])
            An array of Disaster / Emergency Funding Codes
        + `award_type_codes` (optional, AwardTypes)
            If not provided, defaults to all award types
        + `spending_type` (required, enum[string])
            + Default
                + `obligations`
            + Members
                + `obligations`
                + `outlays`

    + Body

            {
                "group": "period",
                "defc": ["L", "M", "N", "O", "P"],
                "spending_type": "obligations"
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (array[TimeResult], fixed-type)
        + `messages` (optional, array[string])
            An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.
    + Body

            {
                "results": [
                    {
                        "amounts": {
                            "total": 1000,
                            "contracts": 100,
                            "idvs": 200,
                            "grants": 100,
                            "loans": 300,
                            "direct_payments": 100,
                            "insurance": 200,
                            "other": 0
                        },
                        "time_period": {
                            "fiscal_year": 2020,
                            "period" 6
                        }
                    },
                    {
                        "amounts": {
                            "total": 1000,
                            "contracts": 300,
                            "idvs": 200,
                            "grants": 100,
                            "loans": 100,
                            "direct_payments": 100,
                            "insurance": 100,
                            "other": 100
                        },
                        "time_period": {
                            "fiscal_year": 2020,
                            "period" 7
                        }
                    }
                ]
            }

# Data Structures

## TimeResult (object)
+ `amounts` (required, AmountsByType)
+ `time_period` (required, TimePeriodGroup)

## TimePeriodGroup (object)
+ `fiscal_year` (required, number)
+ `quarter` (optional, number)
    Excluded when grouping by `fiscal_year` or `period`. A number 1 through 4 representing the fiscal quarter.
+ `period` (optional, number)
    Excluded when grouping by `fiscal_year` or `quarter`. A number 1 through 12 representing the fiscal period (month), where period 1 is October.

## AmountsByType (object)
+ `total` (required, number)
    Dollar obligated or outlayed amount (depending on the `spending_type` requested) for all award types
+ `contracts` (required, number)
+ `idvs` (required, number)
+ `grants` (required, number)
+ `loans` (required, number)
+ `direct_payments` (required, number)
+ `insurance` (required, number)
+ `other` (required, number)


## AwardTypes (array)
List of filterable award types

### Sample
- `A`
- `B`
- `C`
- `D`

### Default
- `02`
- `03`
- `04`
- `05`
- `06`
- `07`
- `08`
- `09`
- `10`
- `11`
- `A`
- `B`
- `C`
- `D`
- `IDV_A`
- `IDV_B`
- `IDV_B_A`
- `IDV_B_B`
- `IDV_B_C`
- `IDV_C`
- `IDV_D`
- `IDV_E`

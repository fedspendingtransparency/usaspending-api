FORMAT: 1A
HOST: https://api.usaspending.gov

# Disaster Spending Over Time [/api/v2/disaster/spending_over_time/]

This endpoint provides award spending data from emergency/disaster funding grouped by time period in ascending order (earliest to most recent).

## POST

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes (object)
        + `group` (required, enum[string])
            + Members
                + `fiscal_year`
                + `quarter`
                + `period`
            + Default
                + `period`
        + `def_codes` (required, array[DEFC], fixed-type)
            An array of Disaster / Emergency Funding Codes
        + `award_type_codes` (optional, array[AwardTypeCodes], fixed-type)
            If not provided, defaults to all award types
        + `spending_type` (required, enum[string])
            + Default
                + `obligations`
            + Members
                + `obligations`
                + `outlays`
        + `page` (optional, number)
            The page number that is currently returned.
            + Default: 1
        + `limit` (optional, number)
            How many results are returned.
            + Default: 10

    + Body

            {
                "group": "period",
                "defc": ["L", "M", "N", "O", "P"],
                "spending_type": "obligations"
            }

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (array[TimeResult], fixed-type)
        + `page_metadata` (required, PageMetadata, fixed-type)
            Information used for pagination of results.
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
+ `amounts` (required, AmountsByType, fixed-type)
+ `time_period` (required, TimePeriodGroup, fixed-type)

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

## PageMetadata (object)
+ `page` (required, number)
+ `next` (required, number, nullable)
+ `previous` (required, number, nullable)
+ `hasNext` (required, boolean)
+ `hasPrevious` (required, boolean)
+ `total` (required, number)
+ `limit` (required, number)

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

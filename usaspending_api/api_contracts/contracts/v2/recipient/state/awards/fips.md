FORMAT: 1A
HOST: https://api.usaspending.gov

# State Award Breakdown [/api/v2/recipient/state/awards/{fips}/{?year}]

This endpoint is used to power USAspending.gov's state profile pages. This data can be used to visualize the government spending that occurs in a specific state or territory.

## GET

This endpoint returns the award amounts and totals, based on award type, of a specific state or territory, given its USAspending.gov `id`.

+ Parameters

    + `fips`: `51` (required, string)
        The FIPS code for the state you want to view. You must include leading zeros.
    + `year`: `2017` (optional, string)
        The fiscal year you would like data for. Use `all` to view all time or `latest` to view the latest 12 months.
        
+ Response 200 (application/json)

    + Attributes (array[StateBreakdown], fixed-type)

# Data Structures

## StateBreakdown (object)
+ `type`: `contracts` (required, string)
    Award types include 'contracts', 'grants', 'direct_payments', 'loans', 'other_financial_assistance'.
+ `amount`: 41725.9 (required, number)
    The aggregate value of awards of this type.
+ `count`: 4 (required, number)
    The number of awards of this type.
+ `total_outlays`: 16572454209.94 (required, number)
    The total outlays of awards of this type.

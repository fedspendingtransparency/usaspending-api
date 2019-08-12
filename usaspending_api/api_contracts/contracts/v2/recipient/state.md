FORMAT: 1A
HOST: https://api.usaspending.gov

# State Overview [/api/v2/recipient/state/]

These endpoints are used to power USAspending.gov's state profile pages. This data can be used to visualize the government spending that occurs in a specific state or territory.


## Get List States [GET /api/v2/recipient/state/]

This endpoint returns a list of states and their amounts.

+ Response 200 (application/json)
    + Attributes (array[StateListing], fixed-type)

## Get State Overview [GET /api/v2/recipient/state/{fips}/{?year}]

This endpoint returns a high-level overview of a specific state or territory, given its USAspending.gov `id`.

+ Parameters

    + fips: 51 (required, string)
        The FIPS code for the state you want to view. You must include leading zeros.
    + year: 2017 (optional, string)
        The fiscal year you would like data for. Use `all` to view all time or `latest` to view the latest 12 months.

+ Response 200 (application/json)

    + Attributes (StateOverview)

# Data Structures

## StateListing (object)
+ name: Virginia (required, string)
+ code: VA (required, string)
+ fips: 51 (required, string)
+ amount: 494274.3 (required, number)
+ type: state (required, enum[string])
    + state
    + territory
    + district

## StateOverview (object)
+ name: Virginia (required, string)
+ code: VA (required, string)
+ fips: 51 (required, string)
+ type: state (required, string)
    A string representing the type of area. Possible values are `state`, `district`, `territory`.
+ population: 8414380 (required, number)
+ pop_year: 2017 (required, number)
    The year the population is based on.
+ median_household_income: 68114 (required, number)
+ mhi_year: 2017 (required, number)
    The year the median household income is based on.
+ total_prime_amount: 300200000000 (required, number)
+ total_prime_awards: 327721 (required, number)
+ award_amount_per_capita: 916023.08 (required, number)

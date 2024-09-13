FORMAT: 1A
HOST: https://api.usaspending.gov

# State Overview [/api/v2/recipient/state/{fips}/{?year}]

These endpoints are used to power USAspending.gov's state profile pages. This data can be used to visualize the government spending that occurs in a specific state or territory.

## GET

This endpoint returns a high-level overview of a specific state or territory, given its USAspending.gov `id`.

+ Parameters

    + `fips`: `51` (required, string)
        The FIPS code for the state you want to view. You must include leading zeros.
    + `year`: `2017` (optional, string)
        The fiscal year you would like data for. Use `all` to view all time or `latest` to view the latest 12 months.

+ Response 200 (application/json)

    + Attributes (StateOverview)

# Data Structures

## StateOverview (object)
+ `name`: `Virginia` (required, string)
+ `code`: `VA` (required, string)
+ `fips`: `51` (required, string)
+ `type`: `state` (required, string)
    A string representing the type of area. Possible values are `state`, `district`, `territory`.
+ `population`: 8414380 (required, number)
+ `pop_year`: 2017 (required, number)
    The year the population is based on.
+ `median_household_income`: 68114 (required, number)
+ `mhi_year`: 2017 (required, number)
    The year the median household income is based on.
+ `total_prime_amount`: 300200000000 (required, number)
+ `total_prime_awards`: 327721 (required, number)
+ `total_face_value_loan_amount`: 88888.0 (required, number) The aggregate face value loan guarantee value of all prime awards associated with this state for the given time period.
+ `total_face_value_loan_prime_awards`: 10 (required, number) The number of prime awards associated with this state for the given time period and face value loan guarantee.
+ `award_amount_per_capita`: 916023.08 (required, number)
+ `total_outlays`: 16657372472.78 (required, number)

# State Data

**Route:** /api/v2/recipient/state/:fips?year=:year

**Method:** GET

This route returns basic information about the specified state.

`:fips` fips code representing the state
`:year` year to filter on (*optional*)
* for the latest 12 months, use `latest`
* for all years, use `all`

## Response Example

```
{
    "name": "Virginia",
    "code": "VA",
    "fips": 51,
    "type": "state",
    "population": 8414380,
    "pop_year": 2017,
    "pop_source": "U.S. Census Bureau, 2017 Population Estimate",
    "median_household_income": 68114,
    "mhi_year": 2017,
    "mhi_source": "U.S. Census Bureau, 2017 American Community Survey 1-Year Estimates",
    "total_prime_amount": 300200000000,
    "total_prime_awards": 327721,
    "award_amount_per_capita": 916,023.08
}
```

* `type` is a string representing the type of area. Possible values are:
    * `state` - Most common type
    *  `district` - Specifically for District of Columbia
    *  `territory`- Guam, Puerto Rico, etc.

* `pop_year`: The year the population is based on
* `pop_source`: The source the population is based on
* `mhi_year`: The year the median household income is based on
* `mhi_source`: The source the median household income is based on
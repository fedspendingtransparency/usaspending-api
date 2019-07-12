FORMAT: 1A
HOST: https://api.usaspending.gov

# IDV Funding [/api/v2/search/transaction_spending_summary/]

Returns the high-level aggregations of the counts and dollar amounts for all transactions which match the keyword filter

## POST

+ Request (application/json)
    + Attributes (object)
        + `filters` (required, FilterObject)
            Need to provide `keywords`

+ Response 200 (application/json)
    + Attributes
        + results (required, object)
            - prime_awards_obligation_amount: 1000000 (required, number)
            - prime_awards_count: 3 (required, number)

    + Body

            {
                "results": {
                    "prime_awards_obligation_amount": 55791124858.71,
                    "prime_awards_count": 101365
                }
            }

# Data Structures

## FilterObject (object)
+ `keywords`: `lockheed` (required, array[string], fixed-type)
FORMAT: 1A
HOST: https://api.usaspending.gov

# IDV Funding [/api/v2/search/spending_by_transaction_count/]

## POST

Returns the counts of transaction records which match the keyword grouped by award categories.

+ Request (application/json)
    + Attributes (object)
        + `filters` (required, FilterObject)
            Need to provide `keywords`

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, object)
            - `idvs`: 1 (required, number)
            - `loans`: 100 (required, number)
            - `direct_payments`: 80 (required, number)
            - `contracts`: 9 (required, number)
            - `other`: 0 (required, number)
            - `grants`: 0 (required, number)

    + Body

            {
                "results": {
                    "idvs": 597,
                    "loans": 1,
                    "direct_payments": 0,
                    "contracts": 100732,
                    "other": 0,
                    "grants": 35
                }
            }

# Data Structures

## FilterObject (object)
+ `keywords`: `lockheed` (required, array[string], fixed-type)
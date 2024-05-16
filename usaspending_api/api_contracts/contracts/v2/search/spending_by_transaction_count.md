FORMAT: 1A
HOST: https://api.usaspending.gov

# Spending By Transaction Count [/api/v2/search/spending_by_transaction_count/]

## POST

Returns the counts of transaction records which match the keyword grouped by award categories.

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes (object)
        + `filters` (required, AdvancedFilterObject)
    + Body


            {
                "filters": {
                    "keywords": ["test"],
                    "award_type_codes": [
                        "A",
                        "B",
                        "C",
                        "D"
                    ]
                }
            }

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

### AdvancedFilterObject (object)
The filters available are defined in [AdvancedFilterObject](./spending_by_transaction.md#advanced-filter-object). The only difference is that the `keywords` and `award_type_codes` filters are not required.

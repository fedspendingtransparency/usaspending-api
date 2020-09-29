FORMAT: 1A
HOST: https://api.usaspending.gov

# IDV Funding [/api/v2/search/transaction_spending_summary/]

## POST

Returns the high-level aggregations of the counts and dollar amounts for all transactions which match the keyword filter

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes (object)
        + `filters` (required, AdvancedFilterObject)
            Need to provide `keywords`
            
    + Body
        
        
            {"filters":{"keyword":"test"}}

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, object)
            - `prime_awards_obligation_amount`: 1000000 (required, number)
            - `prime_awards_count`: 3 (required, number)

    + Body

            
            {
                "results": {
                    "prime_awards_obligation_amount": 55791124858.71,
                    "prime_awards_count": 101365
                }
            }

# Data Structures

## AdvancedFilterObject (object)
+ `keywords`: `lockheed` (required, array[string], fixed-type)
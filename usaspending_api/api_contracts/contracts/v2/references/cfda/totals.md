FORMAT: 1A
HOST: https://api.usaspending.gov

# Opportunity Totals by CFDA [/api/v2/references/cfda/totals/]

This endpoint provides total values for all CFDAs.

## GET

Returns opportunity totals for a CFDA or all opportunity totals.

+ Request (application/json)

+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, array[CFDA], fixed-type)

    + Body

            {
                "results": [
                    {
                        "cfda": "00.000",
                        "posted": 442,
                        "closed": 234,
                        "archived": 1685,
                        "forecasted": 208
                    },
                    {
                        "cfda": "12.012",
                        "posted": 442,
                        "closed": 234,
                        "archived": 1685,
                        "forecasted": 208
                    }
                ]
            }

# Data Structures
## CFDA (object)
+ `cfda` (required, string)
    CFDA Code
+ `posted` (required, number)
    Number of programs posted
+ `closed` (required, number)
    Number of programs closed
+ `archived` (required, number)
    Number of programs archived
+ `forecasted` (required, number)
    Number of programs forecasted

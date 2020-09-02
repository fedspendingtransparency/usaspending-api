FORMAT: 1A
HOST: https://api.usaspending.gov

# Opportunity Totals by CFDA [/api/v2/references/cfda/totals/{?cfda_code}/]

This endpoint provides total values for requested CFDA.

## GET

Returns opportunity totals for a CFDA or all opportunity totals.

+ Request (application/json)
    + Parameters
        + `cfda_code`: `12.012` (required, string)
            A CFDA code to limit results to. If no value is provided, all CFDAs will be returned.

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
+ `cfda` (required, string, nullable)
    CFDA Code
+ `posted` (required, number, nullable)
    Number of programs posted
+ `closed` (required, number, nullable)
    Number of programs closed
+ `archived` (required, number, nullable)
    Number of programs archived
+ `forecasted` (required, number, nullable)
    Number of programs forecasted

FORMAT: 1A
HOST: https://api.usaspending.gov

# Opportunity Totals by CFDA [/api/v2/references/cfda/opportunities/totals/{?cfda_code}/]

This endpoint provides insights on the opportunity totals for CFDA's.

## GET

Returns opportunity totals for a CFDA or all opportunity totals.

+ Request (application/json)
    + Parameters
        + `cfda_code` (optional, CFDACode, fixed-type)
            A CFDA code to limit results to.

+ Response 200 (application/json)
    + Attributes (object)
        + `cfdas` (required, array[CFDAS], fixed-type)

    + Body

            {
                "cfdas": [
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
## CFDAS (object)
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

## CFDACode (string)
A CFDA Code

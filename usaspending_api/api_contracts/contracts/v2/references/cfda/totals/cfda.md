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

    + Body
    
            {
                "cfda": "00.000",
                "posted": 442,
                "closed": 234,
                "archived": 1685,
                "forecasted": 208
            }

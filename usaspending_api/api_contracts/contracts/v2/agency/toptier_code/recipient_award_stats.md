FORMAT: 1A
HOST: https://api.usaspending.gov

# Federal Obligations [/api/v2/agency/{toptier_code}/recipient_award_stats/{?fiscal_year}]

This endpoint is used to power USAspending.gov's agency profile pages.

## GET

This endpoint returns a set of award amount statistics received by a toptier agency in a fiscal year: greatest single award amount, least award amount (note negative amounts are possible), and amounts of awards at median and 25th & 75th percentile.

+ Parameters
    + `toptier_code`: `086` (required, string)
        The toptier code of an agency (could be a CGAC or FREC) so only numeric character strings of length 3-4 are accepted.
    + `fiscal_year`: 2017 (required, number)
        The fiscal year for which you are querying data. Defaults to the current fiscal year if not provided.
        
+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, RecipientAwardStats, fixed-type)

+ Body

        {
            "results":
                {
                    'max': 1484593892,
                    '75pct': 723892349,
                    'median': 598274389,
                    '25pct': 283479234,
                    'min': -4792034
                }
        }

# Data Structures

## RecipientAwardStats (object)
+ `max` (required, number)
+ `75pct` (required, number)
+ `median` (required, number)
+ `25pct` (required, number)
+ `min` (required, number)

FORMAT: 1A
HOST: https://api.usaspending.gov

# Federal Obligations [/api/v2/agency/{toptier_code}/recipient_award_stats/{?fiscal_year}]

This endpoint is used to power USAspending.gov's agency profile pages.

## GET

This endpoint returns a sorted array of award amount statistics received by a toptier agency/FY combination: greatest single award amount, least award amount (note negative amounts are possible), and amounts of awards at median and 25th & 75th percentile.

+ Parameters
    + `toptier_code`: `086` (required, string)
        The toptier code of an agency (could be a CGAC or FREC) so only numeric character strings of length 3-4 are accepted.
    + `fiscal_year`: 2017 (required, number)
        The fiscal year for which you are querying data. Defaults to the current fiscal year if not provided.
        
+ Response 200 (application/json)
    + Attributes (object)
        + `results` (required, array[RecipientAwardStats], fixed-type)
            Sorted by amount, descending.

    + Body

            {
                "results": [
                    1484593892,
                    723892349,
                    498274389,
                    283479234,
                    -4792034
                ]
            }

# Data Structures

## RecipientAwardStats (array)
+ max award amount
+ 75th percentile amount
+ median award amount
+ 25th percentile amount
+ min award amount

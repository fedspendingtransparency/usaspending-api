FORMAT: 1A
HOST: https://api.usaspending.gov

# Recipient Duns [/api/v2/recipient/count/]

These endpoints are used to power USAspending.gov's recipient profile pages.

## POST 

This endpoint returns the count of recipients given an award_type and keyword string.

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object"
            }

    + Attributes (object)
        + `keyword` (optional, string)
            The keyword results are filtered by. Searches on name, UEI, and DUNS.
        + `award_type` (optional, enum[string])
            The award type results are filtered by.
            + Default: `all`
            + Members
                + `all`
                + `contracts`
                + `grants`
                + `loans`
                + `direct_payments`
                + `other_financial_assistance`

        + Body
                {
                    "award_type": "all"
                }


+ Response 200 (application/json)
    + Attributes
        + `count` (required, number)

    + Body


            {
                "count": 10966055
            }

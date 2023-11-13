FORMAT: 1A
HOST: https://api.usaspending.gov

# Overview of awards for Agency [/api/v2/agency/awards/count{?fiscal_year}]

Return the count of Awards under the Agency

## GET

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "number"
            }

    + Parameters

        + `fiscal_year` (optional, number)
            The desired appropriations fiscal year. Defaults to the current FY


+ Response 200 (application/json)
    + Attributes
        + `results` (required, array[AgencyAwardCountResult], fixed-type)
        + `messages` (required, array[string], fixed-type)
            An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.

    + Body

            {
                "results": {
                    "contracts": 2724,
                    "idvs": 45,
                    "grants": 0,
                    "direct_payments": 0,
                    "loans": 0,
                    "other": 0
                },
                "messages": []
            }

# Data Structures

## AgencyResult (object)
+ `AwardTypeResult`
+ `Awarding Toptier Agency Name`

## AwardTypeResult (object)
+ `grants` (required, number)
+ `loans` (required, number)
+ `contracts` (required, number)
+ `direct_payments` (required, number)
+ `other` (required, number)
+ `idvs` (required, number)



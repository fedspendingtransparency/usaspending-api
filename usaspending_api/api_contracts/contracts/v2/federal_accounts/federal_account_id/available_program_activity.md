FORMAT: 1A
HOST: https://api.usaspending.gov

# List of Available Program Activities [/api/v2/federal_accounts/{federal_account_id}/available_program_activities]

This endpoint supports the Federal Account page and allows for listing all Program Activities with activity by the Federal Account.

## GET

This endpoint returns a list of all Program Activities that the specified federal account has allotted money toward.

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "string"
            }

    + Parameters
        + `federal_account_id`: 5 (required, number)
            Database id for a federal account.

+ Response 200 (application/json)
    + Attributes (object)
        + `count` (required, number)
        + `results` (required, array[ProgramActivity], fixed-type)
    + Body

            {
                "count": 9,
                "results": [
                    {
                        "name": "UNKNOWN/OTHER",
                        "code": "0000"
                    }, {
                        "name": "ZERO OBLIGATION",
                        "code": "0000"
                    }, {
                        "name": "FEDERAL CONTRIBUTION TO MATCH PREMIUMS (SMI)",
                        "code": "0001"
                    }, {
                        "name": "PART D BENEFITS (RX DRUG)",
                        "code": "0002"
                    }, {
                        "name": "PART D FEDERAL ADMINISTRATION (RX DRUG)",
                        "code": "0003"
                    }, {
                        "name": "GENERAL FUND TRANSFERS TO HI",
                        "code": "0004"
                    }, {
                        "name": "FEDERAL BUREAU OF INVESTIGATION (HCFAC)",
                        "code": "0006"
                    }, {
                        "name": "FEDERAL PAYMENTS FROM TAXATION OF OASDI BENEFITS (HI)",
                        "code": "0007"
                    }, {
                        "name": "STATE LOW INCOME DETERMINATIONS",
                        "code": "0011"
                    }
                ]
            }

# Data Structures

## ProgramActivity (object)
+ `name` (required, string)
+ `code` (required, string)

FORMAT: 1A
HOST: https://api.usaspending.gov

# List Treasury Accounts for the specified Federal Account [/api/v2/agency/federal_accounts/{federal_account_code}/{?fiscal_year,order,sort}]

Returns a list of Treasury Accounts in the Federal Account's appropriations for a single fiscal year

## GET

+ Parameters
    + `federal_account_code`: `075-0512` (required, string)
        The code of a federal account.
    + `fiscal_year` (optional, number)
        The desired appropriations fiscal year. Defaults to the current FY.
    + `order` (optional, enum[string])
        Indicates what direction results should be sorted by. Valid options include asc for ascending order or desc for descending order.
        + Default: `desc`
        + Members
            + `desc`
            + `asc`
    + `sort` (optional, enum[string])
        Optional parameter indicating what value results should be sorted by.
        + Default: `obligated_amount`
        + Members
            + `name`
            + `total_budgetary_resources`
            + `obligated_amount`
            + `gross_outlay_amount`

+ Response 200 (application/json)
    + Attributes
        + `fiscal_year` (required, number)
        + `results` (required, FederalAccount, fixed-type)
        + `messages` (required, array[string], fixed-type)
            An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.

    + Body

            {
                "fiscal_year": 2022,
                "name": "Grants to States for Medicaid, Centers for Medicare and Medicaid Services, Health and Human Services",
                "obligated_amount": 458213052864.59,
                "total_budgetary_resources": 200000000,
                "gross_outlay_amount": 421403470100.22,
                "children": [
                    {
                        "name": "Grants to States for Medicaid, Centers for Medicare and Medicaid Services, Health and Human Services",
                        "code": "075-X-0512-000",
                        "obligated_amount": 454052187974.49,
                        "total_budgetary_resources": 100000000,
                        "gross_outlay_amount": 416878753221.45
                    },
                    {
                        "name": "Grants to States for Medicaid, Centers for Medicare and Medicaid Services, Health and Human Services",
                        "code": "075-075-X-0512-009",
                        "obligated_amount": 4160864890.1,
                        "total_budgetary_resources": 100000000,
                        "gross_outlay_amount": 4524716878.77
                    }
                ],
                "messages": []
            }

# Data Structures

## FederalAccount (object)
+ `name` (required, string)
+ `total_budgetary_resources` (required, number)
+ `obligated_amount` (required, number)
+ `gross_outlay_amount` (required, number)
+ `children` (required, array[TreasuryAccount], fixed-type)

## TreasuryAccount (object
+ `name` (required, string)
+ `code` (required, string)
+ `total_budgetary_resources` (required, number)
+ `obligated_amount` (required, number)
+ `gross_outlay_amount` (required, number)

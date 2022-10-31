FORMAT: 1A
HOST: https://api.usaspending.gov

# Federal Accounts Landing Page [/api/v2/federal_accounts/{account_number}/{?fiscal_year}]

This endpoint supports the federal account landing page, which provides a list of all federal accounts for which individual federal accounts pages are available on USAspending.gov.

## GET

This endpoint returns the agency identifier, account code, title, and database id for the given federal account.

+ Parameters
    + `account_number`: `011-1022` (required, string)
        The Federal Account symbol comprised of Agency Code and Main Account Code. A unique identifier for federal accounts.
    + `fiscal_year`: `2022` (optional, number) The desired appropriations fiscal year. Defaults to the current FY.

+ Response 200 (application/json)
    + Attributes
        + `fiscal_year`: (required, number)
        + `id`: (required, number)
        + `agency_identifier`: (required, string)
        + `main_account_code` (required, string)
        + `federal_account_code` (required, string)
        + `account_title` (required, string)
        + `parent_agency_toptier_code` (required, string)
        + `parent_agency_name` (required, string)
        + `bureau_name` (required, string)
        + `bureau_slug` (required, string)
        + `total_obligated_amount` (required, number),
          + Sum of all child Treasury Account `obligated_amount` values
        + `total_gross_outlay_amount` (required, number),
            + Sum of all child Treasury Account `gross_outlay_amount` values
        + `total_budgetary_resources` (required, number),
            + Sum of all child Treasury Account `budgetary_resources_amount` values
        + `children` (required, array[TreasuryAccount], fixed-type)

    + Body

            {
                "fiscal_year": "2022",
                "id": 3356,
                "agency_identifier": "011",
                "main_account_code": "1022",
                "account_title": "International Narcotics Control and Law Enforcement, International Security Assistance, State",
                "federal_account_code": "011-1022",
                "parent_agency_toptier_code": "019",
                "parent_agency_name": "Department of State",
                "bureau_name": "Interest on the Public Debt",
                "bureau_slug": "interest-on-the-public-debt",
                "total_obligated_amount": -31604.5,
                "total_gross_outlay_amount": 7643425.94,
                "total_budgetary_resources": 54653496.23,
                "children": [
                    {
                        "name": "International Narcotics Control and Law Enforcement, International Security Assistance, State",
                        "code": "019-011-2012/2017-1022-000",
                        "obligated_amount": -5497.39,
                        "gross_outlay_amount": 4463819.02,
                        "budgetary_resources_amount": 48917689.28
                    },
                    {
                        "name": "International Narcotics Control and Law Enforcement, International Security Assistance, State",
                        "code": "019-011-2013/2018-1022-000",
                        "obligated_amount": -26107.11,
                        "gross_outlay_amount": 3179606.92,
                        "budgetary_resources_amount": 5735806.95
                    },
                ]
            }

# Data Structures

## FederalAccount (object)
+ `id` (required, number)
+ `agency_identifier` (required, string)
+ `main_account_code` (required, string)
+ `federal_account_code` (required, string)
+ `account_title` (required, string)
+ `parent_agency_toptier_code` (required, string)
+ `parent_agency_name` (required, string)
+ `bureau_name` (required, string)
+ `bureau_slug` (required, string)
+ `total_obligated_amount` (required, number),
+ `total_gross_outlay_amount` (required, number),
+ `total_budgetary_resources` (required, number),
+ `children` (required, array[TreasuryAccount], fixed-type)


## TreasuryAccount (object
+ `name` (required, string)
+ `code` (required, string)
+ `obligated_amount` (required, number)
+ `gross_outlay_amount` (required, number)
+ `budgetary_resources_amount` (required, number)
FORMAT: 1A
HOST: https://api.usaspending.gov

# List Federal Accounts [/api/v2/agency/{toptier_code}/federal_account/{?fiscal_year,filter,order,sort,page,limit}]

Returns a list of Federal Accounts and Treasury Accounts in the Agency's appropriations for a single fiscal year

## GET

+ Parameters
    + `toptier_code`: `086` (required, string)
        The toptier code of an agency (could be a CGAC or FREC) so only numeric character strings of length 3-4 are accepted.
    + `fiscal_year` (optional, number)
        The desired appropriations fiscal year. Defaults to the current FY.
    + `filter` (optional, string)
        This will filter the Federal Account by their name to those matching the text.
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
    + `page` (optional, number)
        The page number that is currently returned.
        + Default: 1
    + `limit` (optional, number)
        How many results are returned.
        + Default: 10

+ Response 200 (application/json)
    + Attributes
        + `toptier_code` (required, string)
        + `fiscal_year` (required, number)
        + `page_metadata` (required, PageMetadata, fixed-type)
            Information used for pagination of results.
        + `totals` (required, Totals, fixed-type)
        + `results` (required, array[FederalAccount], fixed-type)
        + `messages` (required, array[string], fixed-type)
            An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.

    + Body

            {
                "toptier_code": "086",
                "fiscal_year": 2018,
                "page_metadata": {
                    "page": 1,
                    "total": 1,
                    "limit": 2,
                    "next": 2,
                    "previous": null,
                    "hasNext": true,
                    "hasPrevious": false,
                },
                "totals": {
                    "total_budgetary_resources": 66846596521.0,
                    "obligated_amount": 56046596521.0,
                    "gross_outlay_amount": 49589399932.2,
                },
                "results": [
                    "code": "086-0302",
                    "name": "Tenant-Based Rental Assistance, Public and Indian Housing, Housing and Urban Development",
                    "children": [
                        {
                            "name": "Tenant-Based Rental Assistance, Public and Indian Housing, Housing and Urban Development",
                            "code": "086-X-0302-000",
                            "total_budgetary_resources": 65926391527.0,
                            "obligated_amount": 55926391527.0,
                            "gross_outlay_amount": 49506649058.15
                        },
                        {
                            "name": "Tenant-Based Rental Assistance, Public and Indian Housing, Housing and Urban Development",
                            "code": "086-2019/2020-0302-000",
                            "total_budgetary_resources": 920204994.0,
                            "obligated_amount": 120204994.0,
                            "gross_outlay_amount": 82750874.0
                        }
                    ],
                    "total_budgetary_resources": 66846596521.0,
                    "obligated_amount": 56046596521.0,
                    "gross_outlay_amount": 49589399932.2
                ],
                "messages": []
            }

# Data Structures

## FederalAccount (object)
+ `name` (required, string)
+ `code` (required, string)
+ `total_budgetary_resources` (required, number)
+ `obligated_amount` (required, number)
+ `gross_outlay_amount` (required, number)
+ `children` (required, array[TreasuryAccount], fixed-type)

## Totals (object)
+ `total_budgetary_resources` (required, number)
+ `obligated_amount` (required, number)
+ `gross_outlay_amount` (required, number)

## TreasuryAccount (object
+ `name` (required, string)
+ `code` (required, string)
+ `total_budgetary_resources` (required, number)
+ `obligated_amount` (required, number)
+ `gross_outlay_amount` (required, number)

## PageMetadata (object)
+ `page` (required, number)
+ `total` (required, number)
+ `limit` (required, number)
+ `next` (required, number, nullable)
+ `previous` (required, number, nullable)
+ `hasNext` (required, boolean)
+ `hasPrevious` (required, boolean)

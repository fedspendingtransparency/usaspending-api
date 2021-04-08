FORMAT: 1A
HOST: https://api.usaspending.gov

# List Federal Accounts [/api/v2/agency/{toptier_code}/federal_account/{?fiscal_year,filter,order,sort,page,limit}]

Returns a list of Federal Accounts and Treasury Accounts in the Agency's appropriations for a single fiscal year

## GET

+ Request (application/json)
    + Schema

            {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "number"
            }
    + Parameters
        + `toptier_code`: 086 (required, number)
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
            + Default: `total_obligations`
            + Members
                + `name`
                + `total_budgetary_resources`
                + `total_obligations`
                + `total_outlays`
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
        + `combined_total_budgetary_resources` (required, number)
        + `combined_obligations` (required, number)
        + `combined_outlays` (required, number)
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
                "combined_total_budgetary_resources": 66846596521.0,
                "combined_obligations": 56046596521.0,
                "combined_outlays": 49589399932.2,
                "results": [
                    "code": "086-0302",
                    "name": "Tenant-Based Rental Assistance, Public and Indian Housing, Housing and Urban Development",
                    "children": [
                        {
                            "name": "Tenant-Based Rental Assistance, Public and Indian Housing, Housing and Urban Development",
                            "code": "086-X-0302-000",
                            "total_budgetary_resources": 65926391527.0,
                            "total_obligations": 55926391527.0,
                            "total_outlays": 49506649058.15
                        },
                        {
                            "name": "Tenant-Based Rental Assistance, Public and Indian Housing, Housing and Urban Development",
                            "code": "086-2019/2020-0302-000",
                            "total_budgetary_resources": 920204994.0,
                            "total_obligations": 120204994.0,
                            "total_outlays": 82750874.0
                        }
                    ],
                    "total_budgetary_resources": 66846596521.0,
                    "total_obligations": 56046596521.0,
                    "total_outlays": 49589399932.2
                ],
                "messages": []
            }

# Data Structures

## FederalAccount (object)
+ `name` (required, string)
+ `code` (required, string)
+ `total_budgetary_resources` (required, number)
+ `total_obligations` (required, number)
+ `total_outlays` (required, number)
+ `children` (required, array[TreasuryAccount], fixed-type)

## TreasuryAccount (object
+ `name` (required, string)
+ `code` (required, string)
+ `total_budgetary_resources` (required, number)
+ `total_obligations` (required, number)
+ `total_outlays` (required, number)

## PageMetadata (object)
+ `page` (required, number)
+ `total` (required, number)
+ `limit` (required, number)
+ `next` (required, number, nullable)
+ `previous` (required, number, nullable)
+ `hasNext` (required, boolean)
+ `hasPrevious` (required, boolean)

FORMAT: 1A
HOST: https://api.usaspending.gov

# Treasury Account's Program Activities [/api/v2/agency/treasury_account/{tas}/program_activity/{?fiscal_year,filter,order,sort,page,limit}]

Returns a list of Program Activities for the specified Treasury Account Symbol (tas).

## GET

+ Parameters
    + `tas`: `001-X-0000-000` (required, string)
        The treasury account symbol (tas) of a treasury account. This endpoint supports TAS codes with 0 or 1 slashes in the code.
    + `fiscal_year` (optional, number)
        The desired appropriations fiscal year. Defaults to the current FY.
    + `filter` (optional, string)
        This will filter the Object Classes by their name to those matching the text.
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
        + `treasury_account_symbol` (required, string)
        + `fiscal_year` (required, number)
        + `page_metadata` (required, PageMetadata, fixed-type)
            Information used for pagination of results.
        + `results` (required, array[ProgramActivity], fixed-type)
        + `messages` (required, array[string], fixed-type)
            An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.

    + Body

            {
                "treasury_account_symbol": "001-X-0000-000",
                "fiscal_year": 2020,
                "page_metadata": {
                    "hasNext": False,
                    "hasPrevious": False,
                    "limit": 10,
                    "next": None,
                    "page": 1,
                    "previous": None,
                    "total": 2,
                },
                "results": [
                    {
                        "name": "EMPLOYEE RETENTION CREDIT",
                        "obligated_amount": 150.0,
                        "gross_outlay_amount": 100.26
                        "children": [
                            {
                                "name": "Tenant-Based Rental Assistance, Public and Indian Housing, Housing and Urban Development",
                                "obligated_amount": 350.0,
                                "gross_outlay_amount": 200.36
                            }
                        ]
                    },
                    {
                        "name": "BASIC HEALTH PROGRAM",
                        "obligated_amount": 200.0,
                        "gross_outlay_amount": 100.10
                        "children": [
                            {
                                "name": "Tenant-Based Rental Assistance, Public and Indian Housing, Housing and Urban Development",
                                "obligated_amount": 350.0,
                                "gross_outlay_amount": 200.36
                            }
                        ]
                    }
                ],
                "messages": []
            }

# Data Structures

## ProgramActivity (object)
+ `name` (required, string)
+ `obligated_amount` (required, number)
+ `gross_outlay_amount` (required, number)
+ `children` (required, array[ObjectClass], fixed-type)

## ObjectClass (object)
+ `name` (required, string)
+ `obligated_amount` (required, number)
+ `gross_outlay_amount` (required, number)

## PageMetadata (object)
+ `limit` (required, number)
+ `page` (required, number)
+ `next` (required, number, nullable)
+ `previous` (required, number, nullable)
+ `hasNext` (required, boolean)
+ `hasPrevious` (required, boolean)
+ `total` (required, number)

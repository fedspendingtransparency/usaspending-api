FORMAT: 1A
HOST: https://api.usaspending.gov

# List Object Classes [/api/v2/agency/{toptier_code}/object_class/{?fiscal_year,filter,order,sort,page,limit}]

Returns a list of Object Classes in the Agency's appropriations for a single fiscal year

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
            This will filter the Object Classes by their name to those matching the text.
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
        + `results` (required, array[ObjectClass], fixed-type)
        + `messages` (required, array[string], fixed-type)
            An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.

    + Body

            {
                "toptier_code": "086",
                "fiscal_year": 2020,
                "page_metadata": {
                    "limit": 2,
                    "page": 1,
                    "next": 2,
                    "previous": null,
                    "hasNext": true,
                    "hasPrevious": false,
                    "count": 10
                },
                "results": [
                    {
                        "name": "Personnel and compensation benefits",
                        "total_budgetary_resources": 32329194065.31,
                        "total_obligations": 12329194065.31,
                        "total_outlays": 13196218848.88
                        "children": [
                            {
                                "name": "Full-time permanent",
                                "total_budgetary_resources": 329194065.76,
                                "total_obligations": 309194065.19,
                                "total_outlays": 196218848.0
                            },
                            {
                                "name": "Other than full-time permanent",
                                "total_budgetary_resources": 29194065.52,
                                "total_obligations": 20194065.33,
                                "total_outlays": 6218848.63
                            }
                        ]
                    }
                ],
                "messages": []
            }

# Data Structures

## MajorObjectClass (object)
+ `name` (required, string)
+ `total_budgetary_resources` (required, number)
+ `total_obligations` (required, number)
+ `total_outlays` (required, number)
+ `children` (required, array[ObjectClass], fixed-type)

## ObjectClass (object)
+ `name` (required, string)
+ `total_budgetary_resources` (required, number)
+ `total_obligations` (required, number)
+ `total_outlays` (required, number)

## PageMetadata (object)
+ `limit` (required, number)
+ `page` (required, number)
+ `next` (required, number, nullable)
+ `previous` (required, number, nullable)
+ `hasNext` (required, boolean)
+ `hasPrevious` (required, boolean)
+ `total` (required, number)

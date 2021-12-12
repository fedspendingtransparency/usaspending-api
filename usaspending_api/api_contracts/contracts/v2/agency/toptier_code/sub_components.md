FORMAT: 1A
HOST: https://api.usaspending.gov

# List Sub-Components [/api/v2/agency/{toptier_code}/sub_components/{?fiscal_year,agency_type,order,sort,page,limit}]

Returns a list of Sub-Components in the Agency's appropriations for a single fiscal year

## GET

+ Parameters
    + `toptier_code`: `086` (required, string)
        The toptier code of an agency (could be a CGAC or FREC) so only numeric character strings of length 3-4 are accepted.
    + `fiscal_year` (optional, number)
        The desired appropriations fiscal year. Defaults to the current FY.
    + `agency_type` (optional, enum[string])
        Indicated if the data should be pulled from the awarding agency or the funding agency
        + Default: `awarding`
        + Members
          + `awarding`
          + `funding`
    + `order` (optional, enum[string])
        Indicates what direction results should be sorted by. Valid options include asc for ascending order or desc for descending order.
        + Default: `desc`
        + Members
            + `desc`
            + `asc`
    + `sort` (optional, enum[string])
        Optional parameter indicating what value results should be sorted by.
        + Default: `total_budgetary_resources`
        + Members
            + `name`
            + `total_obligations`
            + `total_budgetary_resources`
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
        + `results` (required, array[SubComponent], fixed-type)
        + `messages` (required, array[string], fixed-type)
            An array of warnings or instructional directives to aid consumers of this endpoint with development and debugging.

    + Body

            {
                "toptier_code": "073",
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
                "results": [
                    {
                        "name": "Bureau of the Census",
                        "id": "bureau_of_the_census",
                        "total_budgetary_resources": 500000,
                        "total_obligations": 300000.72
                        "total_outlays": 1000000.45                
                    }
                ],
                "messages": []
            }

# Data Structures

## PageMetadata (object)
+ `page` (required, number)
+ `total` (required, number)
+ `limit` (required, number)
+ `next` (required, number, nullable)
+ `previous` (required, number, nullable)
+ `hasNext` (required, boolean)
+ `hasPrevious` (required, boolean)

## SubComponent (object)
+ `name` (required, string)
+ `id` (required, string) snake_case version of the Sub-Component name (bureau_slug)
+ `total_budgetary_resources` (required, number)
+ `total_obligations` (required, number)
+ `total_outlays` (required, number)
